//go:build oracle && bench

package oracle

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

// benchData pushes lines spread over streams and the last hour, in batches,
// to both stores. Deterministic (seeded).
func benchData(t *testing.T, urls []string, lines, streams int, end time.Time) {
	t.Helper()
	rnd := rand.New(rand.NewSource(7))
	const batch = 5000
	levels := []string{"info", "warn", "error", "debug"}
	for off := 0; off < lines; off += batch {
		var b strings.Builder
		b.WriteString(`{"streams":[`)
		for s := 0; s < streams; s++ {
			if s > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"stream":{"app":"app%d","env":"%s"},"values":[`, s%10, []string{"dev", "prod"}[s%2])
			first := true
			for i := off + s; i < off+batch && i < lines; i += streams {
				if !first {
					b.WriteString(",")
				}
				first = false
				ts := end.Add(-time.Hour).Add(time.Duration(i) * time.Hour / time.Duration(lines))
				fmt.Fprintf(&b, `[%q,"level=%s status=%d duration=%dms msg=\"request %d\""]`,
					strconv.FormatInt(ts.UnixNano(), 10), levels[rnd.Intn(4)], []int{200, 404, 500}[rnd.Intn(3)], rnd.Intn(900), i)
			}
			b.WriteString("]}")
		}
		b.WriteString("]}")
		PushBoth(t, urls, PushBatch{Tenant: "bench", Body: []byte(b.String())})
	}
}

var benchQueries = []struct{ name, path, query string }{
	{"log filter", "query_range", `{app="app1"} |= "status=500"`},
	{"logfmt filter", "query_range", `{env="prod"} | logfmt | status >= 500`},
	{"count_over_time", "query_range", `sum by (app) (count_over_time({env=~".+"}[5m]))`},
	{"rate regex", "query_range", `sum(rate({app=~"app[1-3]"} |~ "level=(warn|error)" [1m]))`},
	{"topk", "query_range", `topk(3, sum by (app) (rate({env="prod"}[5m])))`},
	{"unwrap quantile", "query_range", `quantile_over_time(0.95, {env="prod"} | logfmt | unwrap duration(duration) [5m]) by (app)`},
	{"bytes_over_time", "query_range", `sum(bytes_over_time({env="dev"}[10m]))`},
	{"labels", "labels", ``},
	{"series", "series", `{env="prod"}`},
}

// benchStore is one store the benchmark times.
type benchStore struct {
	name string
	url  string
}

func TestBench(t *testing.T) {
	lines := envInt("LOKIHOUSE_BENCH_LINES", 200000)
	streams := envInt("LOKIHOUSE_BENCH_STREAMS", 50)
	rounds := envInt("LOKIHOUSE_BENCH_ROUNDS", 20)
	const warmup = 2
	// Loki's config template has no ruler storage; give it a local filesystem
	// backend so it starts. The benchmark does not evaluate rules, so no
	// remote_write/alertmanager is needed.
	ruler := func() string {
		return fmt.Sprintf("\nruler_storage:\n  backend: filesystem\n  filesystem: {dir: %s/rules}\n", t.TempDir())
	}
	// The oracle's Loki turns query sharding and splitting off, so its answers
	// are the plain engine's. The benchmark times Loki as deployed: its default
	// query tuning (sharding and splitting on), results cache still off. The
	// unsharded Loki is timed too, to show what the parallelism buys.
	loki := StartLokiConfig(t, lokiDefaultQueryTemplate(), ruler())
	lokiPlain := StartLoki(t, ruler())
	house := StartLokihouse(t, "")
	stores := []benchStore{{"Loki", loki}, {"Loki (unsharded)", lokiPlain}, {"lokihouse", house}}
	end := time.Now().Truncate(time.Minute)
	benchData(t, []string{loki, lokiPlain, house}, lines, streams, end)
	// Make sure Loki has everything (the oracle's readiness wait, scaled).
	waitBenchQueryable(t, loki, lines, end)
	waitBenchQueryable(t, lokiPlain, lines, end)

	var out strings.Builder
	out.WriteString("| query |")
	for _, s := range stores {
		out.WriteString(" " + s.name + " |")
	}
	out.WriteString(" lokihouse ÷ Loki |\n|---|")
	for range stores {
		out.WriteString("---:|")
	}
	out.WriteString("---:|\n")
	for _, q := range benchQueries {
		u := func(base string) string {
			v := url.Values{"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			switch q.path {
			case "query_range":
				v.Set("query", q.query)
				v.Set("step", "60")
				v.Set("limit", "1000")
			case "series":
				v.Set("match[]", q.query)
			}
			return base + "/loki/api/v1/" + q.path + "?" + v.Encode()
		}
		ds, bodies := timeInterleaved(t, stores, u, warmup, rounds)
		// A benchmark of wrong answers is worthless: compare every store with
		// Loki. topk breaks ties by series order, which is undefined between
		// the engines; compare it as a multiset of values instead.
		for i := 1; i < len(stores); i++ {
			name := "bench " + q.name + " (" + stores[i].name + ")"
			if q.name == "topk" {
				CompareMut(t, name, bodies[0], bodies[i], sortTopKValues)
			} else {
				Compare(t, name, bodies[0], bodies[i])
			}
		}
		fmt.Fprintf(&out, "| %s |", q.name)
		for i := range stores {
			fmt.Fprintf(&out, " %s (%s) |", fmtDur(pct(ds[i], 50)), fmtDur(pct(ds[i], 90)))
		}
		fmt.Fprintf(&out, " %.2f |\n", float64(pct(ds[len(stores)-1], 50))/float64(max(pct(ds[0], 50), time.Microsecond)))
	}
	p := os.Getenv("LOKIHOUSE_BENCH_OUT")
	if p == "" {
		p = filepath.Join("..", "..", ".cache", "bench", "results.md")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	header := fmt.Sprintf("%d lines, %d streams, %d interleaved rounds per query after %d warm-up rounds, median (p90), %s\n\n", lines, streams, rounds, warmup, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(p, []byte(header+out.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("results in %s\n%s", p, out.String())
}

// lokiDefaultQueryTemplate is the oracle's Loki config with Loki's default
// query sharding and splitting restored.
func lokiDefaultQueryTemplate() string {
	s := strings.Replace(lokiConfigTemplate, "  parallelise_shardable_queries: false\n", "", 1)
	return strings.Replace(s, "  split_queries_by_interval: 0\n", "", 1)
}

// timeInterleaved warms every store up, then times rounds rounds of the
// query, rotating the order of the stores each round so that background work
// (ClickHouse merges, Loki flushes) does not always land on the same store.
// It returns the durations per store and each store's last response body
// (for Compare).
func timeInterleaved(t *testing.T, stores []benchStore, u func(string) string, warmup, rounds int) ([][]time.Duration, [][]byte) {
	t.Helper()
	ds := make([][]time.Duration, len(stores))
	bodies := make([][]byte, len(stores))
	for _, s := range stores {
		for range warmup {
			Get(t, u(s.url), "bench")
		}
	}
	for r := range rounds {
		for i := range stores {
			k := (r + i) % len(stores)
			start := time.Now()
			bodies[k] = Get(t, u(stores[k].url), "bench")
			ds[k] = append(ds[k], time.Since(start))
		}
	}
	return ds, bodies
}

// pct returns the p-th percentile of ds (nearest rank).
func pct(ds []time.Duration, p int) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[(len(s)-1)*p/100]
}

// fmtDur renders d at 0.1 ms resolution: the metadata endpoints answer in
// well under a millisecond.
func fmtDur(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
}

// sortTopKValues reduces each topk result series to a single [ts=0, max-value]
// point (a valid [timestamp, value] pair) and drops the series labels. topk
// picks k of possibly-more series that tie; the tie-break order (and which tied
// series wins) is undefined between the two engines, so the only guaranteed
// invariant is the multiset of top-k values.
func sortTopKValues(v any) {
	root, _ := v.(map[string]any)
	data, _ := root["data"].(map[string]any)
	res, _ := data["result"].([]any)
	for i := range res {
		m, _ := res[i].(map[string]any)
		mx := math.Inf(-1)
		if raw, ok := m["values"].([]any); ok {
			for _, e := range raw {
				p, _ := e.([]any)
				if len(p) >= 2 {
					if s, ok := p[1].(string); ok {
						if fv, err := strconv.ParseFloat(s, 64); err == nil && fv > mx {
							mx = fv
						}
					}
				}
			}
		}
		m["values"] = []any{[]any{float64(0), mx}}
		delete(m, "metric")
		res[i] = m
	}
	sort.SliceStable(res, func(i, j int) bool {
		a, _ := res[i].(map[string]any)
		b, _ := res[j].(map[string]any)
		return topKV(a) > topKV(b)
	})
}

func topKV(m map[string]any) float64 {
	mx := math.Inf(-1)
	if vals, ok := m["values"].([]any); ok {
		for _, e := range vals {
			p, _ := e.([]any)
			if len(p) >= 2 {
				if f, ok := p[1].(float64); ok && f > mx {
					mx = f
				}
			}
		}
	}
	return mx
}

// waitBenchQueryable polls Loki until the instant query over a 2h window sees
// all bench lines, or fails after a 10m deadline.
func waitBenchQueryable(t *testing.T, loki string, lines int, end time.Time) {
	t.Helper()
	v := url.Values{"query": {`sum(count_over_time({env=~".+"}[2h]))`}, "time": {strconv.FormatInt(end.Add(time.Minute).UnixNano(), 10)}}
	want := fmt.Sprintf(`"%d"`, lines)
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if strings.Contains(string(Get(t, loki+"/loki/api/v1/query?"+v.Encode(), "bench")), want) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("loki did not report %d lines within 10m", lines)
}
