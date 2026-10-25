//go:build oracle && bench

package oracle

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
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

func TestBench(t *testing.T) {
	lines := envInt("LOKIHOUSE_BENCH_LINES", 200000)
	streams := envInt("LOKIHOUSE_BENCH_STREAMS", 50)
	rounds := envInt("LOKIHOUSE_BENCH_ROUNDS", 5)
	// Loki's config template has no ruler storage; give it a local filesystem
	// backend so it starts. The benchmark does not evaluate rules, so no
	// remote_write/alertmanager is needed.
	lokiDir := t.TempDir()
	loki := StartLoki(t, fmt.Sprintf(`
ruler_storage:
  backend: filesystem
  filesystem: {dir: %s/rules}
`, lokiDir))
	house := StartLokihouse(t, "")
	// The fallback floor: a pool of two, the smallest that can ingest (a push
	// with new streams holds two connections), which caps the fan-out's budget
	// at one — every sample query reads all streams in one query, and every
	// query runs on a single connection.
	houseFallback := StartLokihouseConns(t, "", 2)
	end := time.Now().Truncate(time.Minute)
	benchData(t, []string{loki, house, houseFallback}, lines, streams, end)
	// Make sure Loki has everything (the oracle's readiness wait, scaled).
	waitBenchQueryable(t, loki, lines, end)

	var out strings.Builder
	fmt.Fprintf(&out, "| query | Loki | lokihouse (optimal) | lokihouse (single connection) |\n|---|---:|---:|---:|\n")
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
		lt, lb := timeIt(t, u(loki), rounds)
		ht, hb := timeIt(t, u(house), rounds)
		ft, fb := timeIt(t, u(houseFallback), rounds)
		// A benchmark of wrong answers is worthless: compare all stores.
		// topk breaks ties by series order, which is undefined between the two
		// engines; compare it as a multiset of values instead.
		if q.name == "topk" {
			CompareMut(t, "bench "+q.name, lb, hb, sortTopKValues)
			CompareMut(t, "bench "+q.name+" (single connection)", lb, fb, sortTopKValues)
		} else {
			Compare(t, "bench "+q.name, lb, hb)
			Compare(t, "bench "+q.name+" (single connection)", lb, fb)
		}
		fmt.Fprintf(&out, "| %s | %s | %s | %s |\n", q.name, minmax(lt), minmax(ht), minmax(ft))
	}
	p := os.Getenv("LOKIHOUSE_BENCH_OUT")
	if p == "" {
		p = filepath.Join("..", "..", ".cache", "bench", "results.md")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	header := fmt.Sprintf("%d lines, %d streams, %d rounds per query, %s\n\n", lines, streams, rounds, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(p, []byte(header+out.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("results in %s\n%s", p, out.String())
}

// timeIt issues the query rounds times, returning the durations and the body
// of the last response (for Compare).
func timeIt(t *testing.T, u string, rounds int) ([]time.Duration, []byte) {
	t.Helper()
	var ds []time.Duration
	var body []byte
	for i := 0; i < rounds; i++ {
		start := time.Now()
		body = Get(t, u, "bench")
		ds = append(ds, time.Since(start))
	}
	return ds, body
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

// minmax formats the fastest and the slowest round as a range.
func minmax(ds []time.Duration) string {
	lo, hi := ds[0], ds[0]
	for _, d := range ds[1:] {
		lo = min(lo, d)
		hi = max(hi, d)
	}
	return lo.Round(time.Millisecond).String() + "–" + hi.Round(time.Millisecond).String()
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
