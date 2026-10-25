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

	"github.com/bzed/lokihouse/internal/chstore"
)

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

// envDuration reads a time.ParseDuration value from the environment.
func envDuration(t *testing.T, k string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		t.Fatalf("%s=%q: want a positive duration", k, v)
	}
	return d
}

// benchData pushes lines spread over streams and the span before end, in
// batches, to all stores. Deterministic (seeded).
func benchData(t *testing.T, urls []string, lines, streams int, end time.Time, span time.Duration) {
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
				ts := end.Add(-span).Add(time.Duration(i) * span / time.Duration(lines))
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

// benchTable is one result table: a subset of the stores (indexes into the
// benchmark's store list) and the median ratios to print after them.
type benchTable struct {
	title  string
	stores []int
	ratios []benchRatio
}

// benchRatio is the column "num ÷ den" of two stores' medians.
type benchRatio struct{ num, den int }

func TestBench(t *testing.T) {
	lines := envInt("LOKIHOUSE_BENCH_LINES", 200000)
	streams := envInt("LOKIHOUSE_BENCH_STREAMS", 50)
	rounds := envInt("LOKIHOUSE_BENCH_ROUNDS", 20)
	span := envDuration(t, "LOKIHOUSE_BENCH_SPAN", time.Hour)
	const warmup = 2
	// Loki's config template has no ruler storage; give it a local filesystem
	// backend so it starts. The benchmark does not evaluate rules, so no
	// remote_write/alertmanager is needed.
	ruler := func() string {
		extra := fmt.Sprintf("\nruler_storage:\n  backend: filesystem\n  filesystem: {dir: %s/rules}\n", t.TempDir())
		if span > 2*time.Hour {
			// Flushed chunks drop out of Loki's results after about 2h40m
			// (see TestOracle); keep a long span in the ingester.
			extra += "ingester: {max_chunk_age: 24h, chunk_idle_period: 24h}\n"
		}
		return extra
	}
	// The oracle's Loki turns query sharding and splitting off, so its answers
	// are the plain engine's. The benchmark times Loki as deployed: its default
	// query tuning (sharding and splitting on), results cache still off. The
	// unsharded Loki is timed too, to show what the parallelism buys.
	stores := []benchStore{
		{"Loki", StartLokiConfig(t, lokiDefaultQueryTemplate(), ruler())},
		{"Loki (unsharded)", StartLoki(t, ruler())},
		{"lokihouse", StartLokihouse(t, "")},
	}
	tables := []benchTable{{"Recent data, local disk", []int{0, 1, 2}, []benchRatio{{2, 0}}}}
	// The object-storage scenario (test/bench/up.sh): Loki with chunks and
	// index in S3, flushed and queried from the store only, against lokihouse
	// on ClickHouse servers whose MergeTree tables live in S3.
	// lokiS3Reader, when set, replaces the S3 Loki's writer with its reader
	// once the data is pushed.
	var lokiS3Reader func() string
	if s3 := os.Getenv("LOKIHOUSE_BENCH_S3"); s3 != "" {
		chS3, chS3Cache := os.Getenv("LOKIHOUSE_BENCH_CH_S3"), os.Getenv("LOKIHOUSE_BENCH_CH_S3_CACHE")
		if chS3 == "" || chS3Cache == "" {
			t.Fatal("LOKIHOUSE_BENCH_S3 needs LOKIHOUSE_BENCH_CH_S3 and LOKIHOUSE_BENCH_CH_S3_CACHE (make bench sets all three)")
		}
		single := chstore.Topology{}
		n := len(stores)
		var writer string
		writer, lokiS3Reader = startLokiS3(t, s3, ruler())
		stores = append(stores,
			benchStore{"Loki S3", writer},
			benchStore{"lokihouse S3", StartLokihouseOn(t, []string{chS3}, single, "")},
			benchStore{"lokihouse S3 + cache", StartLokihouseOn(t, []string{chS3Cache}, single, "")},
		)
		tables = append(tables, benchTable{"Object storage (S3 on RustFS), Loki flushed", []int{n, n + 1, n + 2}, []benchRatio{{n + 1, n}, {n + 2, n}}})
	}
	urls := make([]string, len(stores))
	for i, s := range stores {
		urls[i] = s.url
	}
	end := time.Now().Truncate(time.Minute)
	benchData(t, urls, lines, streams, end, span)
	if lokiS3Reader != nil {
		for i := range stores {
			if stores[i].name == "Loki S3" {
				stores[i].url = lokiS3Reader()
			}
		}
	}
	// Make sure every Loki has everything (the oracle's readiness wait,
	// scaled).
	for _, s := range stores {
		if strings.HasPrefix(s.name, "Loki") {
			waitBenchQueryable(t, s.url, lines, end, span)
		}
	}

	outs := make([]strings.Builder, len(tables))
	for i, tb := range tables {
		outs[i].WriteString(tableHeader(stores, tb))
	}
	for _, q := range benchQueries {
		u := func(base string) string {
			v := url.Values{"start": {strconv.FormatInt(end.Add(-span).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
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
		for i, tb := range tables {
			fmt.Fprintf(&outs[i], "| %s |", q.name)
			for _, k := range tb.stores {
				fmt.Fprintf(&outs[i], " %s (%s) |", fmtDur(pct(ds[k], 50)), fmtDur(pct(ds[k], 90)))
			}
			for _, r := range tb.ratios {
				fmt.Fprintf(&outs[i], " %.2f |", float64(pct(ds[r.num], 50))/float64(max(pct(ds[r.den], 50), time.Microsecond)))
			}
			outs[i].WriteString("\n")
		}
	}
	p := os.Getenv("LOKIHOUSE_BENCH_OUT")
	if p == "" {
		p = filepath.Join("..", "..", ".cache", "bench", "results.md")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%d lines over %s, %d streams, %d interleaved rounds per query after %d warm-up rounds, median (p90), %s\n", lines, span, streams, rounds, warmup, time.Now().UTC().Format(time.RFC3339))
	for i, tb := range tables {
		fmt.Fprintf(&out, "\n### %s\n\n%s", tb.title, outs[i].String())
	}
	if err := os.WriteFile(p, []byte(out.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("results in %s\n%s", p, out.String())
}

// tableHeader renders the Markdown header of one result table.
func tableHeader(stores []benchStore, tb benchTable) string {
	var b strings.Builder
	b.WriteString("| query |")
	for _, k := range tb.stores {
		b.WriteString(" " + stores[k].name + " |")
	}
	for _, r := range tb.ratios {
		b.WriteString(" " + stores[r.num].name + " ÷ " + stores[r.den].name + " |")
	}
	b.WriteString("\n|---|")
	for range len(tb.stores) + len(tb.ratios) {
		b.WriteString("---:|")
	}
	b.WriteString("\n")
	return b.String()
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
func waitBenchQueryable(t *testing.T, loki string, lines int, end time.Time, span time.Duration) {
	t.Helper()
	v := url.Values{"query": {fmt.Sprintf(`sum(count_over_time({env=~".+"}[%ds]))`, int(span.Seconds())+120)}, "time": {strconv.FormatInt(end.Add(time.Minute).UnixNano(), 10)}}
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
