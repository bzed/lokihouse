//go:build oracle && soak

package oracle

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// metricValue returns the value of an unlabelled metric from /metrics.
func metricValue(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[0] == name {
			v, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// saveProfile stores /debug/pprof/<name> (after a GC for heap) in dir. For a
// heap profile it also returns the post-GC heap in use. The ?gc=1 request
// forces a GC, but the GC is asynchronous, so it polls the live
// go_memstats_heap_inuse_bytes a few times and takes the minimum — the
// post-GC floor. A leak shows as a rising floor, not a rising transient peak
// (a single metric read at an arbitrary point in the GC sawtooth would make
// the leak test flaky).
func saveProfile(t *testing.T, base, name, dir, tag string) float64 {
	t.Helper()
	q := ""
	if name == "heap" {
		q = "?gc=1"
	}
	resp, err := http.Get(base + "/debug/pprof/" + name + q)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, tag+"-"+name+".pb.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	_ = f.Close()
	if name != "heap" {
		return 0
	}
	// The ?gc=1 fetch forced a GC, but it is asynchronous; poll a few times and
	// take the minimum (the post-GC floor). A leak shows as a rising floor, not
	// a rising transient peak.
	heap := metricValue(t, base, "go_memstats_heap_inuse_bytes")
	for i := 0; i < 4; i++ {
		time.Sleep(100 * time.Millisecond)
		if v := metricValue(t, base, "go_memstats_heap_inuse_bytes"); v < heap {
			heap = v
		}
	}
	return heap
}

// TestSoak runs a mixed workload against lokihouse for LOKIHOUSE_SOAK_DURATION
// (default 10m) and checks that, after a warm-up, the post-GC heap and the
// goroutine count do not grow. Profiles from the end of the warm-up and from
// the end are kept for `go tool pprof -base`.
func TestSoak(t *testing.T) {
	dur, err := time.ParseDuration(envOr("LOKIHOUSE_SOAK_DURATION", "10m"))
	if err != nil {
		t.Fatal(err)
	}
	out := envOr("LOKIHOUSE_SOAK_OUT", filepath.Join("..", "..", ".cache", "soak"))
	if err := os.MkdirAll(out, 0o750); err != nil {
		t.Fatal(err)
	}
	rules := t.TempDir()
	house := StartLokihouse(t, fmt.Sprintf("ruler_storage: {backend: filesystem, filesystem: {dir: %s}}\nruler: {rule_path: %s, poll_interval: 5s, evaluation_interval: 5s}\n",
		filepath.Join(rules, "rules"), filepath.Join(rules, "rules-temp")))
	putRule(t, house)

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	var wg sync.WaitGroup
	for _, w := range []func(context.Context, *testing.T, string){soakPush, soakQuery, soakTail} {
		wg.Go(func() { w(ctx, t, house) })
	}

	type sample struct{ heap, goroutines float64 }
	var samples []sample
	warmup := dur / 5
	tick := time.NewTicker(dur / 40)
	defer tick.Stop()
	start := time.Now()
	warmSaved := false
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			// saveProfile on "heap" forces a GC and returns the post-GC heap,
			// so s.heap is the GC-consistent size (not a sawtooth sample).
			s := sample{saveProfile(t, house, "heap", t.TempDir(), "gc"), metricValue(t, house, "go_goroutines")}
			if time.Since(start) >= warmup {
				if !warmSaved {
					saveProfile(t, house, "heap", out, "warm")
					saveProfile(t, house, "goroutine", out, "warm")
					warmSaved = true
				}
				samples = append(samples, s)
			}
		}
	}
	wg.Wait()
	saveProfile(t, house, "heap", out, "end")
	saveProfile(t, house, "goroutine", out, "end")

	csv := "heap_inuse_bytes,goroutines\n"
	for _, s := range samples {
		csv += fmt.Sprintf("%.0f,%.0f\n", s.heap, s.goroutines)
	}
	if err := os.WriteFile(filepath.Join(out, "samples.csv"), []byte(csv), 0o600); err != nil {
		t.Fatal(err)
	}
	if len(samples) < 10 {
		t.Fatalf("only %d samples; run longer", len(samples))
	}
	// Leak criteria (after warm-up): the last quarter's median heap is at
	// most 20% above the first quarter's, and goroutines at most 20 above
	// the first sample (live tail connections come and go). The median, not
	// the mean: a sample taken while a query is mid-flight counts that
	// query's live buffers in the post-GC floor, so a quarter's mean is
	// dominated by however many samples caught concurrent fan-outs reading
	// (their driver blocks are megabytes each). A leak raises the idle floor
	// itself, which the median still tracks; a transient spike does not.
	q := len(samples) / 4
	median := func(ss []sample) float64 {
		heaps := make([]float64, len(ss))
		for i, s := range ss {
			heaps[i] = s.heap
		}
		slices.Sort(heaps)
		return heaps[len(heaps)/2]
	}
	first, last := median(samples[:q]), median(samples[len(samples)-q:])
	if last > first*1.2 {
		t.Errorf("heap grew from %.1f MiB to %.1f MiB after warm-up; compare: go tool pprof -base %s %s",
			first/(1<<20), last/(1<<20), filepath.Join(out, "warm-heap.pb.gz"), filepath.Join(out, "end-heap.pb.gz"))
	}
	if g0, g1 := samples[0].goroutines, samples[len(samples)-1].goroutines; g1 > g0+20 {
		t.Errorf("goroutines grew from %.0f to %.0f; see %s", g0, g1, filepath.Join(out, "end-goroutine.pb.gz"))
	}
}

func putRule(t *testing.T, house string) {
	t.Helper()
	body := "name: soak\ninterval: 5s\nrules:\n  - record: soak:lines:rate1m\n    expr: sum(rate({app=\"soak\"}[1m]))\n"
	req, _ := http.NewRequest(http.MethodPost, house+"/loki/api/v1/rules/soak", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "soak")
	req.Header.Set("Content-Type", "application/yaml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create rule: %d", resp.StatusCode)
	}
}

// soakPush pushes 100 lines over 10 streams every 100ms.
func soakPush(ctx context.Context, t *testing.T, house string) {
	for i := 0; ctx.Err() == nil; i++ {
		var b strings.Builder
		b.WriteString(`{"streams":[`)
		for s := 0; s < 10; s++ {
			if s > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"stream":{"app":"soak","pod":"p%d"},"values":[`, s)
			for j := 0; j < 10; j++ {
				if j > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, `[%q,"level=info i=%d j=%d msg=\"soak line\""]`, strconv.FormatInt(time.Now().UnixNano(), 10), i, j)
			}
			b.WriteString("]}")
		}
		b.WriteString("]}")
		if err := soakDo(ctx, http.MethodPost, house+"/loki/api/v1/push", "application/json", b.String(), http.StatusNoContent); err != nil {
			if ctx.Err() == nil {
				t.Errorf("push: %v", err)
			}
			return
		}
		sleepCtx(ctx, 100*time.Millisecond)
	}
}

// soakQuery runs log, metric and metadata queries in a loop.
func soakQuery(ctx context.Context, t *testing.T, house string) {
	qs := []string{
		`{app="soak"} |= "soak" | logfmt`,
		`sum by (pod) (count_over_time({app="soak"}[1m]))`,
		`topk(3, sum by (pod) (rate({app="soak"} | logfmt | level="info" [1m])))`,
	}
	for i := 0; ctx.Err() == nil; i++ {
		now := time.Now()
		v := url.Values{"query": {qs[i%len(qs)]}, "start": {strconv.FormatInt(now.Add(-5*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"15"}, "limit": {"500"}}
		for _, u := range []string{house + "/loki/api/v1/query_range?" + v.Encode(), house + "/loki/api/v1/labels"} {
			if err := soakDo(ctx, http.MethodGet, u, "", "", http.StatusOK); err != nil {
				if ctx.Err() == nil {
					t.Errorf("query: %v", err)
				}
				return
			}
		}
		sleepCtx(ctx, 200*time.Millisecond)
	}
}

// soakTail opens a tail, reads for 5s, closes it, and repeats.
func soakTail(ctx context.Context, t *testing.T, house string) {
	u := "ws" + strings.TrimPrefix(house, "http") + "/loki/api/v1/tail?" + url.Values{"query": {`{app="soak"}`}}.Encode()
	for ctx.Err() == nil {
		c, resp, err := websocket.DefaultDialer.DialContext(ctx, u, http.Header{"X-Scope-OrgID": {"soak"}})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Errorf("tail dial: %v", err)
			return
		}
		resp.Body.Close()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				break
			}
		}
		_ = c.Close()
	}
}

// soakDo sends one request as tenant "soak" and checks the status. Worker
// goroutines use it instead of PushBoth/Get, which call t.Fatal (only allowed
// on the test goroutine).
func soakDo(ctx context.Context, method, u, contentType, body string, want int) error {
	req, err := http.NewRequestWithContext(ctx, method, u, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Scope-OrgID", "soak")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: status %d: %s", method, u, resp.StatusCode, b)
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
