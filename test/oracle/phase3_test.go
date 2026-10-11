//go:build oracle

package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// endpointCase is one Phase 3 API call compared between Loki and lokihouse.
// check, when set, replaces the default CompareMut.
type endpointCase struct {
	name   string
	path   string
	params url.Values
	mut    func(any)
	check  func(t *testing.T, loki, house []byte)
}

// formatQueryOracle runs /loki/api/v1/format_query over every corpus query on
// both stores and compares the responses. The route needs no tenant auth; Get
// sends the header anyway, which is harmless.
func formatQueryOracle(t *testing.T, loki, house string) {
	t.Helper()
	files, _ := filepath.Glob("corpus/*.logql")
	if len(files) == 0 {
		t.Fatal("no corpus files found")
	}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		i := 0
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			// Strip the "range:"/"instant:" prefix if present.
			q := line
			if k, rest, ok := strings.Cut(line, ":"); ok && (k == "range" || k == "instant") {
				q = rest
			}
			i++
			name := fmt.Sprintf("format_query %s#%d", filepath.Base(f), i)
			v := url.Values{"query": {q}}
			lb := Get(t, loki+"/loki/api/v1/format_query?"+v.Encode(), "tenant-a")
			hb := Get(t, house+"/loki/api/v1/format_query?"+v.Encode(), "tenant-a")
			Compare(t, name, lb, hb)
		}
		fh.Close()
	}
}

// Stats and volume run on selectors without web/dev/p1, which carries the
// fixture's deliberate duplicates (see docs/divergences.md).
var phase3Cases = []endpointCase{
	{name: "index_stats_api", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app="api"}`}}, check: checkStats},
	{name: "index_stats_api_legacy", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app=~"api|legacy"}`}}, check: checkStats},
	{name: "index_stats_prod", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app=~"api|legacy", env="prod"}`}}, check: checkStats},
	{name: "volume_series", path: "/loki/api/v1/index/volume", params: url.Values{"query": {`{app=~"api|legacy"}`}}, check: checkVolume},
	{name: "volume_labels_env", path: "/loki/api/v1/index/volume", params: url.Values{"query": {`{app=~"api|legacy"}`}, "targetLabels": {"env"}, "aggregateBy": {"labels"}}, check: checkVolume},
	{name: "volume_range_10m", path: "/loki/api/v1/index/volume_range", params: url.Values{"query": {`{app="api"}`}, "step": {"600"}}, check: checkVolume},
	{name: "detected_labels_all", path: "/loki/api/v1/detected_labels", params: url.Values{"query": {`{app=~".+"}`}}, mut: sortArrayBy("detectedLabels", "label")},
	{name: "detected_labels_api", path: "/loki/api/v1/detected_labels", params: url.Values{"query": {`{app="api"}`}}, mut: sortArrayBy("detectedLabels", "label")},
	// Empty selector: the no-query branch of chstore.DetectedLabels.
	{name: "detected_labels_no_query", path: "/loki/api/v1/detected_labels", params: url.Values{}, mut: sortArrayBy("detectedLabels", "label")},
	{name: "detected_fields_api_json", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="api"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_fields_web_logfmt", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_field_values_level", path: "/loki/api/v1/detected_field/level/values", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortStrings("values")},
	{name: "patterns_web", path: "/loki/api/v1/patterns", params: url.Values{"query": {`{app="web"}`}, "step": {"60s"}}, mut: sortArrayBy("data", "pattern")},
	{name: "patterns_legacy", path: "/loki/api/v1/patterns", params: url.Values{"query": {`{app="legacy"}`}, "step": {"300s"}}, mut: sortArrayBy("data", "pattern")},
}

// phase3Endpoints runs every case for both tenants over [start, end].
func phase3Endpoints(t *testing.T, loki, house string, start, end time.Time) {
	for _, c := range phase3Cases {
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			t.Run("p3/"+tenant+"/"+c.name, func(t *testing.T) {
				v := url.Values{}
				for k, vs := range c.params {
					v[k] = vs
				}
				v.Set("start", strconv.FormatInt(start.UnixNano(), 10))
				v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
				u := c.path + "?" + v.Encode()
				a, b := Get(t, loki+u, tenant), Get(t, house+u, tenant)
				if c.check != nil {
					c.check(t, a, b)
					return
				}
				CompareMut(t, c.name, a, b, c.mut)
			})
		}
	}
}

// checkStats compares index/stats: streams and entries exactly, bytes within
// 2% (Loki adds a per-chunk symbol table; docs/divergences.md). "chunks" is
// storage-specific and ignored.
func checkStats(t *testing.T, loki, house []byte) {
	t.Helper()
	var a, b struct{ Streams, Entries, Bytes uint64 }
	if err := json.Unmarshal(loki, &a); err != nil {
		t.Fatalf("loki: %v: %s", err, loki)
	}
	if err := json.Unmarshal(house, &b); err != nil {
		t.Fatalf("lokihouse: %v: %s", err, house)
	}
	if a.Streams != b.Streams || a.Entries != b.Entries || !within(float64(a.Bytes), float64(b.Bytes), 0.02) {
		t.Errorf("index/stats differ:\nloki=%s\nhouse=%s", loki, house)
	}
}

func within(a, b, rel float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= rel*math.Max(math.Abs(a), math.Abs(b))
}

// checkVolume compares index/volume(_range). The volume *value* is a documented
// divergence (docs/divergences.md): a live query goes through Loki's querier,
// which sums KB<<10 per flushed 10m chunk, while lokihouse uses raw head-block
// bytes per hourly bucket — so per-point values differ by ~15-30% structurally.
// We therefore check the response *shape* exactly (result type, metric set,
// timestamps, point count) and the per-metric volume *total* within 30%.
func checkVolume(t *testing.T, loki, house []byte) {
	t.Helper()
	type res struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	var a, b res
	if json.Unmarshal(loki, &a) != nil || json.Unmarshal(house, &b) != nil {
		t.Fatalf("decode:\nloki=%s\nhouse=%s", loki, house)
	}
	if a.Data.ResultType != b.Data.ResultType {
		t.Fatalf("resultType differs: loki=%q house=%q", a.Data.ResultType, b.Data.ResultType)
	}
	// Per metric: timestamps exactly, total volume within the documented tolerance.
	type perMetric struct {
		timestamps []float64
		total      float64
	}
	collect := func(r res) map[string]perMetric {
		out := map[string]perMetric{}
		for _, s := range r.Data.Result {
			key, _ := json.Marshal(s.Metric)
			vs := s.Values
			if s.Value != nil {
				vs = [][]any{s.Value}
			}
			pm := out[string(key)]
			for _, p := range vs {
				ts, _ := strconv.ParseFloat(fmt.Sprint(p[0]), 64)
				v, _ := strconv.ParseFloat(fmt.Sprint(p[1]), 64)
				pm.timestamps = append(pm.timestamps, ts)
				pm.total += v
			}
			sort.Float64s(pm.timestamps)
			out[string(key)] = pm
		}
		return out
	}
	pa, pb := collect(a), collect(b)
	if len(pa) != len(pb) {
		t.Fatalf("metric count differs: loki=%d house=%d", len(pa), len(pb))
	}
	for k, am := range pa {
		bm, found := pb[k]
		if !found {
			t.Errorf("metric %s: present in loki, absent in house", k)
			continue
		}
		if !sameFloats(am.timestamps, bm.timestamps) {
			t.Errorf("metric %s: timestamps differ\nloki=%v\nhouse=%v", k, am.timestamps, bm.timestamps)
		}
		if !within(am.total, bm.total, 0.30) {
			t.Errorf("metric %s: total volume differs beyond 30%%\nloki=%s\nhouse=%s", k, loki, house)
		}
	}
}

func sameFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sortStrings sorts a top-level array of strings.
func sortStrings(field string) func(any) {
	return func(v any) {
		for _, m := range objects(v) {
			arr, _ := m[field].([]any)
			sort.Slice(arr, func(i, j int) bool { return fmt.Sprint(arr[i]) < fmt.Sprint(arr[j]) })
		}
	}
}

// waitDetectedLabelsReady polls Loki's detected_labels for tenant-a until it
// returns at least one label, or until the timeout is reached.
func waitDetectedLabelsReady(t *testing.T, loki string, tenant string, query string, start, end time.Time, timeout time.Duration) {
	t.Helper()
	v := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		body := Get(t, loki+"/loki/api/v1/detected_labels?"+v.Encode(), tenant)
		var r struct {
			DetectedLabels []any `json:"detectedLabels"`
		}
		if json.Unmarshal(body, &r) == nil && len(r.DetectedLabels) > 0 {
			t.Logf("detected_labels ready after waiting")
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Logf("detected_labels still empty after %v (Loki may genuinely have none; continuing)", timeout)
}

// waitPatternsReady polls Loki's patterns endpoint for tenant-a until data is
// non-empty, or until the timeout is reached. The pattern ingester trains
// asynchronously after push, so this can take a while.
func waitPatternsReady(t *testing.T, loki string, tenant string, query string, start, end time.Time, timeout time.Duration) {
	t.Helper()
	v := url.Values{
		"query": {query},
		"step":  {"60s"},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		body := Get(t, loki+"/loki/api/v1/patterns?"+v.Encode(), tenant)
		var r struct {
			Data []any `json:"data"`
		}
		if json.Unmarshal(body, &r) == nil && len(r.Data) > 0 {
			t.Logf("patterns ready after waiting")
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("patterns did not become ready within %v", timeout)
}

// waitStatsReady forces a flush of Loki's ingester chunks and then polls
// index/stats until it returns non-zero entries, or until the timeout is
// reached. This is needed because Loki's index/stats reads from the TSDB
// index, which is built asynchronously after the ingester flushes chunks.
func waitStatsReady(t *testing.T, loki string, tenant string, query string, start, end time.Time, timeout time.Duration) {
	t.Helper()
	// Force the ingester to flush all chunks.
	req, _ := http.NewRequest(http.MethodPost, loki+"/flush", nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Logf("flush triggered: status %d", resp.StatusCode)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		a := Get(t, loki+"/loki/api/v1/index/stats?"+url.Values{
			"query": {query},
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		}.Encode(), tenant)
		var stats struct{ Entries uint64 }
		if err := json.Unmarshal(a, &stats); err == nil && stats.Entries > 0 {
			t.Logf("index/stats ready after waiting")
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("index/stats did not become ready within %v", timeout)
}

// deletesOracle exercises the delete API end-to-end: push a stream, delete a
// subset of its lines, then compare the filtered query results and the delete
// list between Loki and lokihouse. It uses a fresh tenant (tenant-del) that is
// never queried before the delete request exists: Loki caches a tenant's delete
// list for 5 minutes from its first lookup, so a pre-delete query would cache
// the (empty) list and the delete would not take effect during the test.
func deletesOracle(t *testing.T, loki, house string, base time.Time) {
	const tenant = "tenant-del"

	// 20 lines over 20 seconds, every other line contains "secret".
	var vals []string
	for i := 0; i < 20; i++ {
		line := "ok"
		if i%2 == 0 {
			line = "secret " + strconv.Itoa(i)
		}
		vals = append(vals, `["`+strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10)+`","`+line+`"]`)
	}
	body := PushBatch{Tenant: tenant, Body: []byte(`{"streams":[{"stream":{"app":"del"},"values":[` + strings.Join(vals, ",") + `]}]}`)}
	PushBoth(t, []string{loki, house}, body)

	// Send the delete request to both stores. The end must be in the past
	// relative to the store's clock: Loki (and lokihouse, copied from Loki)
	// reject delete requests whose end is in the future. The data spans
	// [base, base+20s]; base-24h..base+2m covers it with margin so it is safely
	// in the past on both stores' clocks.
	startS, endS := strconv.FormatInt(base.Add(-24*time.Hour).Unix(), 10), strconv.FormatInt(base.Add(2*time.Minute).Unix(), 10)
	dq := url.Values{"query": {`{app="del"} |= "secret"`}, "start": {startS}, "end": {endS}}
	for _, u := range []string{loki, house} {
		req, _ := http.NewRequest(http.MethodPost, u+"/loki/api/v1/delete?"+dq.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", tenant)
		resp, err := http.DefaultClient.Do(req)
		require := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		require(err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete request to %s: status %d: %s", u, resp.StatusCode, b)
		}
	}

	// Compare the filtered range query and an instant count.
	rq := url.Values{"query": {`{app="del"}`}, "start": {strconv.FormatInt(base.UnixNano(), 10)}, "end": {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)}, "step": {"60s"}, "limit": {"1000"}}
	a, b := Get(t, loki+"/loki/api/v1/query_range?"+rq.Encode(), tenant), Get(t, house+"/loki/api/v1/query_range?"+rq.Encode(), tenant)
	Compare(t, "del_range", a, b)

	// Sanity check that Loki's own result is filtered to 10 lines.
	var lr struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(a, &lr) == nil {
		var n int
		for _, s := range lr.Data.Result {
			n += len(s.Values)
		}
		if n != 10 {
			t.Fatalf("loki delete sanity check: range query returned %d lines, want 10: %s", n, a)
		}
	}

	iv := url.Values{"query": {`count_over_time({app="del"}[1m])`}, "time": {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)}}
	Compare(t, "del_count", Get(t, loki+"/loki/api/v1/query?"+iv.Encode(), tenant), Get(t, house+"/loki/api/v1/query?"+iv.Encode(), tenant))

	// Compare the delete list, dropping the store-specific request id and creation time.
	CompareMut(t, "del_list", Get(t, loki+"/loki/api/v1/delete", tenant), Get(t, house+"/loki/api/v1/delete", tenant), dropArrayKeys("request_id", "created_at"))
}
