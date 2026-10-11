//go:build oracle

package oracle

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
	{name: "detected_fields_api_json", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="api"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_fields_web_logfmt", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_field_values_level", path: "/loki/api/v1/detected_field/level/values", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortStrings("values")},
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
