//go:build oracle

package oracle

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
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
