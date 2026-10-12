//go:build oracle

package oracle

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logql/syntax"

	"github.com/bzed/lokihouse/internal/pushdown"
)

func TestOracle(t *testing.T) {
	lokiSinks, houseSinks := newRulerSinks(t), newRulerSinks(t)
	loki := StartLoki(t, rulerYAML(t.TempDir(), lokiSinks))
	house := StartLokihouse(t, rulerYAML(t.TempDir(), houseSinks))

	// Get initial Tier A leaf count (if debug endpoint is available)
	initialTierALeaves, hasDebug := getTierALeaves(t, house)

	// Recent enough that Loki still serves all of it from ingester memory: entries older
	// than ~2h40m are flushed and drop out of query results (seen with the old
	// hour-truncated base, which left the fixture up to 3h old).
	base := time.Now().Truncate(time.Minute).Add(-100 * time.Minute)
	for _, b := range Fixture(base) {
		PushBoth(t, []string{loki, house}, b)
	}
	WaitQueryable(t, loki, base)

	start, end := base, base.Add(95*time.Minute)
	files, _ := filepath.Glob("corpus/*.logql")
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			kind, q := "range", line
			if k, rest, ok := strings.Cut(line, ":"); ok && (k == "range" || k == "instant") {
				kind, q = k, rest
			}
			for _, tenant := range []string{"tenant-a", "tenant-b"} {
				t.Run(tenant+"/"+kind+"/"+q, func(t *testing.T) {
					v := url.Values{"query": {q}, "limit": {"1000"}}
					path := "/loki/api/v1/query_range"
					if kind == "instant" {
						path = "/loki/api/v1/query"
						v.Set("time", strconv.FormatInt(end.UnixNano(), 10))
					} else {
						v.Set("start", strconv.FormatInt(start.UnixNano(), 10))
						v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
						v.Set("step", "60s")
					}
					a := Get(t, loki+path+"?"+v.Encode(), tenant)
					b := Get(t, house+path+"?"+v.Encode(), tenant)
					Compare(t, q, a, b)
				})
			}
		}
		fh.Close()
	}
	for _, p := range []string{"/loki/api/v1/labels", "/loki/api/v1/label/app/values", "/loki/api/v1/series?match[]=" + url.QueryEscape(`{app=~".+"}`)} {
		t.Run("meta"+p, func(t *testing.T) {
			sep := "?"
			if strings.Contains(p, "?") {
				sep = "&"
			}
			qs := sep + "start=" + strconv.FormatInt(start.UnixNano(), 10) + "&end=" + strconv.FormatInt(end.UnixNano(), 10)
			Compare(t, p, Get(t, loki+p+qs, "tenant-a"), Get(t, house+p+qs, "tenant-a"))
		})
	}

	// detected_labels before the flush below (see detectedLabelsCases).
	phase3Endpoints(t, loki, house, start.Add(-time.Minute), end, detectedLabelsCases)

	// Wait for Loki's TSDB index to be ready before comparing index/stats.
	// Loki's index/stats reads from the TSDB index, which is built asynchronously
	// after the ingester flushes chunks.
	waitStatsReady(t, loki, "tenant-a", `{app="api"}`, start, end, 5*time.Minute)

	// The pattern ingester trains asynchronously; wait for it to produce data.
	waitPatternsReady(t, loki, "tenant-a", `{app="web"}`, start, end, 60*time.Second)

	phase3Endpoints(t, loki, house, start.Add(-time.Minute), end, phase3Cases)

	// Multi-tenant queries (both fixture tenants in one X-Scope-OrgID).
	multiTenantOracle(t, loki, house, start, end)

	// The ruler: rule CRUD, evaluation state, remote write and notifications
	// over tenant-a's fixture.
	rulerOracle(t, loki, house, lokiSinks, houseSinks)

	// The delete API, on a fresh tenant (Loki caches a tenant's delete list for
	// 5 minutes from its first lookup, so tenant-del must not be queried above).
	deletesOracle(t, loki, house, start)

	// Tail over WebSocket, last because it pushes new data after the fixture.
	tailOracle(t, loki, house)

	// format_query over the whole corpus (no tenant auth needed).
	formatQueryOracle(t, loki, house)

	// Assert Tier A coverage: the counter must have grown by at least the number
	// of eligible leaves in pushdown.logql × 2 tenants. Only check when push-down
	// is enabled (mode "on").
	if hasDebug && os.Getenv("LOKIHOUSE_ORACLE_PUSHDOWN") == "" {
		finalTierALeaves, _ := getTierALeaves(t, house)
		growth := finalTierALeaves - initialTierALeaves
		expected := int64(countEligibleLeaves(t, "corpus/pushdown.logql") * 2) // 2 tenants
		if growth < expected {
			t.Errorf("Tier A coverage: counter grew by %d, want >= %d (eligible leaves in pushdown.logql × 2 tenants)", growth, expected)
		} else {
			t.Logf("Tier A coverage: counter grew by %d (expected >= %d)", growth, expected)
		}
	}
}

// PushBoth POSTs the same body to both endpoints and requires 204 from both.
func PushBoth(t *testing.T, urls []string, b PushBatch) {
	t.Helper()
	for _, u := range urls {
		req, err := http.NewRequest(http.MethodPost, u+"/loki/api/v1/push", strings.NewReader(string(b.Body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Scope-OrgID", b.Tenant)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("push to %s: %v", u, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("push to %s: status %d: %s", u, resp.StatusCode, body)
		}
	}
}

// Get fetches a query, requiring 200 (or 400 with matching error on both sides).
func Get(t *testing.T, u, tenant string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", tenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", u, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("get %s: status %d: %s", u, resp.StatusCode, body)
	}
	return body
}

// WaitQueryable polls Loki until tenant-a's whole fixture (totalEntries after
// dedup) is countable. The instant query's 2h window covers every fixture entry,
// including the one at exactly base that a left-open 90m window would miss.
func WaitQueryable(t *testing.T, loki string, base time.Time) {
	t.Helper()
	v := url.Values{
		"query": {`sum(count_over_time({app=~".+"}[2h]))`},
		"time":  {strconv.FormatInt(base.Add(95*time.Minute).UnixNano(), 10)},
	}
	var last string
	begin := time.Now()
	deadline := begin.Add(300 * time.Second)
	for time.Now().Before(deadline) {
		body := Get(t, loki+"/loki/api/v1/query?"+v.Encode(), "tenant-a")
		var res struct {
			Data struct {
				Result []struct {
					Value []any `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &res); err == nil && len(res.Data.Result) == 1 {
			if v, _ := res.Data.Result[0].Value[1].(string); v != last {
				t.Logf("loki count %s after %s", v, time.Since(begin).Round(time.Second))
				last = v
			}
			if last == strconv.Itoa(totalEntries) {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("loki count is %q, want %d after 300s", last, totalEntries)
}

// getTierALeaves fetches the Tier A leaf counter from the debug endpoint.
// Returns the count and whether the endpoint was available.
func getTierALeaves(t *testing.T, house string) (int64, bool) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, house+"/debug/lokihouse/tiera_leaves", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, _ := io.ReadAll(resp.Body)
	n, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// countEligibleLeaves parses the pushdown corpus and counts the number of
// eligible range-aggregation leaves. It uses a TierA with no connection
// because Eligible is static.
func countEligibleLeaves(t *testing.T, corpusFile string) int {
	t.Helper()
	fh, err := os.Open(corpusFile)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	var a pushdown.TierA
	count := 0
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Strip the prefix
		q := line
		if k, rest, ok := strings.Cut(line, ":"); ok && (k == "range" || k == "instant") {
			q = rest
		}
		// Parse and count eligible leaves
		expr, err := syntax.ParseSampleExpr(q)
		if err != nil {
			continue // Not a valid sample expression, skip
		}
		_, leaves, ok := pushdown.Map(expr)
		if !ok {
			continue
		}
		for _, leaf := range leaves {
			if a.Eligible(leaf) {
				count++
			}
		}
	}
	return count
}
