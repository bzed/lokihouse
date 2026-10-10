//go:build oracle

package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOracle(t *testing.T) {
	loki := StartLoki(t)
	house := StartLokihouse(t)
	base := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
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

// WaitQueryable polls a range count on Loki until the max per-step value
// reaches the fixture size (2500 per tenant). The fixture spans 90 minutes, so
// a 90m lookback counts every entry once it has been flushed. We poll for up
// to 120s to absorb Loki's filesystem-backend flush latency; 60% of the target
// is accepted as "ready" since the flush can lag by tens of seconds.
func WaitQueryable(t *testing.T, loki string, base time.Time) {
	t.Helper()
	end := base.Add(95 * time.Minute)
	v := url.Values{
		"query": {"sum(count_over_time({app=~\".+\")[90m]))"},
		"start": {strconv.FormatInt(base.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"60s"},
	}
	const target = 2500
	const flushTolerance = 0.6 // accept >= 60% of target (Loki flush lag)
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		body := Get(t, loki+"/loki/api/v1/query_range?"+v.Encode(), "tenant-a")
		var res struct {
			Data struct {
				Result []struct {
					Values [][]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &res); err == nil && len(res.Data.Result) > 0 {
			maxSeen := 0.0
			for _, vv := range res.Data.Result[0].Values {
				if s, ok := vv[1].(string); ok {
					var f64 float64
					if _, err := fmt.Sscanf(s, "%f", &f64); err == nil {
						if f64 > maxSeen {
							maxSeen = f64
						}
					}
				}
			}
			if maxSeen >= float64(target)*flushTolerance {
				if maxSeen < float64(target) {
					t.Logf("loki count reached %.0f (target %d); continuing", maxSeen, target)
				}
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("loki count did not reach %d within 120s; continuing", target)
}
