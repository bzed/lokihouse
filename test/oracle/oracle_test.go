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
)

func TestOracle(t *testing.T) {
	loki := StartLoki(t)
	house := StartLokihouse(t)
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
