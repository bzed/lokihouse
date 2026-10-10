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

// WaitQueryable polls a count query on Loki until it equals the fixture size.
func WaitQueryable(t *testing.T, loki string, base time.Time) {
	t.Helper()
	end := base.Add(95 * time.Minute)
	v := url.Values{
		"query": {"sum(count_over_time({app=~\".+\")[1h]))"},
		"start": {strconv.FormatInt(base.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {"60s"},
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		body := Get(t, loki+"/loki/api/v1/query_range?"+v.Encode(), "tenant-a")
		var res struct {
			Data struct {
				Result []struct {
					Value []any
				} `json:"value"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &res); err == nil && len(res.Data.Result) > 0 {
			if f, ok := res.Data.Result[0].Value[1].(float64); ok && f >= 2000 {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Logf("loki count did not reach 2000 within 60s; continuing")
}
