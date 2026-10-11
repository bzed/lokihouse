//go:build oracle

package oracle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage/remote"
)

// rulerSinks receive one store's recording-rule samples (Prometheus remote
// write) and alert notifications (Alertmanager API v2).
type rulerSinks struct {
	rw, am  *httptest.Server
	mu      sync.Mutex
	samples map[string]float64 // series labels -> latest value
	tenants map[string]bool    // X-Scope-OrgID headers seen on remote write
	alerts  map[string]amAlert // alert labels -> latest notification
}

type amAlert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generatorURL"`
}

func newRulerSinks(t *testing.T) *rulerSinks {
	s := &rulerSinks{samples: map[string]float64{}, tenants: map[string]bool{}, alerts: map[string]amAlert{}}
	s.rw = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.tenants[r.Header.Get("X-Scope-OrgID")] = true
		for _, ts := range req.Timeseries {
			m := map[string]string{}
			for _, l := range ts.Labels {
				m[l.Name] = l.Value
			}
			if n := len(ts.Samples); n > 0 {
				s.samples[fmt.Sprint(m)] = ts.Samples[n-1].Value
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.am = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var as []amAlert
		if err := json.NewDecoder(r.Body).Decode(&as); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, a := range as {
			s.alerts[fmt.Sprint(a.Labels)] = a
		}
	}))
	t.Cleanup(func() { s.rw.Close(); s.am.Close() })
	return s
}

// snapshot returns the sink state as JSON for Compare.
func (s *rulerSinks) snapshot() (samples, tenants, alerts []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	samples, _ = json.Marshal(s.samples)
	tenants, _ = json.Marshal(s.tenants)
	alerts, _ = json.Marshal(s.alerts)
	return
}

func (s *rulerSinks) counts() (series, alerts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.samples), len(s.alerts)
}

// rulerYAML is the ruler config both stores get (Loki's keys; lokihouse reads
// the same ones), with its own directories and sinks.
func rulerYAML(dir string, s *rulerSinks) string {
	return fmt.Sprintf(`
ruler:
  poll_interval: 1s
  evaluation_interval: 1s
  rule_path: %[1]s/rules-temp
  external_url: http://grafana.oracle
  alertmanager_url: %[2]s
  remote_write:
    enabled: true
    clients:
      oracle: {url: "%[3]s/api/v1/write", queue_config: {batch_send_deadline: 1s}}
  wal: {dir: %[1]s/ruler-wal}
ruler_storage:
  backend: filesystem
  filesystem: {dir: %[1]s/rules}
`, dir, s.am.URL, s.rw.URL)
}

// oracleGroup covers recording rules (plain, parser, bytes), a firing alert
// with templated annotations, a pending alert and an inactive one, over the
// tenant-a fixture (all of it lies inside [now-3h, now]).
const oracleGroup = `name: oracle
interval: 1s
rules:
  - record: app:lines:count
    expr: sum by (app) (count_over_time({app=~".+"}[3h]))
  - record: api:status:count
    expr: sum by (status) (count_over_time({app="api"} | json [3h]))
  - record: env:bytes
    expr: sum by (env) (bytes_over_time({app=~".+"}[3h]))
  - alert: ManyLines
    expr: sum by (app) (count_over_time({app=~".+"}[3h])) > 100
    labels: {severity: page}
    annotations: {summary: '{{ $labels.app }} has {{ $value }} lines'}
  - alert: ManyLinesPending
    expr: sum by (app) (count_over_time({app=~".+"}[3h])) > 100
    for: 1h
  - alert: Never
    expr: sum(count_over_time({app="api"}[3h])) > 1e9
`

// brokenGroup is accepted by Loki (the expression parses) but every
// evaluation fails: a log query is not a vector.
const brokenGroup = `name: broken
interval: 1s
rules:
  - record: logs:raw
    expr: '{app="api"}'
`

const tmpGroup = `name: tmp
interval: 1m
rules:
  - record: tmp:count
    expr: sum(count_over_time({app="web"}[1m]))
`

// send performs one ruler API call and returns status and body.
func send(t *testing.T, method, u, tenant, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, u, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", tenant)
	if body != "" {
		req.Header.Set("Content-Type", "application/yaml")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, u, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// normRules drops the evaluation timestamps and durations (each store runs its
// own schedule) and orders each rule's alerts by labels.
func normRules(v any) {
	switch t := v.(type) {
	case map[string]any:
		for _, k := range []string{"lastEvaluation", "evaluationTime", "activeAt"} {
			delete(t, k)
		}
		if as, ok := t["alerts"].([]any); ok {
			sort.SliceStable(as, func(i, j int) bool {
				return fmt.Sprint(as[i].(map[string]any)["labels"]) < fmt.Sprint(as[j].(map[string]any)["labels"])
			})
		}
		for _, c := range t {
			normRules(c)
		}
	case []any:
		for _, c := range t {
			normRules(c)
		}
	}
}

// waitRulesEvaluated polls until every rule of tenant has been evaluated and
// every rule outside group "broken" is healthy (the first evaluations can fail
// while the remote-write WAL starts).
func waitRulesEvaluated(t *testing.T, u, tenant string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var r struct {
			Data struct {
				Groups []struct {
					Name  string
					Rules []struct{ Health, LastEvaluation string }
				}
			}
		}
		_ = json.Unmarshal(Get(t, u+"/prometheus/api/v1/rules", tenant), &r)
		n := 0
		for _, g := range r.Data.Groups {
			for _, rule := range g.Rules {
				if !strings.HasPrefix(rule.LastEvaluation, "0001-") && (g.Name == "broken" || rule.Health == "ok") {
					n++
				}
			}
		}
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d of %d rules evaluated after %s", u, n, want, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func rulerOracle(t *testing.T, loki, house string, ls, hs *rulerSinks) {
	both := func(name, method, path, tenant, body string) {
		t.Helper()
		lc, lb := send(t, method, loki+path, tenant, body)
		hc, hb := send(t, method, house+path, tenant, body)
		if lc != hc {
			t.Errorf("%s: status loki=%d lokihouse=%d\nloki: %s\nlokihouse: %s", name, lc, hc, lb, hb)
			return
		}
		Compare(t, name, lb, hb)
	}

	// Rule-group CRUD, Loki's validation errors and tenant isolation.
	both("rules_create", "POST", "/loki/api/v1/rules/oracle%20ns", "tenant-a", oracleGroup)
	both("rules_create_broken", "POST", "/loki/api/v1/rules/oracle%20ns", "tenant-a", brokenGroup)
	both("rules_bad_expr", "POST", "/loki/api/v1/rules/bad", "tenant-a", "name: g\nrules:\n  - record: x\n    expr: sum(\n")
	both("rules_bad_record_name", "POST", "/loki/api/v1/rules/bad", "tenant-a", "name: g\nrules:\n  - record: 'not a name'\n    expr: sum(count_over_time({app=\"api\"}[1m]))\n")
	both("rules_bad_yaml", "POST", "/loki/api/v1/rules/bad", "tenant-a", "{{{")
	both("rules_list", "GET", "/loki/api/v1/rules", "tenant-a", "")
	both("rules_list_ns", "GET", "/loki/api/v1/rules/oracle%20ns", "tenant-a", "")
	both("rules_get_group", "GET", "/loki/api/v1/rules/oracle%20ns/oracle", "tenant-a", "")
	both("rules_get_missing", "GET", "/loki/api/v1/rules/oracle%20ns/missing", "tenant-a", "")
	// %2F separates namespace and group in both (Loki routes on the decoded path).
	both("rules_get_group_encoded_slash", "GET", "/loki/api/v1/rules/oracle%20ns%2Foracle", "tenant-a", "")
	both("rules_legacy_get_group_encoded_slash", "GET", "/api/prom/rules/oracle%20ns%2foracle", "tenant-a", "")
	both("rules_legacy_list", "GET", "/api/prom/rules", "tenant-a", "")
	both("rules_other_tenant", "GET", "/loki/api/v1/rules", "tenant-b", "")
	both("rules_tmp_create", "POST", "/loki/api/v1/rules/tmp", "tenant-a", tmpGroup)
	both("rules_tmp_delete_group", "DELETE", "/loki/api/v1/rules/tmp/tmp", "tenant-a", "")
	both("rules_tmp_get_deleted", "GET", "/loki/api/v1/rules/tmp/tmp", "tenant-a", "")
	both("rules_tmp_recreate", "POST", "/loki/api/v1/rules/tmp", "tenant-a", tmpGroup)
	both("rules_tmp_delete_ns", "DELETE", "/loki/api/v1/rules/tmp", "tenant-a", "")
	both("rules_tmp_list_deleted", "GET", "/loki/api/v1/rules/tmp", "tenant-a", "")

	// Evaluation state: 6 rules in "oracle" + 1 in "broken".
	for _, u := range []string{loki, house} {
		waitRulesEvaluated(t, u, "tenant-a", 7, 2*time.Minute)
	}
	for _, p := range []string{"/prometheus/api/v1/rules", "/prometheus/api/v1/alerts"} {
		CompareMut(t, "ruler"+p, Get(t, loki+p, "tenant-a"), Get(t, house+p, "tenant-a"), normRules)
	}

	// Recording-rule samples and Alertmanager notifications. The fixture has
	// 3 apps, 3 statuses and 2 envs: 8 series; ManyLines fires for 3 apps.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ln, la := ls.counts()
		hn, ha := hs.counts()
		if ln >= 8 && hn >= 8 && la >= 3 && ha >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sinks incomplete after 2m: loki %d series/%d alerts, lokihouse %d series/%d alerts", ln, la, hn, ha)
		}
		time.Sleep(500 * time.Millisecond)
	}
	lsamp, ltenants, lalerts := ls.snapshot()
	hsamp, htenants, halerts := hs.snapshot()
	Compare(t, "remote_write_samples", lsamp, hsamp)
	Compare(t, "remote_write_tenants", ltenants, htenants)
	Compare(t, "alertmanager_alerts", lalerts, halerts)
}
