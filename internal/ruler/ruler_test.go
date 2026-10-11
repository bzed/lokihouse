package ruler_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ruler"
)

// group is a rule group evaluated every 100ms over {app="a"}.
const group = `name: g1
interval: 100ms
rules:
  - record: app:lines:count
    expr: sum by (app) (count_over_time({app="a"}[5m]))
  - alert: Lines
    expr: sum by (app) (count_over_time({app="a"}[5m])) > 0
    labels: {severity: page}
    annotations: {summary: '{{ $labels.app }} has {{ $value }} lines'}
`

type harness struct {
	srv  *httptest.Server
	r    *rulerbase.Ruler
	once sync.Once
}

// startRuler starts Loki's ruler over eng with filesystem rule storage in dir,
// polling and evaluating every 100ms, behind lokihouse's API router. mut, when
// set, adjusts the ruler config. The ruler is stopped when the test ends; only
// one ruler may run at a time (Loki keeps its remote-write registry global).
func startRuler(t *testing.T, dir string, eng ruler.Engine, mut func(*lokiruler.Config)) *harness {
	t.Helper()
	rc, sc := ruler.Defaults()
	rc.PollInterval = 100 * time.Millisecond
	rc.EvaluationInterval = 100 * time.Millisecond
	rc.RulePath = filepath.Join(dir, "rules-temp")
	rc.WAL.Dir = filepath.Join(dir, "ruler-wal")
	sc.Backend = "filesystem"
	sc.Filesystem.Directory = filepath.Join(dir, "rules")
	if mut != nil {
		mut(&rc)
	}
	require.NoError(t, ruler.Validate(rc, sc))
	r, rapi, err := ruler.New(context.Background(), rc, sc, eng, overrides(t), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	a := &api.API{Queries: eng, Rules: rapi, AuthEnabled: true}
	h := &harness{srv: httptest.NewServer(a.Router()), r: r}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.once.Do(func() {
		h.srv.Close()
		_ = services.StopAndAwaitTerminated(context.Background(), h.r)
	})
}

func (h *harness) do(t *testing.T, method, path, tenant, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-Scope-OrgID", tenant)
	if body != "" {
		req.Header.Set("Content-Type", "application/yaml")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

type promRules struct {
	Data struct {
		Groups []struct {
			Name, File string
			Rules      []struct {
				Name, Type, Health, LastError, State, LastEvaluation string
			}
		}
	}
}

type promAlerts struct {
	Data struct {
		Alerts []struct {
			Labels      map[string]string
			Annotations map[string]string
			State       string
			Value       string
		}
	}
}

func (h *harness) rules(t *testing.T, tenant string) promRules {
	t.Helper()
	code, body := h.do(t, http.MethodGet, "/prometheus/api/v1/rules", tenant, "")
	require.Equal(t, http.StatusOK, code, body)
	var r promRules
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	return r
}

func (h *harness) alerts(t *testing.T, tenant string) promAlerts {
	t.Helper()
	code, body := h.do(t, http.MethodGet, "/prometheus/api/v1/alerts", tenant, "")
	require.Equal(t, http.StatusOK, code, body)
	var a promAlerts
	require.NoError(t, json.Unmarshal([]byte(body), &a))
	return a
}

// evaluated reports whether every rule of tenant has been evaluated with the
// given health.
func (h *harness) evaluated(t *testing.T, tenant, health string) bool {
	r := h.rules(t, tenant)
	n := 0
	for _, g := range r.Data.Groups {
		for _, rule := range g.Rules {
			if rule.Health != health || strings.HasPrefix(rule.LastEvaluation, "0001-") {
				return false
			}
			n++
		}
	}
	return n > 0
}

func TestRules_CRUD(t *testing.T) {
	conn, _ := chtest.New(t)
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)

	code, body := h.do(t, http.MethodPost, "/loki/api/v1/rules/team%20a", "t1", group)
	require.Equal(t, http.StatusAccepted, code, body)
	require.Contains(t, body, `"status":"success"`)

	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, "team a:")
	require.Contains(t, body, "name: g1")

	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, "record: app:lines:count")

	code, _ = h.do(t, http.MethodGet, "/api/prom/rules/team%20a", "t1", "")
	require.Equal(t, http.StatusOK, code) // legacy path

	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusAccepted, code)
	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusNotFound, code, body)

	code, _ = h.do(t, http.MethodPost, "/loki/api/v1/rules/team%20a", "t1", group)
	require.Equal(t, http.StatusAccepted, code)
	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/team%20a", "t1", "")
	require.Equal(t, http.StatusAccepted, code)
	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusNotFound, code)
	require.Contains(t, body, "no rule groups found")
}

func TestRules_InvalidGroupRejected(t *testing.T) {
	conn, _ := chtest.New(t)
	dir := t.TempDir()
	h := startRuler(t, dir, engine(t, conn), nil)
	for name, tc := range map[string]struct{ body, want string }{
		"syntax error":    {"name: g\nrules:\n  - record: x\n    expr: sum(\n", "could not parse expression for record 'x'"},
		"bad record name": {"name: g\nrules:\n  - record: 'not a name'\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "invalid recording rule name"},
		"for on record":   {"name: g\nrules:\n  - record: x\n    for: 1m\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "invalid field 'for' in recording rule"},
		"not yaml":        {"{{{", "unable to decoded rule group"},
		"empty group name": {
			"rules:\n  - record: x\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "Groupname must not be empty",
		},
	} {
		t.Run(name, func(t *testing.T) {
			code, body := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", tc.body)
			require.Equal(t, http.StatusBadRequest, code, body)
			require.Contains(t, body, tc.want)
		})
	}
	code, _ := h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusNotFound, code, "nothing may be stored")

	// A namespace that escapes the storage directory never reaches the store.
	code, _ = h.do(t, http.MethodPost, "/loki/api/v1/rules/%2E%2E", "t1", group)
	require.NotEqual(t, http.StatusAccepted, code)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.Contains(t, []string{"rules", "rules-temp", "ruler-wal"}, e.Name())
	}
}

func TestRules_TenantIsolation(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t2", 3) // only t2 has data
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)

	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	code, _ = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t2", "")
	require.Equal(t, http.StatusNotFound, code, "t2 must not see t1's groups")
	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/ns", "t2", "")
	require.Equal(t, http.StatusNotFound, code, "t2 must not delete t1's namespace")
	code, _ = h.do(t, http.MethodGet, "/loki/api/v1/rules/ns/g1", "t1", "")
	require.Equal(t, http.StatusOK, code)

	// t1's rules only see t1's (empty) data: evaluated, healthy, not firing.
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
	require.Empty(t, h.alerts(t, "t1").Data.Alerts)
	require.Empty(t, h.rules(t, "t2").Data.Groups)
}

func TestRules_EvaluatesAgainstClickHouse(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { return len(h.alerts(t, "t1").Data.Alerts) == 1 }, 30*time.Second, 100*time.Millisecond)
	a := h.alerts(t, "t1").Data.Alerts[0]
	require.Equal(t, "firing", a.State)
	require.Equal(t, map[string]string{"alertname": "Lines", "app": "a", "severity": "page"}, a.Labels)
	require.Equal(t, "a has 3 lines", a.Annotations["summary"])
	require.Equal(t, "3e+00", a.Value)
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
}

// flakyEngine fails every query while fail is set.
type flakyEngine struct {
	ruler.Engine
	fail atomic.Bool
}

func (f *flakyEngine) Query(ctx context.Context, p logql.Params) logql.Query {
	if f.fail.Load() {
		return failingQuery{}
	}
	return f.Engine.Query(ctx, p)
}

type failingQuery struct{}

func (failingQuery) Exec(context.Context) (logqlmodel.Result, error) {
	return logqlmodel.Result{}, errors.New("clickhouse: connection refused")
}

func TestRules_EvaluationErrorReported(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	eng := &flakyEngine{Engine: engine(t, conn)}
	eng.fail.Store(true)
	h := startRuler(t, t.TempDir(), eng, nil)
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "err") }, 30*time.Second, 100*time.Millisecond)
	for _, r := range h.rules(t, "t1").Data.Groups[0].Rules {
		require.Contains(t, r.LastError, "connection refused")
	}

	eng.fail.Store(false)
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
}

func TestRules_SurviveRestart(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	dir := t.TempDir()

	h1 := startRuler(t, dir, engine(t, conn), nil)
	code, _ := h1.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)
	h1.stop()

	h2 := startRuler(t, dir, engine(t, conn), nil)
	code, body := h2.do(t, http.MethodGet, "/loki/api/v1/rules/ns/g1", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Eventually(t, func() bool { return len(h2.alerts(t, "t1").Data.Alerts) == 1 }, 30*time.Second, 100*time.Millisecond)
}
