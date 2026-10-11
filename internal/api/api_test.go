package api_test

import (
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/patterns"
)

func newServer(t *testing.T, auth bool) *httptest.Server {
	srv, _ := newServerConn(t, auth)
	return srv
}

func newServerConn(t *testing.T, auth bool) (*httptest.Server, driver.Conn) {
	return newServerConnWithLimits(t, auth, nil)
}

func newServerConnWithLimits(t *testing.T, auth bool, mutate func(*validation.Limits)) (*httptest.Server, driver.Conn) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	if mutate != nil {
		mutate(&l)
	}
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	q := &chstore.Querier{Conn: conn}
	a := &api.API{
		Queries:     api.NewPlainQuerier(logql.NewEngine(logql.EngineOpts{}, q, o, log.NewNopLogger())),
		Q:           q,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()},
		Limits:      o,
		Patterns:    &patterns.Querier{Q: q, Limits: o},
		AuthEnabled: auth,
	}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	return srv, conn
}

func do(t *testing.T, method, u, tenant, body string) (int, string) {
	req, _ := http.NewRequest(method, u, strings.NewReader(body))
	if tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	return resp.StatusCode, b.String()
}

func TestAPI_TenantIsolation(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	ts := strconv.FormatInt(now.UnixNano(), 10)
	code, body := do(t, "POST", srv.URL+"/loki/api/v1/push", "", `{"streams":[]}`)
	require.Equal(t, http.StatusUnauthorized, code)
	require.Contains(t, body, "no org id")

	code, body = do(t, "POST", srv.URL+"/loki/api/v1/push", "a", `{"streams":[{"stream":{"app":"x"},"values":[["`+ts+`","from a"]]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)

	qs := url.Values{"query": {`{app="x"}`}, "start": {strconv.FormatInt(now.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	_, body = do(t, "GET", srv.URL+"/loki/api/v1/query_range?"+qs.Encode(), "b", "")
	var r struct{ Data struct{ Result []any } }
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	require.Empty(t, r.Data.Result)

	_, body = do(t, "GET", srv.URL+"/loki/api/v1/query_range?"+qs.Encode(), "a", "")
	require.Contains(t, body, "from a")
	require.Contains(t, body, `"resultType":"streams"`)
}

func TestAPI_LabelsAndReady(t *testing.T) {
	srv := newServer(t, false)
	code, _ := do(t, "GET", srv.URL+"/ready", "", "")
	require.Equal(t, http.StatusOK, code)
	code, body := do(t, "GET", srv.URL+"/loki/api/v1/labels", "", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"status":"success"}`, onlyStatus(body))
}

func onlyStatus(b string) string {
	var m map[string]any
	_ = json.Unmarshal([]byte(b), &m)
	out, _ := json.Marshal(map[string]any{"status": m["status"]})
	return string(out)
}
