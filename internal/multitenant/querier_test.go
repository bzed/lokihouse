package multitenant_test

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/multitenant"
	"github.com/bzed/lokihouse/internal/patterns"
)

// newMTServer builds the API the way server.newAPI will after Task 10 wiring:
// when mt is true, log/metric queries fan out per tenant through
// multitenant.Querier and label/series use the same multi-tenant querier.
func newMTServer(t *testing.T, mt bool) *httptest.Server {
	t.Helper()
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	store := &deletes.Store{Conn: conn}
	q := &chstore.Querier{Conn: conn, Deletes: store}
	nop := log.NewNopLogger()

	var queries api.Querier
	var meta api.MetaQuerier
	if mt {
		mq := &multitenant.Querier{Next: q}
		queries = api.NewPlainQuerier(logql.NewEngine(logql.EngineOpts{}, mq, o, nop))
		meta = mq
	} else {
		queries = api.NewPlainQuerier(logql.NewEngine(logql.EngineOpts{}, q, o, nop))
	}
	a := &api.API{
		Queries:     queries,
		Q:           q,
		Meta:        meta,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: nop},
		Limits:      o,
		Patterns:    &patterns.Querier{Q: q, Limits: o},
		Deletes:     &deletes.Handler{Store: store, Limits: o, MaxInterval: time.Hour, CancelPeriod: time.Hour},
		AuthEnabled: true,
	}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	return srv
}

func mtDo(t *testing.T, method, u, tenant string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, u, strings.NewReader(""))
	if tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func pushOne(t *testing.T, base, tenant string, ts time.Time, line string) {
	t.Helper()
	body := fmt.Sprintf(`{"streams":[{"stream":{"app":"a"},"values":[[%q,%q]]}]}`, strconv.FormatInt(ts.UnixNano(), 10), line)
	req, _ := http.NewRequest(http.MethodPost, base+"/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", tenant)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func query(t *testing.T, base, tenant, q string, now time.Time) string {
	t.Helper()
	v := url.Values{"query": {q}, "start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body := mtDo(t, http.MethodGet, base+"/loki/api/v1/query_range?"+v.Encode(), tenant)
	require.Equal(t, http.StatusOK, code, body)
	return body
}

func labels(t *testing.T, base, tenant string, now time.Time) string {
	t.Helper()
	v := url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body := mtDo(t, http.MethodGet, base+"/loki/api/v1/labels?"+v.Encode(), tenant)
	require.Equal(t, http.StatusOK, code, body)
	return body
}

func series(t *testing.T, base, tenant, match string, now time.Time) string {
	t.Helper()
	v := url.Values{"match[]": {match}, "start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body := mtDo(t, http.MethodGet, base+"/loki/api/v1/series?"+v.Encode(), tenant)
	require.Equal(t, http.StatusOK, code, body)
	return body
}

func TestMultiTenant_QueryLabelsSeries(t *testing.T) {
	srv := newMTServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now, "one")
	pushOne(t, srv.URL, "t2", now, "two")

	body := query(t, srv.URL, "t1|t2", `{app="a"}`, now)
	require.Contains(t, body, `"__tenant_id__":"t1"`)
	require.Contains(t, body, `"__tenant_id__":"t2"`)

	body = query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__="t2"}`, now)
	require.NotContains(t, body, `"__tenant_id__":"t1"`)
	require.Contains(t, body, "two")

	body = query(t, srv.URL, "t1|t2", `sum by (__tenant_id__) (count_over_time({app="a"}[5m]))`, now)
	require.Contains(t, body, `"__tenant_id__":"t1"`)

	require.Contains(t, labels(t, srv.URL, "t1|t2", now), `"__tenant_id__"`)
	require.Contains(t, series(t, srv.URL, "t1|t2", `{app="a"}`, now), `"__tenant_id__":"t2"`)
}

func TestMultiTenant_NoForeignTenant(t *testing.T) {
	srv := newMTServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "c", now, "secret")
	body := query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__="c"}`, now)
	require.NotContains(t, body, "secret")
	body = query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__=~".+"}`, now)
	require.NotContains(t, body, "secret")
}

func TestMultiTenant_DisabledRejects(t *testing.T) {
	srv := newMTServer(t, false)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now, "one")
	v := url.Values{"query": {`{app="a"}`}, "start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body := mtDo(t, http.MethodGet, srv.URL+"/loki/api/v1/query_range?"+v.Encode(), "t1|t2")
	require.NotEqual(t, http.StatusOK, code)
	require.Contains(t, body, "multiple org IDs present")
}
