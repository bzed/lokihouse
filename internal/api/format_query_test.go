package api

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"
)

func formatAPI() *API {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, _ := validation.NewOverrides(l, nil)
	return &API{Limits: o, AuthEnabled: true}
}

func doFormat(t *testing.T, a *API, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/loki/api/v1/format_query?"+values.Encode(), nil)
	w := httptest.NewRecorder()
	a.Router().ServeHTTP(w, req)
	return w
}

func TestFormatQuery_NoTenantRequired(t *testing.T) {
	// auth_enabled: true, but format_query is registered without the tenant
	// wrapper, so no X-Scope-OrgID header must still return 200.
	a := formatAPI()
	q := `sum(rate({a="b"}[1m]))`
	w := doFormat(t, a, url.Values{"query": {q}})
	require.Equal(t, http.StatusOK, w.Code)
	var got FormatQueryResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, "success", got.Status)
	require.Equal(t, syntax.Prettify(syntax.MustParseExpr(q)), got.Data)
	require.Empty(t, got.Err)
}

func TestFormatQuery_InvalidQuery(t *testing.T) {
	a := formatAPI()
	w := doFormat(t, a, url.Values{"query": {`{a="b"`}})
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.True(t, strings.Contains(w.Body.String(), `"status":"invalid-query"`), "body: %s", w.Body.String())
	require.True(t, strings.Contains(w.Body.String(), `"error":"`), "body: %s", w.Body.String())
}
