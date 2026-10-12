package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// routeName is the dskit route label: the ServeMux pattern minus its method,
// non-alphanumerics folded to underscores, trimmed and lowercased; empty is "other".
func TestRouteName(t *testing.T) {
	for in, want := range map[string]string{
		"":                                     "other",
		"GET /loki/api/v1/query":               "loki_api_v1_query",
		"GET /loki/api/v1/label/{name}/values": "loki_api_v1_label_name_values",
		"POST /api/prom/push":                  "api_prom_push",
		"/loki/api/v1/series":                  "loki_api_v1_series",
	} {
		require.Equal(t, want, routeName(in), in)
	}
}

// methodLabels returns the method label values loki_request_duration_seconds
// has seen.
func methodLabels(t *testing.T) map[string]bool {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(requestDuration))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]bool{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "method" {
					out[l.GetValue()] = true
				}
			}
		}
	}
	return out
}

// An arbitrary request method must not become a label value: each one would
// be a new series (unbounded cardinality from untrusted input).
func TestInstrument_UnknownMethodIsOther(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(http.ResponseWriter, *http.Request) {})
	h := instrument(mux, mux)
	for _, m := range []string{"BOGUS", "get", http.MethodGet} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(m, "/x", nil))
	}
	got := methodLabels(t)
	require.False(t, got["BOGUS"])
	require.False(t, got["get"])
	require.True(t, got["other"])
	require.True(t, got[http.MethodGet])
}
