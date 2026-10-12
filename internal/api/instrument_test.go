package api

import (
	"testing"

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
