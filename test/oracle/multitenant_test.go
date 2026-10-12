//go:build oracle

package oracle

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// multiTenantOracle compares Loki and lokihouse for a query whose
// X-Scope-OrgID names both fixture tenants ("tenant-a|tenant-b"): the same data
// and queries on both stores, compared for equality.
func multiTenantOracle(t *testing.T, loki, house string, start, end time.Time) {
	t.Helper()
	tenant := "tenant-a|tenant-b"
	queries := []struct {
		name, path, query string
	}{
		{"log", "/loki/api/v1/query_range", `{app="api"}`},
		{"metric by tenant", "/loki/api/v1/query_range", `sum by (__tenant_id__, app) (count_over_time({app=~".+"}[5m]))`},
		{"filtered tenant", "/loki/api/v1/query_range", `{app="api", __tenant_id__="tenant-b"}`},
	}
	for _, q := range queries {
		v := url.Values{"query": {q.query}, "limit": {"1000"},
			"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60s"}}
		a := Get(t, loki+q.path+"?"+v.Encode(), tenant)
		b := Get(t, house+q.path+"?"+v.Encode(), tenant)
		Compare(t, "multi-tenant "+q.name, a, b)
	}
	meta := []string{
		"/loki/api/v1/labels",
		"/loki/api/v1/label/__tenant_id__/values",
		"/loki/api/v1/series?match[]=" + url.QueryEscape(`{app="api"}`),
	}
	for _, p := range meta {
		sep := "?"
		if strings.Contains(p, "?") {
			sep = "&"
		}
		qs := sep + "start=" + strconv.FormatInt(start.UnixNano(), 10) + "&end=" + strconv.FormatInt(end.UnixNano(), 10)
		a := Get(t, loki+p+qs, tenant)
		b := Get(t, house+p+qs, tenant)
		Compare(t, "multi-tenant meta "+p, a, b)
	}
}
