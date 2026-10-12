package api_test

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func ns(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func TestQueryLimits(t *testing.T) {
	srv, _ := newServerConnWithLimits(t, true, func(l *validation.Limits) {
		l.MaxQueryLength = model.Duration(time.Hour)
		l.MaxQueryLookback = model.Duration(48 * time.Hour)
		l.MaxEntriesLimitPerQuery = 100
		l.MaxStreamsMatchersPerQuery = 2
		l.RequiredLabels = []string{"app"}
	})
	now := time.Now()
	get := func(path string, v url.Values) (int, string) {
		return do(t, http.MethodGet, srv.URL+path+"?"+v.Encode(), "t1", "")
	}
	rng := func(q string, start, end time.Time, limit int) url.Values {
		return url.Values{"query": {q}, "start": {ns(start)}, "end": {ns(end)}, "limit": {strconv.Itoa(limit)}}
	}

	code, body := get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-2*time.Hour), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "the query time range exceeds the limit (query length: 2h0m0s, limit: 1h)")

	code, body = get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-time.Minute), now, 1000))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "max entries limit per query exceeded, limit > max_entries_limit_per_query (1000 > 100)")

	code, body = get("/loki/api/v1/query_range", rng(`{app="a", env="b", pod="c"}`, now.Add(-time.Minute), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "max streams matchers per query exceeded, matchers-count > limit (3 > 2)")

	code, body = get("/loki/api/v1/query_range", rng(`{env="b"}`, now.Add(-time.Minute), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "stream selector is missing required matchers [app]")

	// Entirely before max_query_lookback: Loki answers with an empty result.
	code, body = get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-72*time.Hour), now.Add(-71*time.Hour), 10))
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"result":[]`)

	// Metric queries get the matcher checks too.
	code, _ = get("/loki/api/v1/query_range", url.Values{"query": {`count_over_time({env="b"}[1m])`}, "start": {ns(now.Add(-time.Minute))}, "end": {ns(now)}, "step": {"60"}})
	require.Equal(t, http.StatusBadRequest, code)
}

func TestQueryTimeout(t *testing.T) {
	srv, _ := newServerConnWithLimits(t, true, func(l *validation.Limits) { l.QueryTimeout = model.Duration(time.Nanosecond) })
	now := time.Now()
	code, _ := do(t, http.MethodGet, srv.URL+"/loki/api/v1/query_range?"+url.Values{
		"query": {`{app="a"}`}, "start": {ns(now.Add(-time.Hour))}, "end": {ns(now)},
	}.Encode(), "t1", "")
	require.Equal(t, http.StatusGatewayTimeout, code, "Loki maps a query timeout to 504")
}
