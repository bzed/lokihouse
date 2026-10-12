package api_test

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPI_RangeQuery(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)

	start := now.Add(-time.Hour)
	end := now.Add(time.Minute)
	q := url.Values{
		"query": {`{app="a"}`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"limit": {"100"},
	}

	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/query_range?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"resultType":"streams"`)
	require.Contains(t, body, "hello")
}
