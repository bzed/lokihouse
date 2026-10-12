package api_test

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPI_SeriesAndLabelValues(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)
	q := url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}

	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/label/env/values?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, `{"status":"success","data":["dev"]}`, body)

	q.Set("match[]", `{app="a"}`)
	code, body = do(t, http.MethodGet, srv.URL+"/loki/api/v1/series?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	// The response may include discovered fields like service_name
	require.Contains(t, body, `"app":"a"`)
	require.Contains(t, body, `"env":"dev"`)
}

func TestAPI_InstantQuery(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)
	q := url.Values{"query": {`count_over_time({app="a"}[5m])`}, "time": {strconv.FormatInt(now.Add(time.Second).UnixNano(), 10)}}
	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"resultType":"vector"`)

	q.Set("query", `{app="a"}`) // a log query is not a valid instant query (Loki)
	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusBadRequest, code)

	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?query=%7B", "t1", "")
	require.Equal(t, http.StatusBadRequest, code)
}

func TestAPI_BuildInfo(t *testing.T) {
	srv := newServer(t, false)
	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/status/buildinfo", "", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, `"goVersion"`)
}

func TestAPI_LabelValuesByNameAndBadQueries(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)
	q := url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}

	// /label/{name}/values sets req.Name via the path value.
	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/label/env/values?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, `{"status":"success","data":["dev"]}`, body)

	// A malformed series query is a 400, not a panic.
	badMatch := url.Values{"match": {"{bad"}}
	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/series?"+q.Encode()+"&"+badMatch.Encode(), "t1", "")
	require.Equal(t, http.StatusBadRequest, code)
}
