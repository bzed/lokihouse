package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"
)

func TestAPI_Volume(t *testing.T) {
	now := time.Now()
	var vals []string
	for i, line := range []string{"a", "bb", "ccc"} {
		vals = append(vals, `["`+strconv.FormatInt(now.Add(time.Duration(i)*time.Second).UnixNano(), 10)+`","`+line+`"]`)
	}
	qs := url.Values{"query": {`{app="x"}`},
		"start": {strconv.FormatInt(now.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}

	// With volume_enabled = false the endpoint returns 404.
	srvOff, _ := newServerConnWithLimits(t, false, func(l *validation.Limits) { l.VolumeEnabled = false })
	code, body := do(t, "POST", srvOff.URL+"/loki/api/v1/push", "", `{"streams":[{"stream":{"app":"x"},"values":[`+strings.Join(vals, ",")+`]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)
	code, body = do(t, "GET", srvOff.URL+"/loki/api/v1/index/volume?"+qs.Encode(), "", "")
	require.Equal(t, http.StatusNotFound, code, "volume disabled → 404")

	// With volume_enabled = true (the default) the endpoint returns the volume.
	srvOn, _ := newServerConn(t, false)
	code, body = do(t, "POST", srvOn.URL+"/loki/api/v1/push", "", `{"streams":[{"stream":{"app":"x"},"values":[`+strings.Join(vals, ",")+`]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)
	code, body = do(t, "GET", srvOn.URL+"/loki/api/v1/index/volume?"+qs.Encode(), "", "")
	require.Equal(t, http.StatusOK, code, body)
	var r struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	require.Equal(t, "success", r.Status)
	require.Equal(t, "vector", r.Data.ResultType)
	require.Len(t, r.Data.Result, 1)
	require.Equal(t, "x", r.Data.Result[0].Metric["app"])
}

func TestAPI_IndexStats(t *testing.T) {
	srv, conn := newServerConn(t, false)
	now := time.Now()
	var vals []string
	for i, line := range []string{"a", "bb", "ccc"} {
		vals = append(vals, `["`+strconv.FormatInt(now.Add(time.Duration(i)*time.Second).UnixNano(), 10)+`","`+line+`"]`)
	}
	code, body := do(t, "POST", srv.URL+"/loki/api/v1/push", "", `{"streams":[{"stream":{"app":"x"},"values":[`+strings.Join(vals, ",")+`]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)

	var wantBytes uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT sum(length(line) + 8 * length(mapKeys(structured_metadata))) FROM logs WHERE tenant = 'fake'").Scan(&wantBytes))

	qs := url.Values{"query": {`{app="x"}`},
		"start": {strconv.FormatInt(now.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body = do(t, "GET", srv.URL+"/loki/api/v1/index/stats?"+qs.Encode(), "", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, fmt.Sprintf(`{"streams":1,"chunks":1,"bytes":%d,"entries":3}`, wantBytes), body)
}
