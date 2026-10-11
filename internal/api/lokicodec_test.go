package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
