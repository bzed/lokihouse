package ingest

import (
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

type captureLogger struct{ keyvals []any }

func (c *captureLogger) Log(kv ...any) error { c.keyvals = append(c.keyvals, kv...); return nil }

// Client-controlled values in the debug log must not carry line breaks, which
// could forge extra log lines in a plain-text sink.
func TestRecordParsedRequest_LogValuesHaveNoLineBreaks(t *testing.T) {
	r := httptest.NewRequest("POST", "/loki/api/v1/push", nil)
	r.URL.Path = "/loki/api/v1/push\nforged path=1"
	r.Header.Set("User-Agent", "agent\r\nlevel=error msg=forged")
	cl := &captureLogger{}
	var logger log.Logger = cl
	recordParsedRequest(r, logger, "t1", "json", &logproto.InternalPushRequest{}, &push.Stats{})
	require.NotEmpty(t, cl.keyvals)
	for _, v := range cl.keyvals {
		if s, ok := v.(string); ok {
			require.NotContains(t, s, "\n")
			require.NotContains(t, s, "\r")
		}
	}
}
