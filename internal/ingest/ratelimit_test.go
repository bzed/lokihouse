package ingest_test

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
)

// rateLimited returns a handler whose tenants may ingest 104 bytes/s with a
// 104-byte burst (0.0001 MB), and its ClickHouse connection.
func rateLimited(t *testing.T) (*ingest.Handler, clickhouse.Conn) {
	t.Helper()
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.IngestionRateMB, l.IngestionBurstSizeMB = 0.0001, 0.0001
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger(), RateLimiter: ingest.NewRateLimiter(o)}, conn
}

func rows(t *testing.T, conn clickhouse.Conn, tenant string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant = ?", tenant).Scan(&n))
	return n
}

func TestRateLimit_RejectsWholePushAndIsolatesTenants(t *testing.T) {
	h, conn := rateLimited(t)
	ts := itoa(time.Now().UnixNano())
	small := `{"streams":[{"stream":{"app":"ok"},"values":[["` + ts + `","small"]]}]}`
	// Two streams: the first fits on its own, both together do not. The whole push is rejected.
	both := `{"streams":[{"stream":{"app":"ok"},"values":[["` + ts + `","small"]]},` +
		`{"stream":{"app":"big"},"values":[["` + ts + `","` + strings.Repeat("x", 200) + `"]]}]}`

	rec := push(t, h, "t1", both)
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "ingestion rate limit exceeded for user t1 (limit: 104 bytes/sec) while attempting to ingest '2' lines totaling '205' bytes")
	require.Equal(t, uint64(0), rows(t, conn, "t1"), "nothing is stored from a rejected push")

	rec = push(t, h, "t2", small)
	require.Equal(t, http.StatusNoContent, rec.Code, "other tenants have their own bucket")
	rec = push(t, h, "t1", small)
	require.Equal(t, http.StatusNoContent, rec.Code, "a rejected push consumes no tokens")
}

func TestRateLimit_DisabledWithoutLimiter(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()} // RateLimiter nil
	rec := push(t, h, "t1", `{"streams":[{"stream":{"app":"a"},"values":[["`+itoa(time.Now().UnixNano())+`","`+strings.Repeat("x", 100<<10)+`"]]}]}`)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}
