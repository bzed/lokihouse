package chstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// A log query stops after `limit` entries; closing the cursor must cancel the
// ClickHouse query instead of draining the rest of the result.
func TestRowReader_CloseCancelsQuery(t *testing.T) {
	addr := os.Getenv("LOKIHOUSE_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	rr, err := openCursor(context.Background(), conn,
		"SELECT toUInt64(0), now64(9), toString(number), CAST(map(), 'Map(String, String)') FROM system.numbers")
	require.NoError(t, err)
	require.True(t, rr.next(), "%v", rr.err)

	done := make(chan error, 1)
	go func() { done <- rr.close() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("close drained an unbounded result instead of cancelling")
	}
}
