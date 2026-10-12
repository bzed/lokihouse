package chstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// Two identical rows landing in the same (ts, stream) group must be dropped
// by rowReader's dedup, exactly like Loki's merge iterator.
func TestRowReader_DedupesIdenticalRows(t *testing.T) {
	addr := os.Getenv("LOKIHOUSE_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	sql := `SELECT toUInt64(1), fromUnixTimestamp64Nano(1, 'UTC'), 'x', CAST(map(), 'Map(String, String)'), toUInt64(7) UNION ALL SELECT toUInt64(1), fromUnixTimestamp64Nano(1, 'UTC'), 'x', CAST(map(), 'Map(String, String)'), toUInt64(7)`
	rr, err := openCursor(context.Background(), conn, sql)
	require.NoError(t, err)
	defer rr.close()
	require.True(t, rr.next())
	require.False(t, rr.next(), "identical row must be dropped")
	require.Empty(t, rr.errOf())
}

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
		"SELECT toUInt64(0), now64(9), toString(number), CAST(map(), 'Map(String, String)'), toUInt64(0) FROM system.numbers")
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
