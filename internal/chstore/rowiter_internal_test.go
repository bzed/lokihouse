package chstore

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/prometheus/model/labels"
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

// scanRows runs literal rows (n, stream_id, ts, line, metadata, entry_hash) through
// rowReader in the order of n and returns the yielded (line, metadata) pairs.
func scanRows(t *testing.T, rows ...string) []string {
	t.Helper()
	addr := os.Getenv("LOKIHOUSE_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	sql := "SELECT stream_id, ts, line, md, entry_hash FROM (" + strings.Join(rows, " UNION ALL ") + ") ORDER BY n"
	rr, err := openCursor(context.Background(), conn, sql)
	require.NoError(t, err)
	defer rr.close()
	var got []string
	for rr.next() {
		got = append(got, rr.cur().line+" "+mdString(rr.cur().md))
	}
	require.NoError(t, rr.errOf())
	return got
}

func mdString(md map[string]string) string { return labels.FromMap(md).String() }

func literalRow(n, ts int, line, md string, hash int) string {
	return fmt.Sprintf("SELECT %d AS n, toUInt64(1) AS stream_id, fromUnixTimestamp64Nano(%d, 'UTC') AS ts, '%s' AS line, CAST(%s, 'Map(String, String)') AS md, toUInt64(%d) AS entry_hash", n, ts, line, md, hash)
}

// A, B, A within one group (a hash collision between x and y) drops only the second A.
func TestRowReader_DedupesHashCollisionGroup(t *testing.T) {
	got := scanRows(t, literalRow(1, 1, "x", "map()", 7), literalRow(2, 1, "y", "map()", 7), literalRow(3, 1, "x", "map()", 7))
	require.Equal(t, []string{"x {}", "y {}"}, got)
}

// Rows that differ only in structured metadata are not duplicates.
func TestRowReader_KeepsMetadataOnlyDifference(t *testing.T) {
	got := scanRows(t, literalRow(1, 1, "x", "map('a','1')", 7), literalRow(2, 1, "x", "map('a','2')", 7))
	require.Equal(t, []string{`x {a="1"}`, `x {a="2"}`}, got)
}

// A new (ts, stream, hash) group forgets what the previous group saw.
func TestRowReader_NewGroupResetsSeen(t *testing.T) {
	got := scanRows(t, literalRow(1, 1, "x", "map()", 7), literalRow(2, 2, "x", "map()", 7))
	require.Equal(t, []string{"x {}", "x {}"}, got)
}
