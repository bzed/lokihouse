package chstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestStreamIdentity_CanonicalOrder(t *testing.T) {
	a, ha, err := chstore.StreamIdentity(`{b="2", a="1"}`)
	require.NoError(t, err)
	b, hb, err := chstore.StreamIdentity(`{a="1",b="2"}`)
	require.NoError(t, err)
	require.Equal(t, ha, hb)
	require.Equal(t, `{a="1", b="2"}`, a.String())
	require.Equal(t, a.String(), b.String())
	require.Equal(t, labels.StableHash(labels.FromStrings("a", "1", "b", "2")), ha)
}

func TestWriter_RoundTrip(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	// Yesterday 23:59:59.123456789 UTC: the second entry crosses midnight.
	ts := time.Now().UTC().Truncate(24 * time.Hour).Add(-time.Second + 123456789)
	err := w.Write(ctx, "t1", []logproto.Stream{{
		Labels: `{job="x", app="y"}`,
		Entries: []logproto.Entry{
			{Timestamp: ts, Line: "hello", StructuredMetadata: logproto.FromLabelsToLabelAdapters(labels.FromStrings("trace_id", "abc"))},
			{Timestamp: ts.Add(2 * time.Second), Line: "next day"},
		},
	}})
	require.NoError(t, err)

	var got time.Time
	var line, trace string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT ts, line, structured_metadata['trace_id'] FROM logs WHERE tenant='t1' ORDER BY ts LIMIT 1").Scan(&got, &line, &trace))
	require.True(t, ts.Equal(got), "nanosecond precision preserved: %v vs %v", ts, got)
	require.Equal(t, "hello", line)
	require.Equal(t, "abc", trace)

	var days uint64
	var lstr string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(), any(labels_str) FROM streams FINAL WHERE tenant='t1'").Scan(&days, &lstr))
	require.Equal(t, uint64(2), days, "one streams row per UTC day touched")
	require.Equal(t, `{app="y", job="x"}`, lstr)
}

// streamInserts counts INSERTs into this database's streams table that reached ClickHouse.
func streamInserts(t *testing.T, conn driver.Conn, db string) uint64 {
	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, `SELECT count() FROM system.query_log
		WHERE type = 'QueryFinish' AND is_initial_query AND current_database = ? AND query LIKE 'INSERT INTO streams%'`, db).Scan(&n))
	return n
}

func TestWriter_RegistersStreamOncePerDay(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	now := time.Now().UTC()
	for i := range 3 {
		require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{
			Labels:  `{app="y"}`,
			Entries: []logproto.Entry{{Timestamp: now.Add(time.Duration(i) * time.Millisecond), Line: "x"}},
		}}))
	}
	require.Equal(t, uint64(1), streamInserts(t, conn, db), "known (tenant, stream, day) is not re-inserted")

	// A new tenant with the same labels is a new registration.
	require.NoError(t, w.Write(ctx, "t2", []logproto.Stream{{Labels: `{app="y"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "x"}}}}))
	require.Equal(t, uint64(2), streamInserts(t, conn, db))
}

// Loki dedups on (timestamp, line, structured metadata): background merges must keep
// entries that differ only in metadata and collapse exact re-pushes.
func TestWriter_MergeDedupKeyIncludesMetadata(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	ts := time.Now().UTC()
	md := func(kv ...string) []logproto.LabelAdapter {
		return logproto.FromLabelsToLabelAdapters(labels.FromStrings(kv...))
	}
	entries := []logproto.Entry{
		{Timestamp: ts, Line: "same", StructuredMetadata: md("trace_id", "a", "z", "1")},
		{Timestamp: ts, Line: "same", StructuredMetadata: md("trace_id", "b")},
	}
	for range 2 { // second round is an exact re-push
		require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="y"}`, Entries: entries}}))
	}
	topo := chtest.Topology()
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+topo.Local("logs")+" "+topo.OnCluster()+" FINAL"))
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs FINAL").Scan(&n))
	require.Equal(t, uint64(2), n)
}

// dailyEntries returns one entry per UTC day for the n days before today.
func dailyEntries(n int) []logproto.Entry {
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Hour)
	var entries []logproto.Entry
	for i := n; i > 0; i-- {
		entries = append(entries, logproto.Entry{Timestamp: day.AddDate(0, 0, -i), Line: fmt.Sprintf("day %d", i)})
	}
	return entries
}

// lastLogsInsertAsync reports whether the latest INSERT INTO logs in db ran as an async insert.
func lastLogsInsertAsync(t *testing.T, conn driver.Conn, db string) bool {
	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var v string
	require.NoError(t, conn.QueryRow(ctx, `SELECT Settings['async_insert'] FROM system.query_log
		WHERE type = 'QueryFinish' AND is_initial_query AND current_database = ? AND query LIKE 'INSERT INTO logs%'
		ORDER BY event_time_microseconds DESC LIMIT 1`, db).Scan(&v))
	// system.query_log lists only settings that differ from the server default,
	// which is async_insert=1 here: an explicit 0 is the only reliable signal.
	return v != "0"
}

func TestWriter_RejectsTooManyPartitions(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	err := w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="backfill"}`, Entries: dailyEntries(chstore.MaxPushPartitions + 1)}})
	require.True(t, errors.Is(err, chstore.ErrTooManyPartitions), "got %v", err)

	var logs, streams uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant='t1'").Scan(&logs))
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM streams WHERE tenant='t1'").Scan(&streams))
	require.Zero(t, logs, "a rejected push stores nothing")
	require.Zero(t, streams, "a rejected push registers no streams")
}

// The partition limit counts UTC days only: 60 days are accepted, 61 are not.
func TestWrite_TooManyDays(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	err := w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="a"}`, Entries: dailyEntries(61)}})
	require.ErrorIs(t, err, chstore.ErrTooManyPartitions)
	require.ErrorContains(t, err, "push spans 61 UTC days, at most 60 are accepted per push; split it by time range")
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="a"}`, Entries: dailyEntries(60)}}))
}

func TestWriter_WidePushIsSynchronous(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}

	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="live"}`, Entries: dailyEntries(1)}}))
	require.True(t, lastLogsInsertAsync(t, conn, db), "a narrow push uses the async-insert buffer")

	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="backfill"}`, Entries: dailyEntries(chstore.MaxPushPartitions)}}))
	require.False(t, lastLogsInsertAsync(t, conn, db), "a wide push bypasses the shared async buffer")

	var rows uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant='t1'").Scan(&rows))
	require.Equal(t, uint64(1+chstore.MaxPushPartitions), rows)
}
