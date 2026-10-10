package chstore_test

import (
	"context"
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
	}}, nil)
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
		}}, nil))
	}
	require.Equal(t, uint64(1), streamInserts(t, conn, db), "known (tenant, stream, day) is not re-inserted")

	// A new tenant with the same labels is a new registration.
	require.NoError(t, w.Write(ctx, "t2", []logproto.Stream{{Labels: `{app="y"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "x"}}}}, nil))
	require.Equal(t, uint64(2), streamInserts(t, conn, db))
}

func TestWriter_RetentionDays(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	// Far older than any default: must survive because retention 0 means keep forever (Loki default).
	old := time.Now().UTC().Add(-400 * 24 * time.Hour)
	require.NoError(t, w.Write(ctx, "keep", []logproto.Stream{{Labels: `{app="y"}`, Entries: []logproto.Entry{{Timestamp: old, Line: "x"}}}}, nil))
	require.NoError(t, w.Write(ctx, "short", []logproto.Stream{{Labels: `{app="y"}`, Entries: []logproto.Entry{
		{Timestamp: old, Line: "expired"}, {Timestamp: time.Now(), Line: "fresh"},
	}}}, func(labels.Labels) uint16 { return 2 }))
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+chtest.Topology().Local("logs")+" "+chtest.Topology().OnCluster()+" FINAL"))

	var keep, short uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT countIf(tenant='keep'), countIf(tenant='short') FROM logs").Scan(&keep, &short))
	require.Equal(t, uint64(1), keep)
	require.Equal(t, uint64(1), short, "only the row older than 2 days expires")
	require.NoError(t, conn.QueryRow(ctx, "SELECT countIf(tenant='keep'), countIf(tenant='short') FROM streams").Scan(&keep, &short))
	require.Equal(t, uint64(1), keep)
	require.Equal(t, uint64(1), short, "expired stream registrations go with their rows")

	// Whole-part TTL drops need one expiry instant per part, at a UTC midnight.
	var misaligned uint64
	require.NoError(t, conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE database = ? AND active AND rows > 0
		AND (delete_ttl_info_min != delete_ttl_info_max OR toTimeZone(delete_ttl_info_min, 'UTC') != toStartOfDay(delete_ttl_info_min, 'UTC'))`, db).Scan(&misaligned))
	require.Zero(t, misaligned)
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
		require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="y"}`, Entries: entries}}, nil))
	}
	topo := chtest.Topology()
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+topo.Local("logs")+" "+topo.OnCluster()+" FINAL"))
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs FINAL").Scan(&n))
	require.Equal(t, uint64(2), n)
}
