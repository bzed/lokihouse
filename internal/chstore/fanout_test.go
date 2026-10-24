package chstore_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// seedFanoutData writes two streams whose entries share timestamps across the
// streams, logfmt lines with a level and a duration, and exact duplicates
// re-pushed into unmerged parts.
func seedFanoutData(t *testing.T, conn driver.Conn) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	var a, b []logproto.Entry
	for i := range 180 {
		ts := t0.Add(time.Duration(i) * time.Minute)
		a = append(a, logproto.Entry{Timestamp: ts, Line: fmt.Sprintf("level=%s duration=%dms", []string{"info", "warn"}[i%2], 10+i)})
		b = append(b, logproto.Entry{Timestamp: ts, Line: fmt.Sprintf("level=%s duration=%dms", []string{"warn", "error"}[i%2], 100+i)})
	}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: a}, logproto.Stream{Labels: `{app="b"}`, Entries: b})
	for range 3 { // exact duplicates in unmerged parts
		seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: a[60:61]})
	}
}

// queriesSince returns the finished Select queries logged since t that contain substr.
func queriesSince(t *testing.T, conn driver.Conn, db string, since time.Time, substr string) []string {
	t.Helper()
	flush, src := "SYSTEM FLUSH LOGS query_log", "system.query_log"
	if topo := chtest.Topology(); topo.Cluster != "" {
		flush = "SYSTEM FLUSH LOGS ON CLUSTER " + topo.Cluster
		src = fmt.Sprintf("clusterAllReplicas(%s, system, query_log)", topo.Cluster)
	}
	require.NoError(t, conn.Exec(context.Background(), flush))
	rows, err := conn.Query(context.Background(), "SELECT query FROM "+src+" WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", db, since.UnixMicro(), substr)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	require.NoError(t, rows.Err())
	return out
}

// A fanned-out sample read (one goroutine per stream, merged in Go) returns the
// same result as the single-query global read, whatever the window size and
// worker count.
func TestSelectSamples_FanoutSameResult(t *testing.T) {
	conn, _ := chtest.New(t)
	seedFanoutData(t, conn)
	t.Cleanup(func() { chstore.SetSampleFanoutMax(16) })
	queries := []string{
		`count_over_time({app=~"a|b"}[5m])`,
		`sum by (level) (rate({app=~"a|b"} | logfmt [5m]))`,
		`quantile_over_time(0.95, {app=~"a|b"} | logfmt | unwrap duration(duration) [5m]) by (app)`,
		`bytes_over_time({app=~"a|b"}[5m])`,
	}
	start, end := t0, t0.Add(3*time.Hour)
	for _, qs := range queries {
		chstore.SetSampleFanoutMax(0) // one query, global order
		want := run(t, engine(t, &chstore.Querier{Conn: conn}), qs, start, end, time.Minute, logproto.FORWARD, 0)
		require.NotEmpty(t, want.Data)
		for _, rows := range []uint64{0, 7} {
			chstore.SetSampleFanoutMax(16) // one goroutine per stream
			got := run(t, engine(t, &chstore.Querier{Conn: conn, WindowRows: rows}), qs, start, end, time.Minute, logproto.FORWARD, 0)
			require.Equal(t, want.Data, got.Data, "%s windowRows=%d", qs, rows)
			got = run(t, engine(t, &chstore.Querier{Conn: conn, WindowRows: rows, Workers: 4}), qs, start, end, time.Minute, logproto.FORWARD, 0)
			require.Equal(t, want.Data, got.Data, "%s windowRows=%d workers=4", qs, rows)
		}
	}
}

// The fan-out merges the per-stream reads in the exact global
// (ts, stream_id, entry_hash) order of the single-query read: equal timestamps
// order by stream ID, and exact duplicates collapse to one sample.
func TestSelectSamples_FanoutStreamOrder(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "a1"}, {Timestamp: t0.Add(2 * time.Second), Line: "a2"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "b1"}, {Timestamp: t0.Add(time.Second), Line: "b2"}}},
		logproto.Stream{Labels: `{app="c"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "c1"}}},
	)
	seed(t, w, "t1", logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "b1"}}}) // exact duplicate

	// the expected order of the t0 samples: by stream ID
	ctx := user.InjectOrgID(context.Background(), "t1")
	ms, err := syntax.ParseMatchers(`{app=~"a|b|c"}`, false)
	require.NoError(t, err)
	streams, err := chstore.ResolveStreams(ctx, conn, "t1", ms, t0.Add(-time.Minute), t0.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, streams, 3)
	sort.Slice(streams, func(i, j int) bool { return streams[i].ID < streams[j].ID })

	it := openSamples(ctx, t, &chstore.Querier{Conn: conn}, `count_over_time({app=~"a|b|c"}[1m])`, t0.Add(-time.Minute), t0.Add(time.Minute))
	defer it.Close()
	var got []string
	var last int64
	for it.Next() {
		s := it.At()
		require.GreaterOrEqual(t, s.Timestamp, last, "timestamps never go backwards")
		last = s.Timestamp
		got = append(got, fmt.Sprintf("%d %s", s.Timestamp, it.Labels()))
	}
	require.NoError(t, it.Err())
	want := []string{
		fmt.Sprintf("%d %s", t0.UnixNano(), streams[0].Labels.String()),
		fmt.Sprintf("%d %s", t0.UnixNano(), streams[1].Labels.String()),
		fmt.Sprintf("%d %s", t0.UnixNano(), streams[2].Labels.String()),
		fmt.Sprintf("%d {app=\"b\"}", t0.Add(time.Second).UnixNano()),
		fmt.Sprintf("%d {app=\"a\"}", t0.Add(2*time.Second).UnixNano()),
	}
	require.Equal(t, want, got)
}

// The fan-out opens every read window once per stream.
func TestSelectSamples_FanoutOpensWindowsPerStream(t *testing.T) {
	conn, _ := chtest.New(t)
	seedWindowData(t, conn) // two streams, three hours
	t.Cleanup(func() { chstore.SetSampleFanoutMax(16) })
	q := &chstore.Querier{Conn: conn, WindowRows: 60}
	collect := func() int {
		it := openSamples(context.Background(), t, q, `count_over_time({app=~"a|b"}[5m])`, t0, t0.Add(3*time.Hour))
		for it.Next() {
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return chstore.OpenedSampleWindows(it)
	}
	chstore.SetSampleFanoutMax(0)
	single := collect()
	require.Positive(t, single)
	chstore.SetSampleFanoutMax(16)
	require.Equal(t, 2*single, collect(), "one window set per stream")
}

// A fanned-out sample query reads one query per stream, in the part's physical
// order and without FINAL (the row reader dedups the adjacent duplicates), and
// over a range of at most two hours skips the hourly-stats round trip.
func TestSelectSamples_FanoutQueryShape(t *testing.T) {
	conn, db := chtest.New(t)
	seedFanoutData(t, conn)
	q := &chstore.Querier{Conn: conn}
	since := time.Now()
	it := openSamples(user.InjectOrgID(context.Background(), "t1"), t, q, `count_over_time({app=~"a|b"}[5m])`, t0.Add(-time.Minute), t0.Add(90*time.Minute))
	for it.Next() {
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	rows := queriesSince(t, conn, db, since, "FROM logs")
	require.Len(t, rows, 2, "one query per stream, one window")
	for _, qs := range rows {
		require.Contains(t, qs, "FROM logs WHERE tenant = 't1' AND stream_id = ", qs)
		require.NotContains(t, qs, "FINAL", qs)
		require.NotContains(t, qs, "lh_stream_ids", qs)
		require.Contains(t, qs, "ORDER BY stream_id ASC, ts ASC, entry_hash ASC", qs)
	}
	require.Empty(t, queriesSince(t, conn, db, since, "stream_stats_hourly"), "no stats query below two hours")
}

// Above the fan-out limit the sample query reads all streams in one query in
// the global order, planning windows from the hourly stats as before.
func TestSelectSamples_AboveFanoutMaxReadsGlobally(t *testing.T) {
	conn, db := chtest.New(t)
	var streams []logproto.Stream
	for s := range 17 {
		streams = append(streams, logproto.Stream{Labels: fmt.Sprintf(`{app="s%02d"}`, s),
			Entries: []logproto.Entry{{Timestamp: t0.Add(time.Duration(s) * time.Second), Line: fmt.Sprint("n", s)}}})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", streams...)
	seed(t, &chstore.Writer{Conn: conn}, "t1", streams[0]) // exact duplicate in a second part: FINAL collapses it
	q := &chstore.Querier{Conn: conn}
	since := time.Now()
	it := openSamples(user.InjectOrgID(context.Background(), "t1"), t, q, `count_over_time({app=~"s[0-9]+"}[5m])`, t0.Add(-time.Minute), t0.Add(90*time.Minute))
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, 17, n, "the duplicate counts once")

	rows := queriesSince(t, conn, db, since, "FROM logs")
	require.Len(t, rows, 1, "one query for all streams")
	require.Contains(t, rows[0], "lh_stream_ids", rows[0])
	require.Contains(t, rows[0], "ORDER BY ts ASC, stream_id ASC, entry_hash", rows[0])
	require.NotEmpty(t, queriesSince(t, conn, db, since, "stream_stats_hourly"), "the global read plans windows from the stats above one hour")
}

// Above the fan-out limit the read keeps FINAL: the text index must still skip
// granules of an unmerged table, and FINAL must still collapse the duplicates
// the newer part re-pushes.
func TestSelectSamples_TextIndexAboveFanoutMax(t *testing.T) {
	const perStream = 10000
	var streams []logproto.Stream
	for s := range 17 {
		var es []logproto.Entry
		for i := range perStream {
			line := fmt.Sprint("row ", i)
			if s == 5 && i == 3133 {
				line += " needle here"
			}
			es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Millisecond), Line: line})
		}
		streams = append(streams, logproto.Stream{Labels: fmt.Sprintf(`{app="s%02d"}`, s), Entries: es})
	}
	conn, db := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1", streams...)
	seed(t, w, "t1", logproto.Stream{Labels: streams[5].Labels, Entries: streams[5].Entries[3133:3134]}) // exact duplicate in a second part

	since := time.Now()
	end := t0.Add(time.Hour)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `count_over_time({app=~"s[0-9]+"} |= "needle" [1h])`, end, end, 0, logproto.FORWARD, 0)
	v := res.Data.(promql.Vector)
	require.Len(t, v, 1)
	require.Equal(t, 1.0, v[0].F, "the duplicate counts once")
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(17*perStream/2))
}
