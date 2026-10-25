package chstore_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/plan"
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
	return queryTypeSince(t, conn, db, since, "QueryFinish", substr)
}

// queryStartsSince returns the started Select queries logged since t that
// contain substr. Pool tests count starts, not finishes: the engine may stop
// consuming before the cursor ends (samples past the last evaluated step
// window), and Close then cancels the query mid-stream, which the server logs
// as ExceptionWhileProcessing rather than QueryFinish.
func queryStartsSince(t *testing.T, conn driver.Conn, db string, since time.Time, substr string) []string {
	t.Helper()
	return queryTypeSince(t, conn, db, since, "QueryStart", substr)
}

func queryTypeSince(t *testing.T, conn driver.Conn, db string, since time.Time, typ, substr string) []string {
	t.Helper()
	flush, src := "SYSTEM FLUSH LOGS query_log", "system.query_log"
	if topo := chtest.Topology(); topo.Cluster != "" {
		flush = "SYSTEM FLUSH LOGS ON CLUSTER " + topo.Cluster
		src = fmt.Sprintf("clusterAllReplicas(%s, system, query_log)", topo.Cluster)
	}
	require.NoError(t, conn.Exec(context.Background(), flush))
	rows, err := conn.Query(context.Background(), "SELECT query FROM "+src+" WHERE type = ? AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", typ, db, since.UnixMicro(), substr)
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

// openSamplesQuery is openSamples without the test assertions, for use from
// goroutines: it runs qs against q through SelectSamples directly, with no
// engine, so the read spans the whole range.
func openSamplesQuery(ctx context.Context, q *chstore.Querier, qs string, start, end time.Time) (iter.SampleIterator, error) {
	expr, err := syntax.ParseSampleExpr(qs)
	if err != nil {
		return nil, err
	}
	return q.SelectSamples(user.InjectOrgID(ctx, "t1"), logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: start, End: end, Plan: &plan.QueryPlan{AST: expr},
	}})
}

// drainSamples collects the samples of it as comparable strings.
func drainSamples(it iter.SampleIterator) ([]string, error) {
	var out []string
	for it.Next() {
		s := it.At()
		out = append(out, fmt.Sprintf("%s %d %v", it.Labels(), s.Timestamp, s.Value))
	}
	return out, it.Err()
}

// collectSamplesQuery runs qs against q through SelectSamples directly — no
// engine, so the read spans the whole range — and drains the iterator fully.
func collectSamplesQuery(ctx context.Context, q *chstore.Querier, qs string, start, end time.Time) ([]string, error) {
	it, err := openSamplesQuery(ctx, q, qs, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	return drainSamples(it)
}

// A fanned-out sample read (one goroutine per stream, merged in Go) returns the
// same result as the single-query global read, whatever the window size and
// worker count.
func TestSelectSamples_FanoutSameResult(t *testing.T) {
	conn, _ := chtest.New(t)
	seedFanoutData(t, conn)
	queries := []string{
		`count_over_time({app=~"a|b"}[5m])`,
		`sum by (level) (rate({app=~"a|b"} | logfmt [5m]))`,
		`quantile_over_time(0.95, {app=~"a|b"} | logfmt | unwrap duration(duration) [5m]) by (app)`,
		`bytes_over_time({app=~"a|b"}[5m])`,
	}
	start, end := t0, t0.Add(3*time.Hour)
	for _, qs := range queries {
		// one query, global order: two streams never fan out at FanoutMax 1
		want := run(t, engine(t, &chstore.Querier{Conn: conn, FanoutMax: 1}), qs, start, end, time.Minute, logproto.FORWARD, 0)
		require.NotEmpty(t, want.Data)
		for _, rows := range []uint64{0, 7} {
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

// The fan-out reads the whole range in one window per stream: a per-stream
// in-order read has no sort for a window to bound, so the hourly stats are
// never consulted, whatever the range.
func TestSelectSamples_FanoutSingleWindow(t *testing.T) {
	conn, db := chtest.New(t)
	seedWindowData(t, conn) // two streams, three hours
	collect := func(q *chstore.Querier) int {
		it := openSamples(context.Background(), t, q, `count_over_time({app=~"a|b"}[5m])`, t0, t0.Add(3*time.Hour))
		for it.Next() {
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return chstore.OpenedSampleWindows(it)
	}
	single := collect(&chstore.Querier{Conn: conn, WindowRows: 60, FanoutMax: 1})
	require.Greater(t, single, 2, "the global read plans several windows from the stats")
	since := time.Now()
	require.Equal(t, 2, collect(&chstore.Querier{Conn: conn, WindowRows: 60}), "one window per stream")
	require.Empty(t, queriesSince(t, conn, db, since, "stream_stats_hourly"), "the fan-out never consults the stats")
}

// A fanned-out sample query reads one query per stream, in the part's physical
// order and without FINAL (the row reader dedups the adjacent duplicates), and
// reads the whole range in one window: the hourly-stats round trip is skipped
// at any range.
func TestSelectSamples_FanoutQueryShape(t *testing.T) {
	conn, db := chtest.New(t)
	seedFanoutData(t, conn)
	q := &chstore.Querier{Conn: conn}
	since := time.Now()
	it := openSamples(user.InjectOrgID(context.Background(), "t1"), t, q, `count_over_time({app=~"a|b"}[5m])`, t0.Add(-time.Minute), t0.Add(3*time.Hour))
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
	require.Empty(t, queriesSince(t, conn, db, since, "stream_stats_hourly"), "no stats query at any range")
}

// A fan-out reserves one connection per stream, all or nothing. A query whose
// reservation (FanoutConns) fails reads all streams in one query instead.
func TestSelectSamples_FanoutFallsBackWhenConnsBusy(t *testing.T) {
	conn, db := chtest.New(t)
	seedFanoutData(t, conn)
	qs := `count_over_time({app=~"a|b"}[5m])`
	start, end := t0.Add(-time.Minute), t0.Add(3*time.Hour)
	want := run(t, engine(t, &chstore.Querier{Conn: conn}), qs, start, end, time.Minute, logproto.FORWARD, 0)
	since := time.Now()
	// A budget of one connection: two streams can never fan out.
	got := run(t, engine(t, &chstore.Querier{Conn: conn, FanoutConns: 1}), qs, start, end, time.Minute, logproto.FORWARD, 0)
	require.Equal(t, want.Data, got.Data)

	rows := queryStartsSince(t, conn, db, since, "FROM logs")
	require.Len(t, rows, 1, "one query for all streams")
	require.Contains(t, rows[0], "FINAL", rows[0])
	require.Contains(t, rows[0], "lh_stream_ids", rows[0])
}

// The connection reservation (FanoutConns, wired to half the pool in
// production) is all or nothing: a fan-out reserves one connection per
// stream, and when the reservation cannot be taken in full the query reads
// all streams in one query instead. With a budget of two, a six-stream query
// never fans out — and a pool of four proves the one-query read needs only
// one connection. (When the budget is granted, a pool smaller than the
// fan-out still succeeds: see TestSelectSamples_FanoutBudgetAbovePoolSucceeds.)
func TestSelectSamples_FanoutPoolTooSmallFallsBack(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 6)
	ctx := context.Background()
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream (a 50 ms span)
	want, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: conn}, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	// A pool of four connections cannot hold a six-stream fan-out.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 4, 4
	opts.DialTimeout = 2 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: the hold engages
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	since := time.Now()
	got, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: pool, FanoutConns: 2}, qs, start, end)
	require.NoError(t, err)
	require.Equal(t, want, got)

	rows := queryStartsSince(t, conn, db, since, "FROM logs")
	require.Len(t, rows, 1, "one query for all streams")
	require.Contains(t, rows[0], "FINAL", rows[0])
	require.Contains(t, rows[0], "lh_stream_ids", rows[0])
}

// seedPoolStreams writes n streams of 50 000 rows each over a 50 ms range, in
// 10 000-row chunks. The ~200-byte lines make a stream carry about ten
// megabytes: at the pool's max_block_size of 500 that is a hundred blocks, so
// a stream's transfer spans many blocks and concurrent streams overlap on the
// pool's connections for a long time — the pool tests need that overlap. (With
// a megabyte per stream the server finishes into the socket and driver buffers
// almost at once and the overlap vanishes; that is why the streams are this
// large.)
func seedPoolStreams(t *testing.T, conn driver.Conn, n int) {
	t.Helper()
	seedPoolStreamsRows(t, conn, n, 50000)
}

// seedPoolStreamsRows writes n streams of rows rows each, one row per
// microsecond from t0, in 10 000-row chunks.
func seedPoolStreamsRows(t *testing.T, conn driver.Conn, n, rows int) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	const chunk = 10000
	for base := 0; base < rows; base += chunk {
		var streams []logproto.Stream
		for s := range n {
			es := make([]logproto.Entry, 0, chunk)
			for i := base; i < base+chunk; i++ {
				es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Microsecond), Line: fmt.Sprintf("row %06d %s", i, strings.Repeat("x", 192))})
			}
			streams = append(streams, logproto.Stream{Labels: fmt.Sprintf(`{app="s%02d"}`, s), Entries: es})
		}
		seed(t, w, "t1", streams...)
	}
}

// Concurrent fan-outs share the FanoutConns budget all or nothing: while the
// whole budget is held, a query reads all streams in one query; with the
// budget free it fans out. Both succeed on a pool that holds one fan-out plus
// headroom.
func TestSelectSamples_FanoutConcurrentPool(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 8)
	ctx := context.Background()
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream

	// A pool that holds one fan-out (8 connections) plus headroom.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 12, 12
	opts.DialTimeout = 10 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: a long transfer
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	q := &chstore.Querier{Conn: pool, FanoutConns: 8}
	want, err := collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	// Hold the whole budget: the next query cannot reserve eight connections
	// and reads all streams in one query.
	require.True(t, chstore.AcquireFanoutBudget(q, 8))
	since := time.Now()
	got, err := collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.Equal(t, want, got)
	rows := queryStartsSince(t, conn, db, since, "FROM logs")
	require.Len(t, rows, 1, "one query for all streams")
	require.Contains(t, rows[0], "FINAL", rows[0])
	require.Contains(t, rows[0], "lh_stream_ids", rows[0])
	chstore.ReleaseFanoutBudget(q, 8)

	// With the budget free the query fans out: one query per stream.
	since = time.Now()
	got, err = collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Len(t, queryStartsSince(t, conn, db, since, "FROM logs"), 8, "one query per stream")
}

// A sample query that cannot take the fan-out's connection reservation in
// full reads all streams in one query — and says so: the fallback is counted
// in lokihouse_chstore_fanout_fallbacks_total and written to the querier's
// log, so a connection budget that is too small is detectable.
func TestSelectSamples_FanoutFallbackCountedAndLogged(t *testing.T) {
	conn, _ := chtest.New(t)
	seedFanoutData(t, conn) // two streams
	ctx := context.Background()
	qs := `count_over_time({app=~"a|b"}[5m])`
	start, end := t0.Add(-time.Minute), t0.Add(3*time.Hour)

	var logBuf bytes.Buffer
	// A budget of one connection cannot hold the fan-out's two streams.
	q := &chstore.Querier{Conn: conn, FanoutConns: 1, Logger: log.NewLogfmtLogger(&logBuf)}
	budgetBefore := chstore.FanoutFallbackCount("budget")
	capBefore := chstore.FanoutFallbackCount("cap")
	got, err := collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	require.Equal(t, budgetBefore+1, chstore.FanoutFallbackCount("budget"), "the reservation fallback is counted")
	require.Equal(t, capBefore, chstore.FanoutFallbackCount("cap"), "no sample-cap fallback")
	require.Contains(t, logBuf.String(), "instead of fanning out")
	require.Contains(t, logBuf.String(), "reason=budget")
	require.Contains(t, logBuf.String(), "tenant=t1")
}

// Enough concurrent one-connection queries to saturate a small pool: the pool
// queues them (its acquire blocks until a connection frees), and every query
// succeeds. Saturation is waiting, not deadlock — each running query holds
// its connection only while it streams to a consumer that drains it.
func TestSelectSamples_SingleConnQueriesQueueOnSmallPool(t *testing.T) {
	conn, db := chtest.New(t)
	seedFanoutData(t, conn) // two streams
	ctx := context.Background()
	qs := `count_over_time({app=~"a|b"}[5m])`
	start, end := t0.Add(-time.Minute), t0.Add(3*time.Hour)
	want, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: conn}, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	// Four connections, twelve concurrent queries that each hold one.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 4, 4
	opts.DialTimeout = 10 * time.Second
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	q := &chstore.Querier{Conn: pool, FanoutMax: 1} // one query, one connection

	const concurrent = 12
	type outcome struct {
		samples []string
		err     error
	}
	out := make(chan outcome, concurrent)
	for range concurrent {
		go func() {
			samples, err := collectSamplesQuery(ctx, q, qs, start, end)
			out <- outcome{samples, err}
		}()
	}
	for range concurrent {
		o := <-out
		require.NoError(t, o.err)
		require.Equal(t, want, o.samples)
	}
}

// Many concurrent fanned-out queries on a pool that cannot hold them all:
// each fan-out drains its streams before the query returns, holding its
// connections only while it transfers and waiting on nothing, so the
// streams queue on the pool and every query succeeds — whatever the mix of
// fan-outs (three connections each, within the budget) and one-query reads
// (one each) the all-or-nothing reservation admits. Before the drain, the
// fan-outs held connections while waiting for each other and failed on the
// pool-acquire timeout.
func TestSelectSamples_FanoutUnderConcurrentLoad(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 3)
	ctx := context.Background()
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream

	// Half the pool for the fan-out budget, as production wires it: one
	// 3-stream fan-out (3 connections) at a time, the rest one-query reads
	// (1 each) — twelve concurrent queries need far more than the pool of 6.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 6, 6
	opts.DialTimeout = 30 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: a long transfer
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	q := &chstore.Querier{Conn: pool, FanoutConns: 3}
	want, err := collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)
	since := time.Now()

	const concurrent = 12
	type outcome struct {
		samples []string
		err     error
	}
	out := make(chan outcome, concurrent)
	for range concurrent {
		go func() {
			samples, err := collectSamplesQuery(ctx, q, qs, start, end)
			out <- outcome{samples, err}
		}()
	}
	for range concurrent {
		o := <-out
		require.NoError(t, o.err)
		require.Equal(t, want, o.samples)
	}

	// Every query read either one query per stream (a fan-out within the
	// budget) or one query over all streams (the reservation failed): twelve
	// queries in total, none failed on the pool.
	var perStream, global int
	for _, logged := range queryStartsSince(t, conn, db, since, "FROM logs") {
		if strings.Contains(logged, "FINAL") {
			global++
		} else {
			perStream++
		}
	}
	require.Zero(t, perStream%3, "per-stream queries come in fan-outs of three")
	require.Equal(t, concurrent, perStream/3+global, "every query read exactly one group of queries")
}

// The connection reservation is a budget, not a set of connections: a budget
// above the pool size is granted, and the fan-out's streams queue on the pool
// — every stream drains its query as fast as the server sends and waits on
// nothing, so a pool smaller than the fan-out cannot deadlock. (The streaming
// merge this replaces needed a row of every stream before emitting one, so a
// stream still waiting for a connection stalled the started streams, which
// held theirs: a circular wait, broken only by the pool-acquire timeout.)
func TestSelectSamples_FanoutBudgetAbovePoolSucceeds(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 6)
	ctx := context.Background()
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream

	// A pool of four connections cannot hold a six-stream fan-out.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 4, 4
	opts.DialTimeout = 30 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: a long transfer
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	want, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: conn}, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	since := time.Now()
	got, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: pool, FanoutConns: 6}, qs, start, end)
	require.NoError(t, err, "a fan-out on a pool smaller than its stream count must not deadlock")
	require.Equal(t, want, got)
	require.Len(t, queryStartsSince(t, conn, db, since, "FROM logs"), 6, "one query per stream")
}

// runQuery is run without the assertions, for use from goroutines.
func runQuery(ng *logql.QueryEngine, qs string, start, end time.Time, step time.Duration) (logqlmodel.Result, error) {
	p, err := logql.NewLiteralParams(qs, start, end, step, 0, logproto.FORWARD, 0, nil, nil)
	if err != nil {
		return logqlmodel.Result{}, err
	}
	return ng.Query(p).Exec(user.InjectOrgID(context.Background(), "t1"))
}

// A request whose engine consumes several sample iterators in lockstep — a
// binary operation loads its two legs' step evaluators in parallel and steps
// them alternately — cannot deadlock the pool either: each leg's fan-out
// drains its streams before the engine reads them, holding its connections
// only while it transfers, never while the engine waits on a sibling leg.
// (The streaming merge this replaces held a leg's connections while its
// streams waited for the engine, whose engine waited on the sibling leg,
// whose streams waited for those connections: a circular wait, broken only
// by the pool-acquire timeout.)
func TestSelectSamples_FanoutMultiLegQueriesNeverDeadlockPool(t *testing.T) {
	conn, db := chtest.New(t)
	// Twenty thousand rows per stream: forty blocks at the pool's max_block_size
	// of 500, comfortably more than the streaming merge this test guards
	// against buffered before a send stalled, so that regression still
	// reproduces. A round moves 160 MB through the pool of four — deep enough
	// queueing to stress it, shallow enough to drain in seconds on a slow
	// runner, far inside the 10 s acquire timeout below.
	seedPoolStreamsRows(t, conn, 4, 20000)
	qs := `sum(count_over_time({app=~"s00|s01"}[1ms])) + sum(count_over_time({app=~"s02|s03"}[1ms]))`
	start, end := t0, t0.Add(60*time.Millisecond) // all 20 000 rows of every stream
	step := time.Millisecond

	// A pool of four connections, and a budget far above it: every leg of
	// every binary operation fans out (ten queries, two legs, two streams
	// each — 40 units at peak), with headroom above the peak so a query or
	// a leg added later still fans out. The test therefore exercises the
	// fan-out's lockstep legs, not the global read's lazy cursors. Those
	// can hold the pool in their own lockstep cycle — a known limitation
	// inherited from 0.12.0 (see docs/operations.md). The drain makes a
	// budget above the pool safe (see
	// TestSelectSamples_FanoutBudgetAbovePoolSucceeds).
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 4, 4
	opts.DialTimeout = 10 * time.Second   // queueing on the pool must never time out; a regression (the streaming merge) still fails within one round
	opts.Settings["max_block_size"] = 500 // small blocks: a long transfer
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	q := &chstore.Querier{Conn: pool, FanoutConns: 64}

	since := time.Now()
	want := run(t, engine(t, q), qs, start, end, step, logproto.FORWARD, 0)
	require.NotEmpty(t, want.Data)

	// Concurrent binary operations on the pool that cannot hold them. The
	// deadlock is timing-dependent, so run several rounds.
	const concurrent = 10
	ngs := make([]*logql.QueryEngine, concurrent)
	for i := range ngs {
		ngs[i] = engine(t, q)
	}
	for range 3 {
		type outcome struct {
			data logqlmodel.Result
			err  error
		}
		out := make(chan outcome, concurrent)
		for _, ng := range ngs {
			go func() {
				res, err := runQuery(ng, qs, start, end, step)
				out <- outcome{res, err}
			}()
		}
		for range concurrent {
			o := <-out
			require.NoError(t, o.err)
			require.Equal(t, want.Data, o.data.Data)
		}
	}

	// Every leg fanned out: a leg that fell back to the global read would
	// log a FINAL query, and would take part in the 0.12.0 lockstep cycle
	// this test pins the absence of.
	rows := queryStartsSince(t, conn, db, since, "FROM logs")
	require.NotEmpty(t, rows)
	for _, logged := range rows {
		require.NotContains(t, logged, "FINAL", logged)
	}
}

// A fan-out whose drained result would exceed the sample cap reads all
// streams in one query instead: the cap bounds the memory a fanned-out read
// buffers before it serves its samples.
func TestSelectSamples_FanoutOverSampleCapFallsBack(t *testing.T) {
	conn, db := chtest.New(t)
	seedFanoutData(t, conn) // two streams, 180 rows each
	ctx := context.Background()
	qs := `count_over_time({app=~"a|b"}[5m])`
	start, end := t0.Add(-time.Minute), t0.Add(3*time.Hour)
	q := &chstore.Querier{Conn: conn}
	want, err := collectSamplesQuery(ctx, q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	// A cap far below the 360 samples the read yields: the query reads all
	// streams in one query.
	since := time.Now()
	capBefore := chstore.FanoutFallbackCount("cap")
	got, err := collectSamplesQuery(ctx, &chstore.Querier{Conn: conn, FanoutMaxSamples: 10}, qs, start, end)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, capBefore+1, chstore.FanoutFallbackCount("cap"), "the sample-cap fallback is counted")

	// The drain starts its per-stream queries, exceeds the cap and gives up;
	// the query then reads all streams in one query.
	var global int
	for _, logged := range queryStartsSince(t, conn, db, since, "FROM logs") {
		if strings.Contains(logged, "FINAL") {
			require.Contains(t, logged, "lh_stream_ids", logged)
			global++
		}
	}
	require.Equal(t, 1, global, "the over-cap query read all streams in one query")
}

// cancelledPerStreamConn passes every query through, but fails the per-stream
// fan-out queries (their ORDER BY is the part's physical order) with the
// context's cancellation.
type cancelledPerStreamConn struct{ driver.Conn }

func (c cancelledPerStreamConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "ORDER BY stream_id ASC") {
		return nil, context.Canceled
	}
	return c.Conn.Query(ctx, query, args...)
}

// A stream whose query is cancelled fails the whole fan-out with that error:
// the query never returns a short result silently.
func TestSelectSamples_FanoutStreamCancelledFailsQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	seedFanoutData(t, conn)
	q := &chstore.Querier{Conn: cancelledPerStreamConn{conn}}
	it, err := openSamplesQuery(context.Background(), q, `count_over_time({app=~"a|b"}[5m])`, t0.Add(-time.Minute), t0.Add(3*time.Hour))
	require.Nil(t, it)
	require.ErrorIs(t, err, context.Canceled)
}

// On a cluster each per-stream query is routed to the shard owning the stream
// (optimize_skip_unused_shards): a stream on the initiator's shard produces no
// remote query at all, a stream on another shard exactly one — never one
// remote query per shard per stream. The query selects one stream of each
// placement, so without the routing both streams would produce a remote query.
func TestSelectSamples_FanoutRoutesToOwningShard(t *testing.T) {
	topo := chtest.Topology()
	if topo.Cluster == "" {
		t.Skip("needs a cluster")
	}
	conn, db := chtest.New(t)
	ctx := context.Background()
	local, remote := seedRoutedStreams(t, conn, db, topo.Cluster)
	qs := fmt.Sprintf(`count_over_time({app=~"%s|%s"}[5m])`, local, remote)

	since := time.Now()
	it := openSamples(user.InjectOrgID(ctx, "t1"), t, &chstore.Querier{Conn: conn}, qs, t0.Add(-time.Minute), t0.Add(10*time.Minute))
	for it.Next() {
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS ON CLUSTER "+topo.Cluster))
	// Remote executions of the per-stream reads run against the local table
	// (`<db>`.`logs_local`) in the remote connection's default database, so
	// they are recognised by the table reference, not by current_database.
	var got uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM clusterAllReplicas("+topo.Cluster+", system, query_log) WHERE type = 'QueryFinish' AND NOT is_initial_query AND query_kind = 'Select' AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", since.UnixMicro(), "`"+db+"`.`"+topo.Local("logs")+"`").Scan(&got))
	require.Equal(t, uint64(1), got, "only the stream on the other shard produces a remote query (%s local, %s remote)", local, remote)
}

// On a cluster a single-stream log query is routed to the shard owning the
// stream as well: its SQL restricts the read with `stream_id = ?`, which the
// shard analysis can evaluate, so a stream on the initiator's shard produces
// no remote query at all and a stream on another shard exactly one.
func TestSelectLogs_SingleStreamRoutesToOwningShard(t *testing.T) {
	topo := chtest.Topology()
	if topo.Cluster == "" {
		t.Skip("needs a cluster")
	}
	conn, db := chtest.New(t)
	local, remote := seedRoutedStreams(t, conn, db, topo.Cluster)

	remoteSelects := func(app string) uint64 {
		since := time.Now()
		run(t, engine(t, &chstore.Querier{Conn: conn}), fmt.Sprintf(`{app="%s"}`, app), t0.Add(-time.Minute), t0.Add(10*time.Minute), 0, logproto.FORWARD, 100)
		require.NoError(t, conn.Exec(context.Background(), "SYSTEM FLUSH LOGS ON CLUSTER "+topo.Cluster))
		var got uint64
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM clusterAllReplicas("+topo.Cluster+", system, query_log) WHERE type = 'QueryFinish' AND NOT is_initial_query AND query_kind = 'Select' AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", since.UnixMicro(), "`"+db+"`.`"+topo.Local("logs")+"`").Scan(&got))
		return got
	}
	require.Equal(t, uint64(0), remoteSelects(local), "a stream on the initiator's shard produces no remote query")
	require.Equal(t, uint64(1), remoteSelects(remote), "a stream on another shard produces exactly one remote query")
}

// seedRoutedStreams seeds candidate streams and returns one whose shard holds
// the initiator and one whose shard does not. It skips the test when the
// candidates did not split across shards.
func seedRoutedStreams(t *testing.T, conn driver.Conn, db, cluster string) (local, remote string) {
	t.Helper()
	ctx := context.Background()

	var initHost string
	require.NoError(t, conn.QueryRow(ctx, "SELECT hostName()").Scan(&initHost))

	// replicasPerShard is the number of hosts every shard of the test cluster
	// runs; the inserts are quorum-synchronous, so once a stream's rows are
	// visible on that many hosts the owner set below is complete.
	replicas := map[uint32]int{}
	rows, err := conn.Query(ctx, "SELECT shard_num, count() FROM system.clusters WHERE cluster = ? GROUP BY shard_num", cluster)
	require.NoError(t, err)
	for rows.Next() {
		var shard uint32
		var n uint64
		require.NoError(t, rows.Scan(&shard, &n))
		replicas[shard] = int(n)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotEmpty(t, replicas)
	replicasPerShard := -1
	for _, n := range replicas {
		if replicasPerShard == -1 {
			replicasPerShard = n
		}
		require.Equal(t, replicasPerShard, n, "uniform replica count per shard")
	}

	// Seed candidate streams and classify each by the shard owning it.
	const candidates = 16 // the fan-out limit
	seeded := make([]logproto.Stream, 0, candidates)
	for i := range candidates {
		seeded = append(seeded, logproto.Stream{
			Labels: fmt.Sprintf(`{app="s%02d"}`, i),
			Entries: []logproto.Entry{
				{Timestamp: t0, Line: "line one"},
				{Timestamp: t0.Add(time.Minute), Line: "line two"},
			},
		})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", seeded...)

	ms, err := syntax.ParseMatchers(`{app=~"s[0-9]+"}`, false)
	require.NoError(t, err)
	streams, err := chstore.ResolveStreams(user.InjectOrgID(ctx, "t1"), conn, "t1", ms, t0.Add(-time.Minute), t0.Add(10*time.Minute))
	require.NoError(t, err)
	require.Len(t, streams, candidates)
	for _, s := range streams {
		var hosts []string
		require.Eventually(t, func() bool {
			hosts = hostsWithStream(t, conn, cluster, db, s.ID)
			return len(hosts) == replicasPerShard
		}, 10*time.Second, 50*time.Millisecond, "stream %d on every replica of its shard", s.ID)
		app := s.Labels.Get("app")
		if slices.Contains(hosts, initHost) {
			local = app // on the initiator's shard: no remote query
		} else {
			remote = app // on another shard: exactly one remote query
		}
	}
	if local == "" || remote == "" {
		t.Skipf("the %d candidate streams did not split across shards", candidates)
	}
	return local, remote
}

// hostsWithStream returns the hosts whose local logs table holds rows of streamID.
func hostsWithStream(t *testing.T, conn driver.Conn, cluster, db string, streamID uint64) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT DISTINCT hostName() FROM clusterAllReplicas("+cluster+", "+db+", logs_local) WHERE stream_id = ?", streamID)
	require.NoError(t, err)
	defer rows.Close()
	var hosts []string
	for rows.Next() {
		var h string
		require.NoError(t, rows.Scan(&h))
		hosts = append(hosts, h)
	}
	require.NoError(t, rows.Err())
	return hosts
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
