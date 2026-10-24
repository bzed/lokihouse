package chstore_test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
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

// A fan-out holds one pool connection per stream while it streams, and the
// merge cannot emit a row before every stream produced its head. A query whose
// connection reservation (FanoutConns) fails reads all streams in one query
// instead of deadlocking the pool.
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

// A fan-out needs one pool connection per stream: the merge cannot emit a row
// before every stream produced its head, and a stream blocked sending its
// next batch holds its connection (see seedPoolStreams). A pool smaller than
// the fan-out therefore deadlocks a single query on its own. The connection
// reservation (FanoutConns, wired to half the pool in production) turns that
// into a fallback: with a pool of four and a budget of two, a six-stream query
// reads all streams in one query and succeeds.
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
// megabytes: at the pool's max_block_size of 500 that is a hundred blocks. A
// stream whose consumer stopped reading has at most ~3 000 rows (~600 KB) read
// and two blocks buffered by the driver, so the server is still mid-stream —
// its socket buffer holds far less than the remaining ~9 MB — and the stream
// keeps its pool connection until the consumer drains it. (With a megabyte per
// stream the server finishes into the socket and driver buffers and no
// connection is held; that is why the streams are this large.)
func seedPoolStreams(t *testing.T, conn driver.Conn, n int) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	const rows, chunk = 50000, 10000
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

// Concurrent fan-outs share the FanoutConns budget all or nothing: while one
// query holds the whole budget — and its eight connections, stalled
// mid-stream — until its iterator closes, the next reads globally. Both
// succeed on a pool that holds one fan-out plus headroom.
func TestSelectSamples_FanoutConcurrentPool(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 8)
	ctx := user.InjectOrgID(context.Background(), "t1")
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream

	// A pool that holds one fan-out (8 connections) plus headroom.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 12, 12
	opts.DialTimeout = 2 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: the hold engages
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	q := &chstore.Querier{Conn: pool, FanoutConns: 8}
	want, err := collectSamplesQuery(context.Background(), q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)
	since := time.Now()

	// The first query fans out; one sample proves every stream's query is in
	// flight, so its eight connections are held, and the unconsumed rest
	// stalls the streams mid-stream (see seedPoolStreams)...
	it := openSamples(ctx, t, q, qs, start, end)
	defer func() { _ = it.Close() }()
	require.True(t, it.Next(), "one sample: every stream's query is in flight")
	s := it.At()
	first := fmt.Sprintf("%s %d %v", it.Labels(), s.Timestamp, s.Value)
	// ...so a concurrent query cannot reserve 8 connections and reads all
	// streams in one query.
	got, err := collectSamplesQuery(context.Background(), q, qs, start, end)
	require.NoError(t, err)
	require.Equal(t, want, got)
	rest, err := drainSamples(it)
	require.NoError(t, err)
	require.Equal(t, want, append([]string{first}, rest...))
	require.NoError(t, it.Close())

	// The first query read one query per stream; the second read all streams
	// in one query.
	var perStream, global int
	for _, logged := range queryStartsSince(t, conn, db, since, "FROM logs") {
		if strings.Contains(logged, "FINAL") {
			global++
		} else {
			perStream++
		}
	}
	require.Equal(t, 8, perStream, "the first query read one query per stream")
	require.Equal(t, 1, global, "the second query read all streams in one query")
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

// Many concurrent fanned-out queries on a pool that cannot hold them all: the
// all-or-nothing reservation admits one fan-out at a time and moves the rest
// to one-query reads, which queue on the pool — every query succeeds. Without
// the reservation the fan-outs hold connections while waiting for each other
// and fail on the pool-acquire timeout.
func TestSelectSamples_FanoutUnderConcurrentLoad(t *testing.T) {
	conn, db := chtest.New(t)
	seedPoolStreams(t, conn, 3)
	ctx := user.InjectOrgID(context.Background(), "t1")
	qs := `count_over_time({app=~"s[0-9]+"}[5m])`
	start, end := t0, t0.Add(60*time.Millisecond) // all 50 000 rows of every stream

	// Half the pool for the fan-out budget, as production wires it: one
	// 3-stream fan-out (3 connections) plus one-query reads (1 each) — five
	// concurrent queries need 3 + 4 = 7 against a pool of 6, so one read
	// queues for a connection.
	opts := chtest.Options(db)
	opts.MaxOpenConns, opts.MaxIdleConns = 6, 6
	opts.DialTimeout = 10 * time.Second
	opts.Settings["max_block_size"] = 500 // small blocks: the hold engages
	pool, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	q := &chstore.Querier{Conn: pool, FanoutConns: 3}
	want, err := collectSamplesQuery(context.Background(), q, qs, start, end)
	require.NoError(t, err)
	require.NotEmpty(t, want)
	since := time.Now()

	// Two queries that are not consumed until the rest finished. One sample
	// each proves their queries are in flight, so their connections are
	// held: the fan-out holds three, stalled mid-stream; the one-query read
	// holds one.
	type held struct {
		it    iter.SampleIterator
		first string
		err   error
	}
	ready := make(chan held, 2)
	for range 2 {
		go func() {
			it, err := openSamplesQuery(ctx, q, qs, start, end)
			if err != nil {
				ready <- held{err: err}
				return
			}
			if !it.Next() {
				err := it.Err()
				_ = it.Close()
				ready <- held{err: err}
				return
			}
			s := it.At()
			ready <- held{it: it, first: fmt.Sprintf("%s %d %v", it.Labels(), s.Timestamp, s.Value)}
		}()
	}
	var holders []held
	for range 2 {
		holders = append(holders, <-ready)
	}
	t.Cleanup(func() {
		for _, h := range holders {
			if h.it != nil {
				_ = h.it.Close()
			}
		}
	})
	for _, h := range holders {
		require.NoError(t, h.err)
	}

	// Three more queries arrive: none can reserve, all read globally, and
	// they queue on the two remaining connections.
	const concurrent = 3
	type outcome struct {
		samples []string
		err     error
	}
	out := make(chan outcome, concurrent)
	for range concurrent {
		go func() {
			samples, err := collectSamplesQuery(context.Background(), q, qs, start, end)
			out <- outcome{samples, err}
		}()
	}
	for range concurrent {
		o := <-out
		require.NoError(t, o.err)
		require.Equal(t, want, o.samples)
	}
	for _, h := range holders {
		rest, err := drainSamples(h.it)
		require.NoError(t, err)
		require.NoError(t, h.it.Close())
		require.Equal(t, want, append([]string{h.first}, rest...))
	}

	// One query fanned out (three per-stream queries); the other four read
	// all streams in one query.
	var perStream, global int
	for _, logged := range queryStartsSince(t, conn, db, since, "FROM logs") {
		if strings.Contains(logged, "FINAL") {
			global++
		} else {
			perStream++
		}
	}
	require.Equal(t, 3, perStream, "one query read one query per stream")
	require.Equal(t, 4, global, "the other four queries read all streams in one query")
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
	ctx := context.Background()
	local, remote := seedRoutedStreams(t, conn, db, topo.Cluster)

	remoteSelects := func(app string) uint64 {
		since := time.Now()
		run(t, engine(t, &chstore.Querier{Conn: conn}), fmt.Sprintf(`{app="%s"}`, app), t0.Add(-time.Minute), t0.Add(10*time.Minute), 0, logproto.FORWARD, 100)
		require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS ON CLUSTER "+topo.Cluster))
		var got uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM clusterAllReplicas("+topo.Cluster+", system, query_log) WHERE type = 'QueryFinish' AND NOT is_initial_query AND query_kind = 'Select' AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", since.UnixMicro(), "`"+db+"`.`"+topo.Local("logs")+"`").Scan(&got))
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
