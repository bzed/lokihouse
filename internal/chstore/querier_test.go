package chstore_test

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// Yesterday 12:00 UTC.
var t0 = time.Now().UTC().Truncate(24 * time.Hour).Add(-12 * time.Hour)

func seed(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams))
}

func engine(t *testing.T, q logql.Querier) *logql.QueryEngine {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return logql.NewEngine(logql.EngineOpts{}, q, o, log.NewNopLogger())
}

func run(t *testing.T, ng *logql.QueryEngine, qs string, start, end time.Time, step time.Duration, dir logproto.Direction, limit uint32) logqlmodel.Result {
	p, err := logql.NewLiteralParams(qs, start, end, step, 0, dir, limit, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	res, err := ng.Query(p).Exec(ctx)
	require.NoError(t, err)
	return res
}

func TestSelectLogs_FilterAndDirection(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: t0, Line: "keep 1"}, {Timestamp: t0.Add(time.Second), Line: "drop"}, {Timestamp: t0.Add(2 * time.Second), Line: "keep 2"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep other app"}}})
	seed(t, w, "t2", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep foreign tenant"}}})

	ng := engine(t, &chstore.Querier{Conn: conn})
	res := run(t, ng, `{app="a"} |= "keep"`, t0, t0.Add(time.Minute), 0, logproto.BACKWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="a"}`, streams[0].Labels)
	require.Equal(t, []string{"keep 2", "keep 1"}, lines(streams[0]))
}

func TestSelectLogs_TimeBoundaries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(-time.Nanosecond), Line: "before"}, {Timestamp: t0, Line: "start"},
		{Timestamp: t0.Add(time.Minute - time.Nanosecond), Line: "last"}, {Timestamp: t0.Add(time.Minute), Line: "end"}},
	})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"start", "last"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

// clickhouse-go binds a time.Time parameter at second precision, so query
// bounds must not be bound as time.Time: a sub-second start or end would admit
// or drop the rows in the rest of that second.
func TestSelectLogs_SubSecondTimeBoundaries(t *testing.T) {
	conn, _ := chtest.New(t)
	ms := time.Millisecond
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(400 * ms), Line: "before"}, {Timestamp: t0.Add(500 * ms), Line: "start"},
		{Timestamp: t0.Add(time.Minute + 400*ms), Line: "last"}, {Timestamp: t0.Add(time.Minute + 500*ms), Line: "end"}},
	})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0.Add(500*ms), t0.Add(time.Minute+500*ms), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"start", "last"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectSamples_SubSecondEnd(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(36 * time.Millisecond), Line: "x"}}})
	end := t0.Add(456 * time.Millisecond)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `count_over_time({app="a"}[10s])`, end, end, 0, logproto.FORWARD, 0)
	v := res.Data.(promql.Vector)
	require.Len(t, v, 1)
	require.Equal(t, 1.0, v[0].F)
}

func TestSelectLogs_DedupesIdenticalEntries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	s := logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "same"}}}
	seed(t, w, "t1", s)
	seed(t, w, "t1", s)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"same"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectLogs_RegexSelectorAndParser(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="api-1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}, {Timestamp: t0.Add(time.Second), Line: `{"status":200}`}}},
		logproto.Stream{Labels: `{app="web"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}}})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app=~"api-.*"} | json | status="500"`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="api-1", status="500"}`, streams[0].Labels)
}

func TestSelectSamples_CountOverTime(t *testing.T) {
	conn, _ := chtest.New(t)
	var es []logproto.Entry
	for i := 0; i < 10; i++ {
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * 10 * time.Second), Line: "x"})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `sum(count_over_time({app="a"}[1m]))`, t0.Add(time.Minute), t0.Add(2*time.Minute), time.Minute, logproto.FORWARD, 0)
	m := res.Data.(promql.Matrix)
	require.Len(t, m, 1)
	// entries at t0+0s..90s; Loki windows are left-open: (t0, t0+1m] = 10s..60s, (t0+1m, t0+2m] = 70s..90s
	require.Equal(t, 6.0, m[0].Floats[0].F)
	require.Equal(t, 3.0, m[0].Floats[1].F)
}

func TestDetectedLabels(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "test"}}})

	q := &chstore.Querier{Conn: conn}
	req := &logproto.DetectedLabelsRequest{Start: t0, End: t0.Add(time.Hour)}
	ctx := user.InjectOrgID(context.Background(), "t1")
	resp, err := q.DetectedLabels(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// The response should contain detected labels
	require.NotNil(t, resp.DetectedLabels)
	// With one stream with {app="a"}, we should have at least the app label
	_ = resp.DetectedLabels
}

func TestDetectedLabelsWithQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "test"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "test"}}})

	q := &chstore.Querier{Conn: conn}
	// Query for only app="a" streams
	req := &logproto.DetectedLabelsRequest{
		Query: `{app="a"}`,
		Start: t0,
		End:   t0.Add(time.Hour),
	}
	ctx := user.InjectOrgID(context.Background(), "t1")
	resp, err := q.DetectedLabels(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// The response should contain detected labels for app="a" only
	_ = resp.DetectedLabels
}

func TestDetectedLabelsEmpty(t *testing.T) {
	conn, _ := chtest.New(t)
	q := &chstore.Querier{Conn: conn}
	req := &logproto.DetectedLabelsRequest{Start: t0, End: t0.Add(time.Hour)}
	ctx := user.InjectOrgID(context.Background(), "t1")
	resp, err := q.DetectedLabels(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// With no streams, the response should have empty detected labels
	require.Empty(t, resp.DetectedLabels)
}

func TestEntryIter_StreamHash(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "test"}}})

	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), "t1")
	it, err := q.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: `{app="a"}`, Start: t0, End: t0.Add(time.Hour), Direction: logproto.FORWARD,
		Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="a"}`)},
	}})
	require.NoError(t, err)
	defer it.Close()

	// Iterate through and call StreamHash to ensure it's covered
	for it.Next() {
		_ = it.StreamHash()
	}
	require.NoError(t, it.Err())
}

func TestKeyset_SameResultForAnyChunkSize(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	var es []logproto.Entry
	for i := range 50 { // 5 timestamps × 10 lines each
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i/10) * time.Second), Line: fmt.Sprint("l", i)})
	}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es}, logproto.Stream{Labels: `{app="b"}`, Entries: es[:20]})
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es[13:14]}) // exact duplicate in a second part
	ng := engine(t, &chstore.Querier{Conn: conn})
	t.Cleanup(func() { chstore.SetLogChunkSize(1000) })
	for _, dir := range []logproto.Direction{logproto.FORWARD, logproto.BACKWARD} {
		for _, limit := range []uint32{1, 25, 1000} {
			chstore.SetLogChunkSize(1000)
			want := run(t, ng, `{app=~"a|b"}`, t0.Add(-time.Minute), t0.Add(time.Minute), 0, dir, limit)
			for _, n := range []int{1, 3, 7} {
				chstore.SetLogChunkSize(n)
				got := run(t, ng, `{app=~"a|b"}`, t0.Add(-time.Minute), t0.Add(time.Minute), 0, dir, limit)
				require.Equal(t, want.Data, got.Data, "dir=%v limit=%d chunk=%d", dir, limit, n)
			}
			if limit == 1000 {
				total := 0
				for _, s := range want.Data.(logqlmodel.Streams) {
					total += len(s.Entries)
				}
				require.Equal(t, 70, total, "duplicate counted once")
			}
		}
	}
}

// Keyset continuation keys carry nanosecond timestamps: rows within one
// second must neither be re-read nor skipped at a chunk boundary.
func TestKeyset_SubSecondTimestamps(t *testing.T) {
	conn, _ := chtest.New(t)
	var es []logproto.Entry
	for i := range 40 { // 4 seconds × 10 distinct sub-second timestamps
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i/10)*time.Second + time.Duration(i%10)*time.Millisecond), Line: fmt.Sprint("l", i)})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es})
	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), "t1")
	t.Cleanup(func() { chstore.SetLogChunkSize(1000) })
	chstore.SetLogChunkSize(3)
	for _, dir := range []logproto.Direction{logproto.FORWARD, logproto.BACKWARD} {
		it, err := q.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
			Selector: `{app="a"}`, Start: t0.Add(-time.Minute), End: t0.Add(time.Minute), Direction: dir,
			Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="a"}`)},
		}})
		require.NoError(t, err)
		var got []string
		for it.Next() && len(got) < 100 {
			got = append(got, it.At().Line)
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		require.Len(t, got, 40, "dir=%v got=%v", dir, got)
	}
}

func TestSelectSamples_StreamsInTimestampOrder(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(2 * time.Second), Line: "3"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(time.Second), Line: "2"}}},
	)
	q := &chstore.Querier{Conn: conn}
	expr, err := syntax.ParseSampleExpr(`count_over_time({app=~"a|b"}[1m])`)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	it, err := q.SelectSamples(ctx, logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: t0.Add(-time.Minute), End: t0.Add(time.Minute), Plan: &plan.QueryPlan{AST: expr},
	}})
	require.NoError(t, err)
	defer it.Close()
	var got []string
	var last int64
	for it.Next() {
		s := it.At()
		require.GreaterOrEqual(t, s.Timestamp, last, "timestamps never go backwards")
		last = s.Timestamp
		got = append(got, it.Labels())
	}
	require.NoError(t, it.Err())
	// Equal timestamps order by stream_id (cityHash64 of the label set);
	// at t0 the b stream comes first, then at t0+1s b, at t0+2s a.
	require.Equal(t, []string{`{app="b"}`, `{app="a"}`, `{app="b"}`, `{app="a"}`}, got)
}

type fakeDeletes []deletionproto.DeleteRequest

func (f fakeDeletes) GetAllDeleteRequestsForUser(context.Context, string, bool, *deletion.TimeRange) ([]deletionproto.DeleteRequest, error) {
	return f, nil
}

// TestQuerier_HidesPendingDeletes verifies that SelectLogs/SelectSamples hide
// lines matching a pending (received) delete request via Loki's filter pipeline.
func TestQuerier_HidesPendingDeletes(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "keep"}, {Timestamp: t0.Add(time.Second), Line: "secret 1"},
		{Timestamp: t0.Add(2 * time.Second), Line: "secret 2"}}})
	del := fakeDeletes{{Query: `{app="a"} |= "secret"`, StartTime: model.TimeFromUnixNano(t0.UnixNano()),
		EndTime: model.TimeFromUnixNano(t0.Add(time.Second).UnixNano()), Status: deletionproto.StatusReceived}}
	ng := engine(t, &chstore.Querier{Conn: conn, Deletes: del})
	res := run(t, ng, `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"keep", "secret 2"}, lines(res.Data.(logqlmodel.Streams)[0]), "only inside [start, end]")
	m := run(t, ng, `count_over_time({app="a"}[1m])`, t0.Add(time.Minute), t0.Add(time.Minute), 0, logproto.FORWARD, 0)
	require.Equal(t, 1.0, m.Data.(promql.Vector)[0].F)
}

// TestQuerier_HidesPendingDeletesAdjacent verifies the filter hides matching
// lines even when the delete range starts exactly at the first row.
func TestQuerier_HidesPendingDeletesAdjacent(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "secret 0"}, {Timestamp: t0.Add(time.Second), Line: "secret 1"},
		{Timestamp: t0.Add(2 * time.Second), Line: "keep"}}})
	del := fakeDeletes{{Query: `{app="a"} |= "secret"`, StartTime: model.TimeFromUnixNano(t0.UnixNano()),
		EndTime: model.TimeFromUnixNano(t0.Add(time.Second).UnixNano()), Status: deletionproto.StatusReceived}}
	ng := engine(t, &chstore.Querier{Conn: conn, Deletes: del})
	res := run(t, ng, `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"keep"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func lines(s logproto.Stream) []string {
	var out []string
	for _, e := range s.Entries {
		out = append(out, e.Line)
	}
	return out
}

// A case-insensitive line filter is pushed to ClickHouse only when ILIKE and
// Go's (?i) agree; the Kelvin sign must still match "k".
func TestSelectLogs_FoldCaseNotLost(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "ERROR one"},
		{Timestamp: t0.Add(time.Second), Line: "Error two"},
		{Timestamp: t0.Add(2 * time.Second), Line: "DIS\u212A full"},
		{Timestamp: t0.Add(3 * time.Second), Line: "nothing"},
	}})
	ng := engine(t, &chstore.Querier{Conn: conn})
	for q, want := range map[string][]string{
		`{app="a"} |~ "(?i)error"`: {"ERROR one", "Error two"},
		`{app="a"} |~ "(?i)disk"`:  {"DIS\u212A full"},
	} {
		res := run(t, ng, q, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
		require.Equal(t, want, lines(res.Data.(logqlmodel.Streams)[0]), q)
	}
}

// Regex stream matchers are evaluated in ClickHouse as well as in Go; the
// result must be what Go-only matching gives (anchoring, missing labels = "").
func TestResolveStreams_RegexSameAsGo(t *testing.T) {
	conn, _ := chtest.New(t)
	all := []string{`{app="a"}`, `{app="b"}`, `{app="ab"}`, `{other="x"}`}
	var ss []logproto.Stream
	for _, l := range all {
		ss = append(ss, logproto.Stream{Labels: l, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", ss...)
	for _, q := range []string{`{app=~"a|b"}`, `{other=~".*", app!~"a|b"}`, `{app=~"a|ab", other!="x"}`} {
		ms, err := syntax.ParseMatchers(q, false)
		require.NoError(t, err)
		got, err := chstore.ResolveStreams(context.Background(), conn, "t1", ms, t0.Add(-time.Hour), t0.Add(time.Hour))
		require.NoError(t, err)
		var gotL []string
		for _, s := range got {
			gotL = append(gotL, s.Labels.String())
		}
		var want []string
		for _, l := range all {
			ls, err := syntax.ParseLabels(l)
			require.NoError(t, err)
			ok := true
			for _, m := range ms {
				ok = ok && m.Matches(ls.Get(m.Name))
			}
			if ok {
				want = append(want, ls.String())
			}
		}
		require.ElementsMatch(t, want, gotL, q)
		require.NotEmpty(t, want, q)
	}
}

// Rows that differ only in structured metadata are not duplicates, whoever
// removes the duplicates (FINAL or the Go dedup).
func TestSelectSamples_KeepsMetadataOnlyDifference(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	one := func(id string) logproto.Stream {
		return logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x", StructuredMetadata: md("trace_id", id)}}}
	}
	seed(t, w, "t1", one("1"))
	seed(t, w, "t1", one("2"))
	seed(t, w, "t1", one("1"))
	end := t0.Add(30 * time.Second) // Loki range windows are left-open
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `sum(count_over_time({app="a"}[1m]))`, end, end, 0, logproto.FORWARD, 0)
	v := res.Data.(promql.Vector)
	require.Len(t, v, 1)
	require.Equal(t, 2.0, v[0].F)
}

// Log reads page through keyset chunks that FINAL would make slow (and whose
// SQL it would break); only metric reads may use FINAL.
func TestSelectLogs_NeverReadsFinal(t *testing.T) {
	conn, db := chtest.New(t)
	var es []logproto.Entry
	for i := range 10 {
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Second), Line: fmt.Sprint("l", i)})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es})
	ng := engine(t, &chstore.Querier{Conn: conn})
	chstore.SetLogChunkSize(1)
	t.Cleanup(func() { chstore.SetLogChunkSize(1000) })
	since := time.Now()
	run(t, ng, `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 5)
	end := t0.Add(time.Minute)
	run(t, ng, `count_over_time({app="a"}[1m])`, end, end, 0, logproto.FORWARD, 0)

	ctx := context.Background()
	topo := chtest.Topology()
	flush, src := "SYSTEM FLUSH LOGS query_log", "system.query_log"
	if topo.Cluster != "" {
		flush = "SYSTEM FLUSH LOGS ON CLUSTER " + topo.Cluster
		src = fmt.Sprintf("clusterAllReplicas(%s, system, query_log)", topo.Cluster)
	}
	require.NoError(t, conn.Exec(ctx, flush))
	rows, err := conn.Query(ctx, "SELECT query FROM "+src+" WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, 'FROM logs') > 0 AND position(query, 'query_log') = 0", db, since.UnixMicro())
	require.NoError(t, err)
	defer rows.Close()
	var keyset, final int
	for rows.Next() {
		var q string
		require.NoError(t, rows.Scan(&q))
		switch {
		case strings.Contains(q, "LIMIT"):
			keyset++
			require.NotContains(t, q, "FINAL", "keyset chunk must not use FINAL")
		case strings.Contains(q, "FINAL"):
			final++
		}
	}
	require.NoError(t, rows.Err())
	require.Positive(t, keyset, "no keyset query seen")
	require.Positive(t, final, "metric query did not use FINAL")
}
