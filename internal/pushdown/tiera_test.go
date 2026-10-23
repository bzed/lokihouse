package pushdown

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func runTierA(t *testing.T, conn driver.Conn, q string, start, end time.Time, step time.Duration) (any, any) {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	a := &TierA{Conn: conn, Limits: overrides}
	b := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	p, err := logql.NewLiteralParams(q, start, end, step, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	want, err := b.Query(p).Exec(ctx)
	require.NoError(t, err)
	e := &Engine{Mode: ModeOn, TierB: b, TierA: a, Limits: overrides, Logger: nopLogger{}}
	got, err := e.Query(ctx, p).Exec(ctx)
	require.NoError(t, err)
	return want.Data, got.Data
}

func TestTierA_WindowBoundaries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	es := []logproto.Entry{
		{Timestamp: base, Line: "x"},
		{Timestamp: base.Add(-time.Minute), Line: "y"},
		{Timestamp: base.Add(-time.Minute + time.Nanosecond), Line: "z"},
		{Timestamp: base.Add(time.Nanosecond), Line: "w"},
	}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es},
	}))
	start, end := base, base.Add(2*time.Minute)
	queries := []string{
		`count_over_time({app="a"}[1m])`,
		`rate({app="a"}[1m])`,
		`count_over_time({app="a"}[1m] offset 1m)`,
		`rate({app="a"}[1m] offset 1m)`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			want, got := runTierA(t, conn, q, start, end, 15*time.Second)
			require.Equal(t, want, got, q)
		})
	}
}

func TestTierA_CountsDuplicatesOnce(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	es := []logproto.Entry{
		{Timestamp: base, Line: "x"},
		{Timestamp: base.Add(time.Second), Line: "y"},
	}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es},
	}))
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es},
	}))
	start, end := base, base.Add(2*time.Minute)
	q := `count_over_time({app="a"}[1m])`
	want, got := runTierA(t, conn, q, start, end, 30*time.Second)
	require.Equal(t, want, got, q)
}

func TestTierA_MetadataSeries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	es1 := []logproto.Entry{
		{Timestamp: base, Line: "x", StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "info"}}},
		{Timestamp: base.Add(time.Second), Line: "y", StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "info"}}},
	}
	es2 := []logproto.Entry{
		{Timestamp: base, Line: "x", StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "warn"}}},
		{Timestamp: base.Add(time.Second), Line: "y", StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "warn"}}},
	}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es1},
	}))
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es2},
	}))
	start, end := base, base.Add(2*time.Minute)
	q := `count_over_time({app="a"}[1m])`
	want, got := runTierA(t, conn, q, start, end, 30*time.Second)
	require.Equal(t, want, got, q)
}

func TestTierA_Filters(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	es := []logproto.Entry{
		{Timestamp: base, Line: "a b c 1"},
		{Timestamp: base.Add(time.Second), Line: "a d c 2"},
		{Timestamp: base.Add(2 * time.Second), Line: "b only"},
		{Timestamp: base.Add(3 * time.Second), Line: "c only"},
	}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es},
	}))
	start, end := base, base.Add(2*time.Minute)
	for _, q := range []string{
		`count_over_time({app="a"} |= "a" != "b" or "c" |~ "x[0-9]+" [1m])`,
		`bytes_over_time({app="a"}[1m])`,
		`bytes_rate({app="a"}[1m])`,
		`bytes_over_time({app="a"} |= "c" [1m])`,
	} {
		want, got := runTierA(t, conn, q, start, end, 30*time.Second)
		require.Equal(t, want, got, q)
	}
}

func TestTierA_Ineligible(t *testing.T) {
	cases := []string{
		`count_over_time({a="b"} | json [1m])`,
		`sum_over_time({a="b"} | unwrap x [1m])`,
		`count_over_time({a="b"} |~ "(?i)x" [1m])`,
	}
	for _, q := range cases {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		ra, ok := expr.(*syntax.RangeAggregationExpr)
		require.True(t, ok, q)
		a := &TierA{}
		require.False(t, a.Eligible(ra), q)
	}
}

func TestTierA_MaxSeries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	es := []logproto.Entry{
		{Timestamp: base, Line: "x"},
	}
	streams := make([]logproto.Stream, 0, 50)
	for i := 0; i < 50; i++ {
		streams = append(streams, logproto.Stream{
			Labels:  `{app="a", n="` + string(rune('a'+i%26)) + `"}`,
			Entries: es,
		})
	}
	require.NoError(t, w.Write(context.Background(), "t1", streams))
	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := base, base.Add(2*time.Minute)
	q := `count_over_time({app="a"}[1m])`
	p, err := logql.NewLiteralParams(q, start, end, 30*time.Second, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.MaxQuerySeries = 10
	limits, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	a := &TierA{Conn: conn, Limits: limits}
	b := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, limits, nopLogger{})
	_, bErr := b.Query(p).Exec(ctx)
	require.Error(t, bErr, "Tier B should hit max series")
	_, aErr := a.Exec(ctx, mustRangeAgg(t, q), p)
	require.Error(t, aErr)
	require.Equal(t, bErr.Error(), aErr.Error())
}

func mustRangeAgg(t *testing.T, q string) *syntax.RangeAggregationExpr {
	expr, err := syntax.ParseSampleExpr(q)
	require.NoError(t, err)
	ra, ok := expr.(*syntax.RangeAggregationExpr)
	require.True(t, ok, q)
	return ra
}

// An instant query has step 0: Tier A must evaluate exactly once at start
// (it used to loop forever, since t never advanced past end).
func TestTierA_InstantQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "x"}, {Timestamp: base.Add(-30 * time.Second), Line: "yy"}}},
	}))
	at := base.Add(250 * time.Millisecond)
	for _, q := range []string{
		`count_over_time({app="a"}[1m])`,
		`bytes_rate({app="a"}[1m] offset 10s)`,
		`sum by (app) (count_over_time({app="a"}[1m]))`,
	} {
		want, got := runTierA(t, conn, q, at, at, 0)
		require.Equal(t, want, got, q)
	}
}

// A leaf whose window holds no streams (here: the day before the only data)
// must return an empty result, not nil Data, which Loki's step evaluator
// rejects.
func TestTierA_NoStreamsInRange(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "x"}}},
	}))
	at := base.Add(-time.Hour)
	for _, step := range []time.Duration{0, time.Minute} {
		for _, q := range []string{
			`count_over_time({app="a"}[1m])`,
			`sum by (app) (rate({app="a"}[1m]))`,
		} {
			want, got := runTierA(t, conn, q, at, at.Add(step), step)
			require.Equal(t, want, got, "q=%s step=%v", q, step)
		}
	}
}

// Loki keys range-vector series by the final label set, so a stream label and
// an equal structured-metadata label from another stream land in one series.
func TestTierA_MergesSeriesWithEqualLabels(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a", level="x"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "one"}}},
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base.Add(time.Second), Line: "two",
			StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: "x"}}}}},
	}))
	want, got := runTierA(t, conn, `count_over_time({app="a"}[1m])`, base, base.Add(time.Minute), 15*time.Second)
	require.Equal(t, want, got)
}

// Above MaxBuckets the leaf runs on Tier B instead of failing the query.
// fakeDeletes returns a fixed set of delete requests.
type fakeDeletes []deletionproto.DeleteRequest

func (f fakeDeletes) GetAllDeleteRequestsForUser(context.Context, string, bool, *deletion.TimeRange) ([]deletionproto.DeleteRequest, error) {
	return f, nil
}

func TestTierA_PendingDeleteFallsBack(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "x"}, {Timestamp: base.Add(time.Second), Line: "secret"}}},
	}))
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	del := fakeDeletes{{Query: `{app="a"} |= "secret"`, StartTime: model.TimeFromUnixNano(base.UnixNano()),
		EndTime: model.TimeFromUnixNano(base.Add(time.Minute).UnixNano()), Status: deletionproto.StatusReceived}}
	b := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn, Deletes: del}, overrides, nopLogger{})
	a := &TierA{Conn: conn, Limits: overrides, Deletes: del}
	e := &Engine{Mode: ModeOn, TierB: b, TierA: a, Limits: overrides, Logger: nopLogger{}}
	p, err := logql.NewLiteralParams(`count_over_time({app="a"}[1m])`, base, base.Add(time.Minute), 15*time.Second, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	want, err := b.Query(p).Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, 1.0, want.Data.(promql.Matrix)[0].Floats[0].F, "Tier B hides the deleted line")
	before := TierALeaves.Load()
	got, err := e.Query(ctx, p).Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, want.Data, got.Data)
	require.Equal(t, before, TierALeaves.Load(), "fallback is not a Tier A execution")
}

func TestTierA_TooManyBucketsFallsBackToTierB(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "x"}}},
	}))
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	b := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	e := &Engine{Mode: ModeOn, TierB: b, TierA: &TierA{Conn: conn, Limits: overrides, MaxBuckets: 2}, Limits: overrides, Logger: nopLogger{}}
	p, err := logql.NewLiteralParams(`count_over_time({app="a"}[1m])`, base, base.Add(time.Minute), 15*time.Second, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	want, err := b.Query(p).Exec(ctx)
	require.NoError(t, err)
	before := TierALeaves.Load()
	got, err := e.Query(ctx, p).Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, want.Data, got.Data)
	require.Equal(t, before, TierALeaves.Load(), "fallback is not a Tier A execution")
}

func seedSumByData(t *testing.T, conn driver.Conn, base time.Time) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	var plain, withLevel []logproto.Entry
	for i := 0; i < 12; i++ {
		lvl := []string{"info", "warn"}[i%2]
		md := []logproto.LabelAdapter{{Name: "level", Value: lvl}, {Name: "trace_id", Value: fmt.Sprintf("tr%d", i)}}
		plain = append(plain, logproto.Entry{Timestamp: base.Add(time.Duration(i) * 3 * time.Second), Line: fmt.Sprintf("x %d", i), StructuredMetadata: md})
		withLevel = append(withLevel, logproto.Entry{Timestamp: base.Add(time.Duration(i)*3*time.Second + time.Second), Line: fmt.Sprintf("y %d", i), StructuredMetadata: md[1:]})
	}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: plain},
		{Labels: `{app="a", level="s"}`, Entries: withLevel},
	}))
}

var sumByQueries = []string{
	`sum by (level) (count_over_time({app="a"}[1m]))`,
	`sum(rate({app="a"}[1m]))`,
	`sum by (level_extracted) (count_over_time({app="a"}[1m]))`,
	`sum by (app, level) (bytes_over_time({app="a"} |= "x" [1m]))`,
	`sum without (trace_id) (count_over_time({app="a"}[1m]))`,
	`count by (level) (count_over_time({app="a"}[1m]))`,
}

func TestTierA_SumByNarrowsMetadata(t *testing.T) {
	conn, _ := chtest.New(t)
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	seedSumByData(t, conn, base)
	for _, q := range sumByQueries {
		before := TierALeaves.Load()
		want, got := runTierA(t, conn, q, base, base.Add(time.Minute), 15*time.Second)
		require.NotEmpty(t, want, q)
		requireMatrixClose(t, want, got, q)
		require.Equal(t, before+1, TierALeaves.Load(), "one Tier A leaf: %s", q)
	}

	// The leaf returns un-summed series without the unneeded trace_id.
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	a := &TierA{Conn: conn, Limits: overrides}
	expr, err := syntax.ParseSampleExpr(sumByQueries[0])
	require.NoError(t, err)
	require.True(t, a.Eligible(expr))
	p, err := logql.NewLiteralParams(sumByQueries[0], base, base.Add(time.Minute), 15*time.Second, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	res, err := a.Exec(user.InjectOrgID(context.Background(), "t1"), expr, p)
	require.NoError(t, err)
	m, ok := res.Data.(promql.Matrix)
	require.True(t, ok)
	require.NotEmpty(t, m)
	for _, s := range m {
		require.False(t, s.Metric.Has("trace_id"), s.Metric.String())
	}
}

func TestTierA_SumLeafFallback(t *testing.T) {
	conn, _ := chtest.New(t)
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	seedSumByData(t, conn, base)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	del := fakeDeletes{{Query: `{app="a"} |= "y 3"`, StartTime: model.TimeFromUnixNano(base.UnixNano()),
		EndTime: model.TimeFromUnixNano(base.Add(time.Minute).UnixNano()), Status: deletionproto.StatusReceived}}
	for name, a := range map[string]*TierA{
		"pending delete": {Conn: conn, Limits: overrides, Deletes: del},
		"max buckets":    {Conn: conn, Limits: overrides, MaxBuckets: 1},
	} {
		t.Run(name, func(t *testing.T) {
			b := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn, Deletes: del}, overrides, nopLogger{})
			e := &Engine{Mode: ModeOn, TierB: b, TierA: a, Limits: overrides, Logger: nopLogger{}}
			p, err := logql.NewLiteralParams(sumByQueries[0], base, base.Add(time.Minute), 15*time.Second, 0, logproto.FORWARD, 0, nil, nil)
			require.NoError(t, err)
			ctx := user.InjectOrgID(context.Background(), "t1")
			want, err := b.Query(p).Exec(ctx)
			require.NoError(t, err)
			before := TierALeaves.Load()
			got, err := e.Query(ctx, p).Exec(ctx)
			require.NoError(t, err)
			requireMatrixClose(t, want.Data, got.Data, name)
			require.Equal(t, before, TierALeaves.Load())
		})
	}
}

func seedMetadataFilterData(t *testing.T, conn driver.Conn, base time.Time) {
	t.Helper()
	var entries []logproto.Entry
	for i := 0; i < 12; i++ {
		lvl := []string{"info", "warn", "error"}[i%3]
		entries = append(entries, logproto.Entry{Timestamp: base.Add(time.Duration(i) * 3 * time.Second), Line: fmt.Sprintf("x %d", i),
			StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: lvl}}})
	}
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="m"}`, Entries: entries},
		{Labels: `{app="m2", level="warn"}`, Entries: entries},
	}))
}

func TestTierA_MetadataFilter(t *testing.T) {
	conn, _ := chtest.New(t)
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	seedMetadataFilterData(t, conn, base)
	for _, q := range []string{
		`count_over_time({app="m"} | level="warn" [1m])`,
		`rate({app="m"} | level="warn" or level="info" [1m])`,
		`bytes_over_time({app="m"} |= "x" | level="error" [1m])`,
	} {
		before := TierALeaves.Load()
		want, got := runTierA(t, conn, q, base, base.Add(time.Minute), 15*time.Second)
		require.NotEmpty(t, want, q)
		requireMatrixClose(t, want, got, q)
		require.Equal(t, before+1, TierALeaves.Load(), "Tier A ran: %s", q)
	}
	// A resolved stream labelled "level" would satisfy the filter: Tier B decides.
	for _, q := range []string{
		`count_over_time({app=~"m|m2"} | level="warn" [1m])`,
		`rate({app=~"m|m2"} | level="warn" or level="info" [1m])`,
		// the clash renames the metadata key of {app="m2", level="warn"} to level_extracted
		`count_over_time({app="m2"} | level_extracted="error" [1m])`,
	} {
		before := TierALeaves.Load()
		want, got := runTierA(t, conn, q, base, base.Add(time.Minute), 15*time.Second)
		require.NotEmpty(t, want, q)
		requireMatrixClose(t, want, got, q)
		require.Equal(t, before, TierALeaves.Load(), "fell back: %s", q)
	}
}

// requireMatrixClose compares two matrices, allowing the last-bits float
// differences from summing in another order (see Map).
func requireMatrixClose(t *testing.T, want, got any, msg string) {
	t.Helper()
	w, ok := want.(promql.Matrix)
	require.True(t, ok, msg)
	g, ok := got.(promql.Matrix)
	require.True(t, ok, msg)
	require.Len(t, g, len(w), msg)
	for i := range w {
		require.Equal(t, w[i].Metric, g[i].Metric, msg)
		require.Len(t, g[i].Floats, len(w[i].Floats), msg)
		for j := range w[i].Floats {
			require.Equal(t, w[i].Floats[j].T, g[i].Floats[j].T, msg)
			require.InEpsilon(t, w[i].Floats[j].F, g[i].Floats[j].F, 1e-9, msg)
		}
	}
}
