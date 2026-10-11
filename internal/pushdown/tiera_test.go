package pushdown

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/validation"
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
	}, nil))
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
	}, nil))
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es},
	}, nil))
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
	}, nil))
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: es2},
	}, nil))
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
	}, nil))
	start, end := base, base.Add(2*time.Minute)
	q := `count_over_time({app="a"} |= "a" != "b" or "c" |~ "x[0-9]+" [1m])`
	want, got := runTierA(t, conn, q, start, end, 30*time.Second)
	require.Equal(t, want, got, q)
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
	require.NoError(t, w.Write(context.Background(), "t1", streams, nil))
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
