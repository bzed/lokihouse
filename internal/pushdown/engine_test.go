package pushdown

import (
	"context"
	"errors"
	"flag"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

type nopLogger struct{}

func (nopLogger) Log(...any) error { return nil }

// In ModeOn, a sample query whose leaves are all ineligible for Tier A must
// run on Tier B (not the downstream path); the result is still identical.
func TestEngine_ModeOnIneligibleFallsBackToTierB(t *testing.T) {
	conn, _ := chtest.New(t)
	ng := &chstore.Querier{Conn: conn}
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	tierB := logql.NewEngine(logql.EngineOpts{}, ng, overrides, nopLogger{})
	ctx := user.InjectOrgID(context.Background(), "t1")
	p, err := logql.NewLiteralParams(`sum by (app) (count_over_time({app="a"} | json [1m]))`, time.Now().Add(-time.Hour), time.Now(), time.Minute, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	e := &Engine{Mode: ModeOn, TierB: tierB, TierA: &TierA{Conn: conn, Limits: overrides}, Limits: overrides, Logger: nopLogger{}}
	want, err := tierB.Query(p).Exec(ctx)
	require.NoError(t, err)
	got, err := e.Query(ctx, p).Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, want.Data, got.Data)
}

// A multi-tenant request (two org IDs) must run on Tier B, not Tier A: Tier A
// SQL is single-tenant, so TierALeaves must not increase.
func TestEngine_MultiTenantBypassesTierA(t *testing.T) {
	conn, _ := chtest.New(t)
	ng := &chstore.Querier{Conn: conn}
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	tierB := logql.NewEngine(logql.EngineOpts{}, ng, overrides, nopLogger{})
	before := TierALeaves.Load()
	// A two-tenant context: "a|b".
	ctx := user.InjectOrgID(context.Background(), "a|b")
	// A range aggregation that would be eligible for Tier A in single-tenant mode.
	p, err := logql.NewLiteralParams(`count_over_time({app="a"}[1m])`, time.Now().Add(-time.Hour), time.Now(), time.Minute, 0, logproto.FORWARD, 0, nil, nil)
	require.NoError(t, err)
	e := &Engine{Mode: ModeOn, TierB: tierB, TierA: &TierA{Conn: conn, Limits: overrides}, Limits: overrides, Logger: nopLogger{}}
	q := e.Query(ctx, p)
	require.NotNil(t, q, "multi-tenant must fall back to Tier B")
	_ = q
	require.Equal(t, before, TierALeaves.Load(), "Tier A must not run for a multi-tenant query")
}

// The downstream path must return its results to the accumulator in order,
// even when one leaf fails: the failing query's index is where the error lands.
type failingAcc struct {
	idx  int
	got  []int
	fail int
}

func (f *failingAcc) Accumulate(context.Context, logqlmodel.Result, int) error {
	f.got = append(f.got, f.idx)
	f.idx++
	if f.idx == f.fail {
		return errors.New("boom")
	}
	return nil
}

func (f *failingAcc) Result() []logqlmodel.Result { return nil }

func TestDownstreamer_AccumulatesInOrderAndStopsOnError(t *testing.T) {
	execs := 0
	ex := func(context.Context, logql.DownstreamQuery) (logqlmodel.Result, error) {
		execs++
		return logqlmodel.Result{}, nil
	}
	acc := &failingAcc{fail: 2}
	res, err := (&downstreamer{execOne: ex}).Downstream(context.Background(), []logql.DownstreamQuery{{}, {}, {}}, acc)
	require.ErrorContains(t, err, "boom")
	require.Len(t, res, 3)
	require.Equal(t, []int{0, 1}, acc.got)
	require.Equal(t, 2, execs)
}

func TestEngine_DownstreamEqualsTierB(t *testing.T) {
	conn, _ := chtest.New(t)
	ng := &chstore.Querier{Conn: conn}
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	tierB := logql.NewEngine(logql.EngineOpts{}, ng, overrides, nopLogger{})

	// Seed a few streams.
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	var es []logproto.Entry
	for i := 0; i < 60; i++ {
		es = append(es, logproto.Entry{Timestamp: base.Add(time.Duration(i) * time.Second), Line: "hello world"})
	}
	require.NoError(t, w.Write(context.Background(), "t1",
		[]logproto.Stream{
			{Labels: `{app="a", env="dev"}`, Entries: es},
			{Labels: `{app="a", env="prod"}`, Entries: es[:30]},
		}, nil))

	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := base, base.Add(2*time.Minute)

	// skipTopk is true for queries where topk's winner depends on internal
	// (non-deterministic) iteration order at a float tie; values are equal but
	// series ordering may differ.
	queries := []string{
		`count_over_time({app="a"}[1m])`,
		`sum by (env) (count_over_time({app="a"}[1m]))`,
		`count_over_time({app="a"}[1m]) / count_over_time({app="a"}[2m])`,
		`sum by (env) (count_over_time({app="a"}[1m])) / sum by (env) (count_over_time({app="a"}[2m]))`,
		`topk(1, sum by (env) (count_over_time({app="a"}[1m])))`,
		`label_replace(count_over_time({app="a"}[1m]), "x", "$1", "env", "(.*)")`,
		`absent_over_time({app="nope"}[5m])`,
		`count_over_time({app="a"}[1m]) * 2`,
		`sum(rate({app="a"}[1m]))`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			p, err := logql.NewLiteralParams(q, start, end, time.Minute, 0, logproto.FORWARD, 0, nil, nil)
			require.NoError(t, err)
			want, err := tierB.Query(p).Exec(ctx)
			require.NoError(t, err)
			e := &Engine{Mode: ModeDownstream, TierB: tierB, Limits: overrides, Logger: nopLogger{}}
			got, err := e.Query(ctx, p).Exec(ctx)
			require.NoError(t, err)
			// topk with k < candidates picks its winner by a float tie-break that is
			// non-deterministic between runs; compare the value multiset instead.
			if strings.HasPrefix(strings.TrimSpace(q), "topk") {
				require.Equal(t, sortedValues(want.Data), sortedValues(got.Data), q)
			} else {
				requireDataEqual(t, want.Data, got.Data, q)
			}
		})
	}
}

// sortedValues returns the sorted list of float values in a Matrix/Vector, so
// non-deterministic series ordering (topk tie-break) does not affect the compare.
func sortedValues(d any) []float64 {
	switch v := d.(type) {
	case promql.Matrix:
		var out []float64
		for _, s := range v {
			for _, p := range s.Floats {
				out = append(out, p.F)
			}
		}
		sort.Float64s(out)
		return out
	case promql.Vector:
		var out []float64
		for _, p := range v {
			out = append(out, p.F)
		}
		sort.Float64s(out)
		return out
	default:
		return nil
	}
}

// requireDataEqual requires equal query data, with float values compared at the
// oracle's relative tolerance (1e-9): vector aggregations and arithmetic over
// pushed-down leaves may sum in a different order than Loki's inline evaluation,
// whose own order is random (it ranges over a map). Labels, timestamps and
// shape must match exactly.
func requireDataEqual(t testing.TB, want, got any, msgAndArgs ...any) {
	t.Helper()
	norm := func(d any) (any, []float64) {
		var fs []float64
		switch v := d.(type) {
		case promql.Matrix:
			cp := make(promql.Matrix, len(v))
			for i, s := range v {
				cp[i] = promql.Series{Metric: s.Metric, Floats: make([]promql.FPoint, len(s.Floats))}
				for j, p := range s.Floats {
					cp[i].Floats[j] = promql.FPoint{T: p.T}
					fs = append(fs, p.F)
				}
			}
			return cp, fs
		case promql.Vector:
			cp := make(promql.Vector, len(v))
			for i, s := range v {
				cp[i] = promql.Sample{Metric: s.Metric, T: s.T}
				fs = append(fs, s.F)
			}
			return cp, fs
		case promql.Scalar:
			return promql.Scalar{T: v.T}, []float64{v.V}
		}
		return d, nil
	}
	ws, wf := norm(want)
	gs, gf := norm(got)
	require.Equal(t, ws, gs, msgAndArgs...)
	for i := range wf {
		if !floatClose(wf[i], gf[i]) {
			require.Equal(t, want, got, msgAndArgs...) // full diff
		}
	}
}

func floatClose(a, b float64) bool {
	if a == b || (math.IsNaN(a) && math.IsNaN(b)) {
		return true
	}
	return math.Abs(a-b) <= 1e-9*math.Abs(a)
}
