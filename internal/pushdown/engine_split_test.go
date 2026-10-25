package pushdown

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/multitenant"
)

func splitEngine(t *testing.T, mode Mode) (*Engine, *logql.QueryEngine, *SplitMetrics, time.Time) {
	conn, base := splitFixtureConn(t)
	s, raw := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
	s.Metrics = NewSplitMetrics(prometheus.NewRegistry())
	return &Engine{
		Mode: mode, TierB: raw, TierA: &TierA{Conn: conn, Limits: s.Limits},
		Limits: s.Limits, Logger: nopLogger{}, Split: s,
	}, raw, s.Metrics, base
}

func partitionSamples(t *testing.T, m *SplitMetrics) uint64 {
	var out dtoMetric
	require.NoError(t, m.partitions.Write(&out))
	return out.Histogram.GetSampleCount()
}

func TestEngine_TierBRangeQuerySplits(t *testing.T) {
	for _, mode := range []Mode{ModeOff, ModeOn, ModeDownstream} {
		t.Run(string(mode), func(t *testing.T) {
			e, raw, metrics, base := splitEngine(t, mode)
			p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute+3*time.Second), base.Add(3*time.Hour+40*time.Minute), time.Minute)
			want, err := raw.Query(p).Exec(tctx())
			require.NoError(t, err)
			got, err := e.Query(tctx(), p).Exec(tctx())
			require.NoError(t, err)
			requireDataEqual(t, want.Data, got.Data)
			require.NotEmpty(t, got.Data)
			require.EqualValues(t, 1, partitionSamples(t, metrics))
			if mode != ModeDownstream {
				require.EqualValues(t, 4, got.Statistics.Summary.Splits)
			}
		})
	}
}

func TestEngine_MultiTenantDoesNotSplit(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, _ := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
	s.Metrics = NewSplitMetrics(prometheus.NewRegistry())
	mt := logql.NewEngine(logql.EngineOpts{}, &multitenant.Querier{Next: &chstore.Querier{Conn: conn}}, s.Limits, nopLogger{})
	s.Engine = mt
	e := &Engine{Mode: ModeOn, TierB: mt, Limits: s.Limits, Logger: nopLogger{}, Split: s}
	ctx := user.InjectOrgID(context.Background(), "t1|t2")
	p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute), base.Add(3*time.Hour), time.Minute)
	got, err := e.Query(ctx, p).Exec(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got.Data)
	require.EqualValues(t, 0, partitionSamples(t, s.Metrics))
}

// barrierLeaf is a Tier A stand-in whose Exec returns only once two calls are
// inside it at the same time.
type barrierLeaf struct{ entered atomic.Int32 }

func (b *barrierLeaf) Eligible(syntax.SampleExpr) bool { return true }

func (b *barrierLeaf) Exec(ctx context.Context, _ syntax.SampleExpr, _ logql.Params) (logqlmodel.Result, error) {
	b.entered.Add(1)
	deadline := time.After(10 * time.Second)
	for b.entered.Load() < 2 {
		select {
		case <-deadline:
			return logqlmodel.Result{}, errors.New("sibling leaf never ran concurrently")
		case <-ctx.Done():
			return logqlmodel.Result{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return logqlmodel.Result{Data: promql.Matrix{}}, nil
}

// Pins Loki's behaviour: newBinOpStepEvaluator loads both sides of a binary
// operation concurrently, so sibling leaves already run in parallel.
func TestEngine_BinOpLeavesRunConcurrently(t *testing.T) {
	var l = newTestOverrides(t)
	e := &Engine{
		Mode: ModeOn, TierB: logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{}, l, nopLogger{}),
		TierA: &barrierLeaf{}, Limits: l, Logger: nopLogger{},
	}
	now := time.Now()
	p := rangeParams(t, `count_over_time({a="1"}[1m]) / count_over_time({a="2"}[1m])`, now.Add(-time.Hour), now, time.Minute)
	_, err := e.Query(tctx(), p).Exec(tctx())
	require.NoError(t, err)
}
