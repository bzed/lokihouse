package pushdown

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// Mode selects the push-down behavior.
type Mode string

const (
	// ModeOn enables Tier A push-down for eligible leaves, falling back to Tier B.
	ModeOn Mode = "on"
	// ModeOff disables push-down; everything runs on the Phase 1 engine.
	ModeOff Mode = "off"
	// ModeDownstream maps every query but runs all leaves on Tier B.
	ModeDownstream Mode = "downstream"
)

// TierALeaves counts Tier A leaf executions (for coverage assertions).
var TierALeaves atomic.Int64

// ErrFallback is returned by Leaf.Exec when a leaf that is statically eligible
// cannot run in SQL for these parameters (e.g. too many buckets); the leaf then
// runs on Tier B.
var ErrFallback = errors.New("pushdown: leaf falls back to Tier B")

// Leaf executes one range-aggregation leaf in SQL.
type Leaf interface {
	Eligible(e *syntax.RangeAggregationExpr) bool
	Exec(ctx context.Context, e *syntax.RangeAggregationExpr, p logql.Params) (logqlmodel.Result, error)
}

// Engine routes a query to the plain Tier B engine or to Loki's downstream engine
// with range-aggregation leaves wrapped for push-down.
type Engine struct {
	Mode   Mode
	TierB  *logql.QueryEngine
	TierA  Leaf // nil until it is wired (Task 6)
	Limits logql.Limits
	Logger log.Logger
}

// Query returns the query to execute for p: the mapped downstream query when the
// mode and the expression allow it, otherwise TierB.Query(p).
func (e *Engine) Query(ctx context.Context, p logql.Params) logql.Query {
	// Tier A SQL is single-tenant; multi-tenant queries run on Tier B, whose
	// querier fans out per tenant (internal/multitenant).
	if ids, err := tenant.TenantIDs(ctx); err == nil && len(ids) > 1 {
		return e.TierB.Query(p)
	}
	if e.Mode == ModeOff {
		return e.TierB.Query(p)
	}
	expr := p.GetExpression()
	se, ok := expr.(syntax.SampleExpr)
	if !ok {
		// Log (non-sample) expression: no push-down.
		return e.TierB.Query(p)
	}
	mapped, leaves, ok := Map(se)
	if !ok || len(leaves) == 0 {
		return e.TierB.Query(p)
	}
	// In "on" mode, only take the downstream path if at least one leaf is eligible
	// for Tier A; otherwise run the original query on Tier B.
	if e.Mode == ModeOn && !e.anyEligible(leaves) {
		return e.TierB.Query(p)
	}
	down := logql.NewDownstreamEngine(logql.EngineOpts{}, e, e.Limits, e.Logger)
	return down.Query(ctx, logql.ParamsWithExpressionOverride{Params: p, ExpressionOverride: mapped})
}

func (e *Engine) anyEligible(leaves []*syntax.RangeAggregationExpr) bool {
	if e.TierA == nil {
		return false
	}
	for _, l := range leaves {
		if e.TierA.Eligible(l) {
			return true
		}
	}
	return false
}

// Downstreamer implements logql.Downstreamable: it executes each downstream query.
func (e *Engine) Downstreamer(ctx context.Context) logql.Downstreamer {
	return &downstreamer{e: e, execOne: e.execOne}
}

// downstreamer runs every downstream query in order, executing Tier A for eligible
// range-aggregation leaves and Tier B otherwise, accumulating each result at its
// index. execOne is (*Engine).execOne in production; tests replace it with a
// stub to drive the accumulation path without a store.
type downstreamer struct {
	e       *Engine
	execOne func(context.Context, logql.DownstreamQuery) (logqlmodel.Result, error)
}

func (d *downstreamer) Downstream(ctx context.Context, queries []logql.DownstreamQuery, acc logql.Accumulator) ([]logqlmodel.Result, error) {
	results := make([]logqlmodel.Result, len(queries))
	for i, q := range queries {
		res, err := d.execOne(ctx, q)
		if err != nil {
			return results, err
		}
		results[i] = res
		if err := acc.Accumulate(ctx, res, i); err != nil {
			return results, err
		}
	}
	return results, nil
}

// execOne executes one downstream query: the Tier A leaf when eligible, Tier B otherwise.
func (e *Engine) execOne(ctx context.Context, q logql.DownstreamQuery) (logqlmodel.Result, error) {
	if e.Mode == ModeOn && e.TierA != nil {
		if leaf, ok := q.Params.GetExpression().(*syntax.RangeAggregationExpr); ok {
			if e.TierA.Eligible(leaf) {
				res, err := e.TierA.Exec(ctx, leaf, q.Params)
				if !errors.Is(err, ErrFallback) {
					TierALeaves.Add(1)
					return res, err
				}
			}
		}
	}
	return e.TierB.Query(q.Params).Exec(ctx)
}
