package ruler

import (
	"context"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// Engine executes a LogQL query; *pushdown.Engine and api.NewPlainQuerier
// satisfy it (it has api.Querier's method set).
type Engine interface {
	Query(ctx context.Context, p logql.Params) logql.Query
}

// Evaluator implements Loki's pkg/ruler.Evaluator on lokihouse's engine, so
// rules get Tier A push-down, Tier B fallback and query-time delete filtering
// like every other query.
type Evaluator struct {
	Engine Engine
}

// Copyright (c) 2015-2026 Grafana Labs.
// Copied from grafana/loki@bc196286 pkg/ruler/evaluator_local.go (LocalEvaluator.Eval);
// modifications: runs Engine instead of *logql.QueryEngine; the per-evaluation
// info log line is dropped.

// Eval evaluates a LogQL query string at the given time.
func (e *Evaluator) Eval(ctx context.Context, qs string, now time.Time) (*logqlmodel.Result, error) {
	params, err := logql.NewLiteralParams(
		qs,
		now,
		now,
		0,
		0,
		logproto.FORWARD,
		0,
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}

	res, err := e.Engine.Query(ctx, params).Exec(ctx)
	if err != nil {
		return nil, err
	}
	return &res, nil
}
