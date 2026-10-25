package pushdown

import (
	"time"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// leg is a contiguous run of a query's step-grid timestamps, both ends inclusive.
type leg struct{ start, end time.Time }

// legParams is the original query's parameters with the evaluation range
// replaced by one leg's.
type legParams struct {
	logql.Params
	start, end time.Time
}

func (p legParams) Start() time.Time { return p.start }
func (p legParams) End() time.Time   { return p.end }

// splitInterval returns the interval to split expr by: configured, but never
// finer than the largest [range] in expr (offsets are ignored). It returns 0
// when splitting is disabled.
//
// Copied from grafana/loki@bc1962860a1607eccb7e0c8ee206970b292b9e80 pkg/querier/queryrange/split_by_interval.go
// (maxRangeVectorAndOffsetDuration); modifications: offset dropped.
func splitInterval(expr syntax.Expr, configured time.Duration) time.Duration {
	if configured <= 0 {
		return 0
	}
	var maxRange time.Duration
	expr.Walk(func(e syntax.Expr) bool {
		if r, ok := e.(*syntax.LogRangeExpr); ok && r.Interval > maxRange {
			maxRange = r.Interval
		}
		return true
	})
	return max(configured, maxRange)
}

// connNeed is the number of ClickHouse connections one evaluation of expr can
// hold at once: one per range selector, plus one for the query itself.
func connNeed(expr syntax.Expr) int64 {
	n := int64(1)
	expr.Walk(func(e syntax.Expr) bool {
		if _, ok := e.(*syntax.LogRangeExpr); ok {
			n++
		}
		return true
	})
	return n
}

// splitLegs cuts the grid start + k·step (k = 0 … ⌊(end−start)/step⌋) into
// maximal runs that share an epoch-based bucket of interval, as Loki's
// frontend buckets. It needs step > 0 and interval > 0.
func splitLegs(start, end time.Time, step, interval time.Duration) []leg {
	last := start.Add((end.Sub(start) / step) * step)
	var legs []leg
	for t := start; !t.After(last); {
		boundary := time.Unix(0, (t.UnixNano()/interval.Nanoseconds()+1)*interval.Nanoseconds())
		// Largest grid point strictly before the boundary, capped at the last.
		e := t.Add((boundary.Sub(t) - 1) / step * step)
		if e.After(last) {
			e = last
		}
		legs = append(legs, leg{t, e})
		t = e.Add(step)
	}
	return legs
}
