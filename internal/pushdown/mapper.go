package pushdown

import (
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// Map wraps every *syntax.RangeAggregationExpr in the sample expression in a
// logql.DownstreamSampleExpr so the downstream engine dispatches each leaf to the
// Downstreamer. It returns the mapped expression, the list of original leaves, and
// ok=false if the tree contains a node kind Map does not handle (the caller then
// runs the original query on Tier B).
//
// The input must be a freshly parsed expression; Map never mutates it.
func Map(expr syntax.SampleExpr) (mapped syntax.SampleExpr, leaves []*syntax.RangeAggregationExpr, ok bool) {
	mapped, ls, ok := mapNode(expr, true)
	if !ok {
		return nil, nil, false
	}
	return mapped, ls, true
}

// mapNode recurses over the sample expression tree, wrapping range-aggregation
// leaves and re-linking the nodes that own sample-expr children. isRoot is true
// for the outermost node, which is where the float-order divergence is detected.
//
// ok=false (and leaves may be partial) on an unsupported node kind or when the
// root shape is one of the diverging ones (root vector agg over a range-agg,
// or root arithmetic binop with a range-agg operand).
func mapNode(e syntax.SampleExpr, isRoot bool) (syntax.SampleExpr, []*syntax.RangeAggregationExpr, bool) {
	switch n := e.(type) {
	case *syntax.RangeAggregationExpr:
		return logql.DownstreamSampleExpr{SampleExpr: n}, []*syntax.RangeAggregationExpr{n}, true
	case *syntax.VectorAggregationExpr:
		if isRoot && containsRangeAgg(n.Left) {
			// Root vector agg (sum, topk, ...) whose subtree contains a
			// range-agg leaf diverges from Loki's inline evaluation in float
			// accumulation order.
			return nil, nil, false
		}
		child, ls, ok := mapNode(n.Left, false)
		if !ok {
			return nil, nil, false
		}
		cp := *n
		cp.Left = child
		return &cp, ls, true
	case *syntax.BinOpExpr:
		if isRoot && arithOp(n.Op) {
			// Root arithmetic binop (+, -, *, /, at) whose operand subtree
			// contains a range-agg leaf diverges the same way.
			if containsRangeAgg(n.SampleExpr) || containsRangeAgg(n.RHS) {
				return nil, nil, false
			}
		}
		l, ls, ok := mapNode(n.SampleExpr, false)
		if !ok {
			return nil, nil, false
		}
		r, rs, ok := mapNode(n.RHS, false)
		if !ok {
			return nil, nil, false
		}
		cp := *n
		cp.SampleExpr = l
		cp.RHS = r
		return &cp, append(ls, rs...), true
	case *syntax.LabelReplaceExpr:
		child, ls, ok := mapNode(n.Left, false)
		if !ok {
			return nil, nil, false
		}
		cp := *n
		cp.Left = child
		return &cp, ls, true
	case *syntax.LiteralExpr, *syntax.VectorExpr:
		return n, nil, true
	default:
		return nil, nil, false
	}
}

func arithOp(op string) bool {
	// The binary operators where operand *evaluation order* affects float
	// accumulation are +, -, *, / and at. Everything else (e.g. comparisons)
	// produces a bool and does not diverge.
	switch op {
	case "+", "-", "*", "/", "at":
		return true
	}
	return false
}

// containsRangeAgg reports whether the subtree contains a range-aggregation leaf.
func containsRangeAgg(e syntax.SampleExpr) bool {
	switch n := e.(type) {
	case *syntax.RangeAggregationExpr:
		return true
	case *syntax.VectorAggregationExpr:
		return containsRangeAgg(n.Left)
	case *syntax.BinOpExpr:
		return containsRangeAgg(n.SampleExpr) || containsRangeAgg(n.RHS)
	case *syntax.LabelReplaceExpr:
		return containsRangeAgg(n.Left)
	default:
		return false
	}
}
