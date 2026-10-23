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
// A sum or sum by over a range aggregation is itself a leaf; the outer sum
// stays above it (as in Loki's shard mapper) and re-aggregates what the leaf
// returns, which lets Tier A group by only the structured-metadata keys the
// grouping can see. Other vector aggregations map their child as before.
//
// Vector aggregations and arithmetic over mapped leaves may differ from Loki's
// inline evaluation in the last float bits: Loki builds each step's vector from
// a Go map, so its own summation order is random. Results agree within the
// oracle's 1e-9 relative tolerance.
//
// The input must be a freshly parsed expression; Map never mutates it.
func Map(expr syntax.SampleExpr) (mapped syntax.SampleExpr, leaves []syntax.SampleExpr, ok bool) {
	return mapNode(expr)
}

// mapNode recurses over the sample expression tree, wrapping range-aggregation
// leaves and re-linking the nodes that own sample-expr children. ok=false (and
// leaves may be partial) on an unsupported node kind.
func mapNode(e syntax.SampleExpr) (syntax.SampleExpr, []syntax.SampleExpr, bool) {
	switch n := e.(type) {
	case *syntax.RangeAggregationExpr:
		return logql.DownstreamSampleExpr{SampleExpr: n}, []syntax.SampleExpr{n}, true
	case *syntax.VectorAggregationExpr:
		if isSumLeaf(n) {
			cp := *n
			cp.Left = logql.DownstreamSampleExpr{SampleExpr: n}
			return &cp, []syntax.SampleExpr{n}, true
		}
		child, ls, ok := mapNode(n.Left)
		if !ok {
			return nil, nil, false
		}
		cp := *n
		cp.Left = child
		return &cp, ls, true
	case *syntax.BinOpExpr:
		l, ls, ok := mapNode(n.SampleExpr)
		if !ok {
			return nil, nil, false
		}
		r, rs, ok := mapNode(n.RHS)
		if !ok {
			return nil, nil, false
		}
		cp := *n
		cp.SampleExpr = l
		cp.RHS = r
		return &cp, append(ls, rs...), true
	case *syntax.LabelReplaceExpr:
		child, ls, ok := mapNode(n.Left)
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

// isSumLeaf reports whether n is a sum, with or without by-grouping, directly
// over a range aggregation.
func isSumLeaf(n *syntax.VectorAggregationExpr) bool {
	if n.Operation != syntax.OpTypeSum || (n.Grouping != nil && n.Grouping.Without) {
		return false
	}
	_, ok := n.Left.(*syntax.RangeAggregationExpr)
	return ok
}
