package pushdown

import (
	"strings"
	"testing"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	// ok=true shapes: the mapped query is valid and the leaf count matches.
	for q, leaves := range map[string]int{
		`rate({a="b"}[5m])`: 1,
		`label_replace(rate({a="b"}[1m]), "d", "$1", "a", "(.*)")`: 1,
		`vector(1) + 2`: 0,
	} {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		mapped, ls, ok := Map(expr)
		require.True(t, ok, q)
		require.Len(t, ls, leaves, q)
		require.Equal(t, leaves, strings.Count(mapped.String(), "downstream<"), q)
	}
	// Root vector aggs and arithmetic map too; they agree with Loki's inline
	// evaluation within float tolerance (see Map).
	for q, leaves := range map[string]int{
		`sum(rate({a="b"}[5m]))`: 1,
		`topk(3, sum by (x) (count_over_time({a="b"} | json [1m])))`: 1,
		`sum(rate({a="b"}[5m])) / sum(rate({a="c"}[5m]))`:            2,
	} {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		_, ls, ok := Map(expr)
		require.True(t, ok, q)
		require.Len(t, ls, leaves, q)
	}
}

func TestMap_SumLeaves(t *testing.T) {
	const inner = `count_over_time({a="b"}[1m])`
	q := `sum by (level) (` + inner + `)`
	expr, err := syntax.ParseSampleExpr(q)
	require.NoError(t, err)
	before := expr.String()
	mapped, ls, ok := Map(expr)
	require.True(t, ok)
	require.Equal(t, before, expr.String(), "Map must not mutate its input")
	require.Len(t, ls, 1)
	leaf, isVec := ls[0].(*syntax.VectorAggregationExpr)
	require.True(t, isVec, "leaf is the sum")
	require.Equal(t, syntax.OpTypeSum, leaf.Operation)
	outer, isVec := mapped.(*syntax.VectorAggregationExpr)
	require.True(t, isVec)
	require.Equal(t, syntax.OpTypeSum, outer.Operation)
	require.Equal(t, leaf.Grouping, outer.Grouping)
	ds, isDS := outer.Left.(logql.DownstreamSampleExpr)
	require.True(t, isDS, "outer sum keeps a downstream child")
	require.Same(t, leaf, ds.SampleExpr)

	// Not narrowable: the leaf stays the range aggregation.
	for _, q := range []string{
		`sum without (x) (` + inner + `)`,
		`count by (level) (` + inner + `)`,
		`avg(` + inner + `)`,
	} {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		_, ls, ok := Map(expr)
		require.True(t, ok, q)
		require.Len(t, ls, 1, q)
		_, isRange := ls[0].(*syntax.RangeAggregationExpr)
		require.True(t, isRange, q)
	}

	// A sum over a sum: the inner sum is the leaf, the outer sum aggregates it.
	expr, err = syntax.ParseSampleExpr(`sum by (l) (sum by (l, m) (` + inner + `))`)
	require.NoError(t, err)
	_, ls, ok = Map(expr)
	require.True(t, ok)
	require.Len(t, ls, 1)
	_, isVec = ls[0].(*syntax.VectorAggregationExpr)
	require.True(t, isVec)
}

func TestMdKeysFor(t *testing.T) {
	g := func(q string) *syntax.Grouping {
		e, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		return e.(*syntax.VectorAggregationExpr).Grouping
	}
	require.Equal(t, []string{}, mdKeysFor(nil))
	require.Equal(t, []string{}, mdKeysFor(g(`sum by () (rate({a="b"}[1m]))`)))
	require.ElementsMatch(t, []string{"app", "level"}, mdKeysFor(g(`sum by (app, level) (rate({a="b"}[1m]))`)))
	require.ElementsMatch(t, []string{"level_extracted", "level"}, mdKeysFor(g(`sum by (level_extracted) (rate({a="b"}[1m]))`)))
}
