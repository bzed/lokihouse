package pushdown

import (
	"strings"
	"testing"

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
	// ok=false shapes: a root vector agg (sum, topk, ...) or a root arithmetic
	// binop (+, -, *, /, at) whose operand subtree contains a range-agg leaf
	// diverges from Loki's inline evaluation in float accumulation order, so
	// Map refuses and the caller runs the original query on Tier B.
	for _, q := range []string{
		`sum(rate({a="b"}[5m]))`,
		`topk(3, sum by (x) (count_over_time({a="b"} | json [1m])))`,
		`sum(rate({a="b"}[5m])) * 2`,
		`sum(rate({a="b"}[5m])) / sum(rate({a="c"}[5m]))`,
		`sum by (x) (rate({a="b"}[5m])) / sum by (x) (rate({a="c"}[5m]))`,
	} {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		_, _, ok := Map(expr)
		require.False(t, ok, q)
	}
}
