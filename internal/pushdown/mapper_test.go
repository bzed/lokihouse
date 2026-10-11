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
