package pushdown

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase/definitions"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func series(lbl string, ts ...int64) promql.Series {
	s := promql.Series{Metric: labels.FromStrings("a", lbl)}
	for _, t := range ts {
		s.Floats = append(s.Floats, promql.FPoint{T: t, F: float64(t) * 1.5})
	}
	return s
}

func TestMergeLegResults_ConcatenatesAndSorts(t *testing.T) {
	r0 := logqlmodel.Result{Data: promql.Matrix{series("2", 1, 2), series("1", 1)}}
	r1 := logqlmodel.Result{Data: promql.Matrix{series("1", 3), series("2", 3, 4)}}
	got, err := mergeLegResults(t.Context(), []logqlmodel.Result{r0, r1})
	require.NoError(t, err)
	require.Equal(t, promql.Matrix{series("1", 1, 3), series("2", 1, 2, 3, 4)}, got.Data)
	require.EqualValues(t, 2, got.Statistics.Summary.Splits)
}

func TestMergeLegResults_EmptyAndPartialSeries(t *testing.T) {
	got, err := mergeLegResults(t.Context(), []logqlmodel.Result{
		{Data: promql.Matrix{}}, {Data: promql.Matrix(nil)},
	})
	require.NoError(t, err)
	m, ok := got.Data.(promql.Matrix)
	require.True(t, ok)
	require.NotNil(t, m)
	require.Len(t, m, 0)

	got, err = mergeLegResults(t.Context(), []logqlmodel.Result{
		{Data: promql.Matrix{}}, {Data: promql.Matrix{series("x", 5, 6)}},
	})
	require.NoError(t, err)
	require.Equal(t, promql.Matrix{series("x", 5, 6)}, got.Data)
}

func TestMergeLegResults_DedupesWarnings(t *testing.T) {
	h := []*definitions.PrometheusResponseHeader{{Name: "h", Values: []string{"v"}}}
	got, err := mergeLegResults(t.Context(), []logqlmodel.Result{
		{Data: promql.Matrix{}, Warnings: []string{"w1", "w2"}, Headers: h},
		{Data: promql.Matrix{}, Warnings: []string{"w2"}, Headers: h},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"w1", "w2"}, got.Warnings)
	require.Equal(t, h, got.Headers)
}

func TestMergeLegResults_RejectsNonMatrix(t *testing.T) {
	_, err := mergeLegResults(t.Context(), []logqlmodel.Result{{Data: promql.Vector{}}})
	require.ErrorContains(t, err, "promql.Vector")
}
