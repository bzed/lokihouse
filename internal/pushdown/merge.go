package pushdown

import (
	"context"
	"fmt"
	"sort"

	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/logqlmodel/metadata"
	"github.com/prometheus/prometheus/promql"
)

// mergeLegResults joins the matrices of consecutive time legs into the matrix
// of the whole range. Legs have disjoint timestamps, so points are only
// concatenated per series in leg order; nothing is re-aggregated.
func mergeLegResults(results []logqlmodel.Result) (logqlmodel.Result, error) {
	var out logqlmodel.Result
	index := map[string]int{}
	merged := make(promql.Matrix, 0, 16)
	md, mdCtx := metadata.NewContext(context.Background())
	for _, r := range results {
		m, ok := r.Data.(promql.Matrix)
		if !ok {
			return logqlmodel.Result{}, fmt.Errorf("merge legs: unexpected result type %T", r.Data)
		}
		for _, s := range m {
			key := s.Metric.String()
			i, ok := index[key]
			if !ok {
				i = len(merged)
				index[key] = i
				merged = append(merged, promql.Series{Metric: s.Metric})
			}
			merged[i].Floats = append(merged[i].Floats, s.Floats...)
			merged[i].Histograms = append(merged[i].Histograms, s.Histograms...)
		}
		out.Statistics.MergeSplit(r.Statistics)
		for _, w := range r.Warnings {
			md.AddWarning(w)
		}
		if err := metadata.JoinHeaders(mdCtx, r.Headers); err != nil {
			return logqlmodel.Result{}, fmt.Errorf("merge legs: join headers: %w", err)
		}
	}
	sort.Sort(merged)
	out.Data = merged
	out.Warnings = md.Warnings()
	out.Headers = md.Headers()
	return out, nil
}
