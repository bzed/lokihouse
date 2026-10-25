package pushdown

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	utilvalidation "github.com/grafana/loki/v3/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"golang.org/x/sync/semaphore"
)

// SplitLimits are the per-tenant limits the Splitter reads; *validation.Overrides
// satisfies it.
type SplitLimits interface {
	logql.Limits
	parallelismLimits
	QuerySplitDuration(userID string) time.Duration
}

// SplitMetrics are the splitter's Prometheus metrics. A nil *SplitMetrics
// observes nothing.
type SplitMetrics struct{ partitions prometheus.Histogram }

// NewSplitMetrics registers loki_query_frontend_partitions on r.
//
// Copied from grafana/loki@bc1962860a1607eccb7e0c8ee206970b292b9e80 pkg/querier/queryrange/split_by_interval.go;
// modifications: type and field names, registered by the caller's registerer.
func NewSplitMetrics(r prometheus.Registerer) *SplitMetrics {
	return &SplitMetrics{
		partitions: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Namespace: constants.Loki,
			Name:      "query_frontend_partitions",
			Help:      "Number of time-based partitions (sub-requests) per request",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 5), // 1 -> 1024
		}),
	}
}

func (m *SplitMetrics) observe(legs int) {
	if m != nil {
		m.partitions.Observe(float64(legs))
	}
}

// Splitter has the shape of *logql.QueryEngine. Its queries cut a metric
// range query into time legs on the query's own step grid, evaluate the legs
// on Engine in parallel (see runPieces) and merge them into the answer the
// unsplit query would give. Everything else runs unsplit on Engine.
type Splitter struct {
	Engine  *logql.QueryEngine
	Limits  SplitLimits
	Budget  *semaphore.Weighted // process-wide connection budget, see NewConnBudget
	Metrics *SplitMetrics
}

// Query returns the query to execute for p.
func (s *Splitter) Query(p logql.Params) logql.Query { return &splitQuery{s: s, p: p} }

type splitQuery struct {
	s *Splitter
	p logql.Params
}

// seriesLimitPartialResultsWarning returns the warning of a query that returns partial results
// because it reached the series limit.
//
// Copied from grafana/loki@bc1962860a1607eccb7e0c8ee206970b292b9e80 pkg/logql/engine.go; modifications: none.
func seriesLimitPartialResultsWarning(maxSeries int) string {
	return fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", maxSeries)
}

func (q *splitQuery) Exec(ctx context.Context) (logqlmodel.Result, error) {
	s, p := q.s, q.p
	unsplit := func() (logqlmodel.Result, error) { return s.Engine.Query(p).Exec(ctx) }

	expr, ok := p.GetExpression().(syntax.SampleExpr)
	if !ok || p.Step() == 0 || !p.Start().Before(p.End()) {
		return unsplit()
	}
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil || len(tenants) != 1 {
		return unsplit()
	}
	iv := splitInterval(expr, utilvalidation.SmallestPositiveNonZeroDurationPerTenant(tenants, s.Limits.QuerySplitDuration))
	if iv == 0 {
		return unsplit()
	}
	legs := splitLegs(p.Start(), p.End(), p.Step(), iv)
	s.Metrics.observe(len(legs))
	if len(legs) == 1 {
		return unsplit()
	}

	runCtx := withRequestSlots(ctx, s.Limits)
	timeout := utilvalidation.SmallestPositiveNonZeroDurationPerTenant(tenants, func(id string) time.Duration {
		return s.Limits.QueryTimeout(runCtx, id)
	})
	if timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, timeout)
		defer cancel()
	}

	results := make([]logqlmodel.Result, len(legs))
	err = runPieces(runCtx, len(legs), connNeed(expr), requestSlots(runCtx), s.Budget, func(ctx context.Context, i int) error {
		res, err := s.Engine.Query(legParams{Params: p, start: legs[i].start, end: legs[i].end}).Exec(ctx)
		results[i] = res
		return err
	})
	if err != nil {
		return logqlmodel.Result{}, err
	}
	merged, err := mergeLegResults(ctx, results)
	if err != nil {
		return logqlmodel.Result{}, err
	}

	maxSeries := utilvalidation.SmallestPositiveIntPerTenant(tenants, s.Limits.MaxQuerySeries)
	n := len(merged.Data.(promql.Matrix))
	over := maxSeries > 0 && n > maxSeries
	if httpreq.IsLogsDrilldownRequest(ctx) {
		// Logs Drilldown gets partial results with a warning instead of an
		// error; which series the unsplit query keeps depends on its whole
		// range, so run it unsplit.
		if over || slices.ContainsFunc(results, func(r logqlmodel.Result) bool {
			return slices.Contains(r.Warnings, seriesLimitPartialResultsWarning(maxSeries))
		}) {
			return unsplit()
		}
	} else if over {
		return logqlmodel.Result{}, logqlmodel.NewSeriesLimitError(maxSeries)
	}
	return merged, nil
}
