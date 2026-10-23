package pushdown

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	querierdeletion "github.com/grafana/loki/v3/pkg/querier/deletion"

	"github.com/bzed/lokihouse/internal/chstore"
)

// TierA executes range-aggregation leaves in ClickHouse.
type TierA struct {
	Conn       driver.Conn
	MaxBuckets int64 // default 100_000; above it Exec returns ErrFallback
	Limits     logql.Limits
	// Deletes, when set, is checked at the start of Exec: a pending delete
	// overlapping the leaf's range makes the leaf fall back to Tier B, which
	// applies Loki's delete filter.
	Deletes querierdeletion.DeleteGetter
}

// Eligible reports whether the range-aggregation expression can be executed
// in Tier A.
func (a *TierA) Eligible(e *syntax.RangeAggregationExpr) bool {
	switch e.Operation {
	case syntax.OpRangeTypeCount, syntax.OpRangeTypeRate,
		syntax.OpRangeTypeBytes, syntax.OpRangeTypeBytesRate:
	default:
		return false
	}
	if e.Grouping != nil {
		return false
	}
	if e.Left.Unwrap != nil {
		return false
	}
	_, ok := chstore.ExactLineFilterSQL(e.Left.Left)
	return ok
}

// Exec executes the range-aggregation leaf in Tier A.
func (a *TierA) Exec(ctx context.Context, e *syntax.RangeAggregationExpr, p logql.Params) (logqlmodel.Result, error) {
	maxBuckets := a.MaxBuckets
	if maxBuckets == 0 {
		maxBuckets = 100_000
	}
	filter, _ := chstore.ExactLineFilterSQL(e.Left.Left)

	r := int64(e.Left.Interval)
	o := int64(e.Left.Offset)
	st := int64(p.Step())
	startNS := p.Start().UnixNano()
	endNS := p.End().UnixNano()

	S := startNS - o
	E := endNS - o

	// g = gcd(st, r), with g = r when st == 0
	var g int64
	if st == 0 {
		g = r
	} else {
		g = gcd(st, r)
	}

	origin := S - r
	numBuckets := (E-origin)/g + 1
	if numBuckets > maxBuckets {
		return logqlmodel.Result{}, ErrFallback
	}

	// A pending delete overlapping the full read range [origin, E] cannot be
	// evaluated in Tier A: fall back to Tier B, which filters the deleted
	// lines out.
	if a.Deletes != nil {
		dels, err := querierdeletion.DeletesForUserQuery(ctx, time.Unix(0, origin), time.Unix(0, E), a.Deletes)
		if err != nil {
			return logqlmodel.Result{}, err
		}
		if len(dels) > 0 {
			return logqlmodel.Result{}, ErrFallback // Tier B applies the delete filter
		}
	}

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return logqlmodel.Result{}, err
	}

	sel := e.Left.Left
	streams, err := chstore.ResolveStreams(ctx, a.Conn, tenantID, sel.Matchers(), time.Unix(0, origin), time.Unix(0, E))
	if err != nil {
		return logqlmodel.Result{}, err
	}
	if len(streams) == 0 {
		// Non-nil empty Data: Loki's step evaluator rejects a nil res.Data.
		return logqlmodel.Result{Data: promql.Matrix{}}, nil
	}
	byID := make(map[uint64]chstore.Stream, len(streams))
	var ids []uint64
	for _, s := range streams {
		byID[s.ID] = s
		ids = append(ids, s.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	rows, err := chstore.BucketCounts(ctx, a.Conn, tenantID, ids, origin, E, g, filter, chstore.BucketOpts{
		Bytes: e.Operation == syntax.OpRangeTypeBytes || e.Operation == syntax.OpRangeTypeBytesRate,
		AllMD: true,
	})
	if err != nil {
		return logqlmodel.Result{}, err
	}

	// Resolve the sample extractor once; it maps (stream labels, structured
	// metadata) to the series label set, exactly like Loki's range-vector path.
	extractor, err := e.Extractor()
	if err != nil {
		return logqlmodel.Result{}, err
	}
	streamEx := make(map[uint64]log.StreamSampleExtractor)

	// Group rows by series label set. Series labels = the stream's real labels
	// (BaseLabels, which does not run the line-filter pipeline) merged with the
	// entry's structured metadata. Metadata wins over a same-named stream label;
	// a clash spills to a _extracted copy, matching Loki's count_over_time
	// semantics. Like Loki's range vector, different (stream, metadata) pairs
	// with an equal label set are one series.
	type rowKey struct {
		StreamID uint64
		MD       string
	}
	type seriesData struct {
		lbls    labels.Labels
		buckets map[int64]bucketCount
	}
	labelsOf := make(map[rowKey]string)
	seriesMap := make(map[string]*seriesData)
	for _, row := range rows {
		rk := rowKey{row.StreamID, mapToString(row.MD)}
		key, ok := labelsOf[rk]
		if !ok {
			se := streamEx[row.StreamID]
			if se == nil {
				se = extractor.ForStream(byID[row.StreamID].Labels)
				streamEx[row.StreamID] = se
			}
			ls := seriesLabels(se, labels.FromMap(row.MD))
			key = ls.String()
			labelsOf[rk] = key
			if seriesMap[key] == nil {
				seriesMap[key] = &seriesData{lbls: ls, buckets: make(map[int64]bucketCount)}
			}
		}
		bc := seriesMap[key].buckets[row.Bucket]
		seriesMap[key].buckets[row.Bucket] = bucketCount{bc.count + row.Count, bc.bytes + row.Bytes}
	}

	maxSeries := a.Limits.MaxQuerySeries(tenantID)
	if len(seriesMap) > maxSeries {
		return logqlmodel.Result{}, logqlmodel.NewSeriesLimitError(maxSeries)
	}

	// Build the result
	var matrix promql.Matrix
	rangeSec := time.Duration(r).Seconds() // Loki divides by selRange.Seconds()
	for _, sd := range seriesMap {
		// Slide over the sorted non-empty buckets with two pointers. The window
		// (tShifted - r, tShifted] is exactly the buckets j in (lo, hi] with
		// lo = (tShifted-r-origin)/g and hi = (tShifted-origin)/g: both divisions
		// are exact since g divides st, r and tShifted-origin, and both bounds
		// only grow with k.
		js := make([]int64, 0, len(sd.buckets))
		for j := range sd.buckets {
			js = append(js, j)
		}
		slices.Sort(js)
		var n, b uint64
		in, out := 0, 0 // js[out:in] are the buckets inside the window
		series := promql.Series{Metric: sd.lbls}
		for k := int64(0); ; k++ {
			// t = start + k*step; t_shifted = t - offset = S + k*step
			tShifted := S + k*st
			if tShifted > E || (st == 0 && k > 0) {
				// st == 0 is an instant query: exactly one evaluation at start.
				break
			}
			lo, hi := (tShifted-r-origin)/g, (tShifted-origin)/g
			for ; in < len(js) && js[in] <= hi; in++ {
				n += sd.buckets[js[in]].count
				b += sd.buckets[js[in]].bytes
			}
			for ; out < in && js[out] <= lo; out++ {
				n -= sd.buckets[js[out]].count
				b -= sd.buckets[js[out]].bytes
			}
			if n == 0 {
				continue
			}
			var val float64
			switch e.Operation {
			case syntax.OpRangeTypeCount:
				val = float64(n)
			case syntax.OpRangeTypeRate:
				val = float64(n) / rangeSec
			case syntax.OpRangeTypeBytes:
				val = float64(b)
			case syntax.OpRangeTypeBytesRate:
				val = float64(b) / rangeSec
			}
			// Output timestamp in ms: t_shifted/1e6 + offset/1e6
			ts := tShifted/1e6 + o/1e6
			series.Floats = append(series.Floats, promql.FPoint{T: ts, F: val})
		}
		if len(series.Floats) > 0 { // rows only between windows (step > range) emit no series
			matrix = append(matrix, series)
		}
	}
	sort.Slice(matrix, func(i, j int) bool {
		return labels.Compare(matrix[i].Metric, matrix[j].Metric) < 0
	})
	return logqlmodel.Result{Data: matrix}, nil
}

// seriesLabels returns the series label set for one (stream, structured
// metadata) pair: the stream's base labels merged with the metadata, where
// metadata wins over a same-named stream label and a clash spills to a
// _extracted copy — exactly how Loki's range-vector path builds series.
func seriesLabels(se log.StreamSampleExtractor, md labels.Labels) labels.Labels {
	b := log.NewBaseLabelsBuilder().ForLabels(se.BaseLabels().Labels(), 0)
	if md.Len() > 0 {
		b.Add(log.StructuredMetadataLabel, md)
	}
	return b.LabelsResult().Labels()
}

type bucketCount struct {
	count uint64
	bytes uint64
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func mapToString(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb []byte
	for i, k := range keys {
		if i > 0 {
			sb = append(sb, ',')
		}
		sb = append(sb, k...)
		sb = append(sb, '=', '"')
		sb = append(sb, m[k]...)
		sb = append(sb, '"')
	}
	return string(sb)
}
