package pushdown

import (
	"context"
	"fmt"
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

	"github.com/bzed/lokihouse/internal/chstore"
)

// TierA executes range-aggregation leaves in ClickHouse.
type TierA struct {
	Conn       driver.Conn
	MaxBuckets int64 // default 100_000; above it the leaf is not eligible
	Limits     logql.Limits
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
	if a.MaxBuckets == 0 {
		a.MaxBuckets = 100_000
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
	numBuckets := (E - origin)/g + 1
	if numBuckets > a.MaxBuckets {
		return logqlmodel.Result{}, fmt.Errorf("bucket count %d exceeds max %d", numBuckets, a.MaxBuckets)
	}

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return logqlmodel.Result{}, err
	}

	sel := e.Left.Left
	streams, err := chstore.ResolveStreams(ctx, a.Conn, tenantID, sel.Matchers(), time.Unix(0, origin), time.Unix(0, E))
	if err != nil || len(streams) == 0 {
		return logqlmodel.Result{}, err
	}
	byID := make(map[uint64]chstore.Stream, len(streams))
	var ids []uint64
	for _, s := range streams {
		byID[s.ID] = s
		ids = append(ids, s.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	rows, err := chstore.BucketCounts(ctx, a.Conn, tenantID, ids, origin, E, g, filter)
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

	// Group rows by series key (StreamID, sorted MD string)
	type seriesKey struct {
		StreamID uint64
		MD       string
	}
	type seriesData struct {
		buckets map[int64]bucketCount
		mdMap   map[string]string
	}
	seriesMap := make(map[seriesKey]*seriesData)
	for _, row := range rows {
		key := seriesKey{row.StreamID, mapToString(row.MD)}
		sd, ok := seriesMap[key]
		if !ok {
			sd = &seriesData{buckets: make(map[int64]bucketCount), mdMap: row.MD}
			seriesMap[key] = sd
		}
		sd.buckets[row.Bucket] = bucketCount{row.Count, row.Bytes}
	}

	maxSeries := a.Limits.MaxQuerySeries(tenantID)
	if len(seriesMap) > maxSeries {
		return logqlmodel.Result{}, logqlmodel.NewSeriesLimitError(maxSeries)
	}

	// Build the result
	var matrix promql.Matrix
	for key, sd := range seriesMap {
		strm := byID[key.StreamID]
		se := streamEx[key.StreamID]
		if se == nil {
			se = extractor.ForStream(strm.Labels)
			streamEx[key.StreamID] = se
		}
		// Series label set = the stream's real labels (BaseLabels, which does not
		// run the line-filter pipeline) merged with the entry's structured
		// metadata. Metadata wins over a same-named stream label; a clash spills
		// to a _extracted copy, matching Loki's count_over_time semantics.
		series := promql.Series{Metric: seriesLabels(se, labels.FromMap(sd.mdMap))}
		for k := int64(0); ; k++ {
			// t = start + k*step (the evaluation time)
			t := startNS + k*st
			if t > endNS {
				break
			}
			// t_shifted = t - offset = S + k*st
			tShifted := t - o
			if tShifted > E {
				break
			}
			// Window: (t - r - o, t - o] = (tShifted - r, tShifted]
			// Buckets: bucket j holds (origin + (j-1)*g, origin + j*g]
			// A bucket j is included if it overlaps the window:
			//   origin + (j-1)*g < tShifted  AND  origin + j*g > tShifted - r
			//   j <= (tShifted - origin)/g   AND   j >= (tShifted - r - origin)/g + 1
			jStart := (tShifted - r - origin) / g
			jEnd := (tShifted - origin) / g
			// The left bound is exclusive: if tShifted - r is exactly at a bucket
			// boundary, we exclude that bucket.
			if (tShifted - r - origin) % g == 0 {
				jStart++
			}
			var n, b uint64
			for j := jStart; j <= jEnd; j++ {
				bc, ok := sd.buckets[j]
				if !ok {
					continue
				}
				n += bc.count
				b += bc.bytes
			}
			if n == 0 {
				continue
			}
			var val float64
			switch e.Operation {
			case syntax.OpRangeTypeCount:
				val = float64(n)
			case syntax.OpRangeTypeRate:
				val = float64(n) / (float64(r) / 1e9)
			case syntax.OpRangeTypeBytes:
				val = float64(b)
			case syntax.OpRangeTypeBytesRate:
				val = float64(b) / (float64(r) / 1e9)
			}
			// Output timestamp in ms: t_shifted/1e6 + offset/1e6
			ts := tShifted/1e6 + o/1e6
			series.Floats = append(series.Floats, promql.FPoint{T: ts, F: val})
		}
		matrix = append(matrix, series)
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
