package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/seriesvolume"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/prometheus/prometheus/model/labels"
)

// HourStat is one (stream, hour) bucket of stream_stats_hourly; timestamps in ns.
type HourStat struct {
	StreamID, Entries, Bytes uint64
	MinTS, MaxTS             int64
}

// HourlyStats returns the hourly buckets of the given streams that can overlap
// [from, through]. Unmerged SimpleAggregateFunction rows are merged by the GROUP BY.
func HourlyStats(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, from, through time.Time) ([]HourStat, error) {
	const q = `SELECT stream_id, sum(entries), sum(bytes),
       toUnixTimestamp64Nano(min(min_ts)), toUnixTimestamp64Nano(max(max_ts))
FROM stream_stats_hourly
WHERE tenant = ? AND stream_id IN ?
  AND hour >= toStartOfHour(fromUnixTimestamp64Nano(?, 'UTC'))
  AND hour <= fromUnixTimestamp64Nano(?, 'UTC')
GROUP BY stream_id, hour`
	rows, err := conn.Query(ctx, q, tenant, ids, from.UnixNano(), through.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HourStat
	for rows.Next() {
		var h HourStat
		if err := rows.Scan(&h.StreamID, &h.Entries, &h.Bytes, &h.MinTS, &h.MaxTS); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// overlaps and factor follow Loki's ingester getStats/getVolume, with an hourly
// bucket in place of a chunk: a bucket counts when from < max && through > min.
func overlaps(h HourStat, from, through int64) bool { return from < h.MaxTS && through > h.MinTS }

func statsFromHours(rows []HourStat, from, through int64) *logproto.IndexStatsResponse {
	res := &logproto.IndexStatsResponse{}
	streams := map[uint64]struct{}{}
	for _, h := range rows {
		if !overlaps(h, from, through) {
			continue
		}
		f := util.GetFactorOfTime(from, through, h.MinTS, h.MaxTS)
		res.Chunks++
		res.Entries += uint64(f * float64(h.Entries))
		res.Bytes += uint64(f * float64(h.Bytes))
		streams[h.StreamID] = struct{}{}
	}
	res.Streams = uint64(len(streams))
	return res
}

// Copied from grafana/loki@bc196286 pkg/ingester/instance.go (getVolume); modifications:
// ClickHouse streams and hourly buckets instead of ingester streams and chunks.
func volumeFromHours(streams []Stream, rows []HourStat, from, through int64, req *logproto.VolumeRequest, labelsToMatch map[string]struct{}, matchAny bool) *logproto.VolumeResponse {
	sizes := map[uint64]uint64{}
	seen := map[uint64]bool{}
	for _, h := range rows {
		if !overlaps(h, from, through) {
			continue
		}
		f := util.GetFactorOfTime(from, through, h.MinTS, h.MaxTS)
		sizes[h.StreamID] += uint64(float64(h.Bytes) * f)
		seen[h.StreamID] = true
	}
	aggregateBySeries := seriesvolume.AggregateBySeries(req.AggregateBy) || req.AggregateBy == ""
	volumes := map[string]uint64{}
	b := labels.NewScratchBuilder(len(labelsToMatch))
	for _, s := range streams {
		if !seen[s.ID] {
			continue
		}
		size := sizes[s.ID]
		if aggregateBySeries {
			b.Reset()
			s.Labels.Range(func(l labels.Label) {
				if _, ok := labelsToMatch[l.Name]; matchAny || ok {
					b.Add(l.Name, l.Value)
				}
			})
			volumes[b.Labels().String()] += size
			continue
		}
		s.Labels.Range(func(l labels.Label) {
			if len(req.TargetLabels) > 0 {
				if _, ok := labelsToMatch[l.Name]; matchAny || ok {
					volumes[l.Name] += size
				}
			} else {
				volumes[l.Name] += size
			}
		})
	}
	return seriesvolume.MapToVolumeResponse(volumes, int(req.Limit))
}

// Volume serves one bucket of /loki/api/v1/index/volume(_range).
func (q *Querier) Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	ms, err := syntax.ParseMatchers(req.Matchers, true)
	if err != nil && req.Matchers != seriesvolume.MatchAny {
		return nil, err
	}
	labelsToMatch, ms, matchAny := util.PrepareLabelsAndMatchers(req.TargetLabels, ms)
	matchAny = matchAny || len(ms) == 0
	from, through := req.From.Time(), req.Through.Time()
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
	if err != nil || len(streams) == 0 {
		return &logproto.VolumeResponse{Volumes: []logproto.Volume{}}, err
	}
	rows, err := HourlyStats(ctx, q.Conn, tenantID, sortedIDs(streams), from, through)
	if err != nil {
		return nil, err
	}
	return volumeFromHours(streams, rows, from.UnixNano(), through.UnixNano(), req, labelsToMatch, matchAny), nil
}

// IndexStats serves /loki/api/v1/index/stats from stream_stats_hourly.
func (q *Querier) IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	ms, err := syntax.ParseMatchers(req.Matchers, true)
	if err != nil {
		return nil, err
	}
	from, through := req.From.Time(), req.Through.Time()
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
	if err != nil || len(streams) == 0 {
		return &logproto.IndexStatsResponse{}, err
	}
	rows, err := HourlyStats(ctx, q.Conn, tenantID, sortedIDs(streams), from, through)
	if err != nil {
		return nil, err
	}
	return statsFromHours(rows, from.UnixNano(), through.UnixNano()), nil
}
