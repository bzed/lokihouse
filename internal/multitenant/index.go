package multitenant

// Copied from grafana/loki@39c256e5 pkg/querier/multi_tenant_querier.go
// (IndexStats, Volume, Patterns, DetectedLabels); modifications: wraps
// lokihouse's IndexStore instead of Loki's querier.Querier, and IndexStats
// takes the codec's *logproto.IndexStatsRequest instead of *loghttp.RangeQuery.

import (
	"context"
	"sort"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/querier/pattern"
	"github.com/grafana/loki/v3/pkg/storage/detected"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/seriesvolume"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
)

// IndexStore answers the index and drilldown endpoints for the single tenant
// in the context.
type IndexStore interface {
	IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error)
	Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error)
	DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error)
	Patterns(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error)
}

// Index answers the index and drilldown endpoints for an X-Scope-OrgID naming
// several tenants: one call per tenant, merged as Loki merges them. A single
// tenant passes straight through (Volume always merges, as in Loki).
type Index struct {
	Next IndexStore
}

// IndexStats sums the per-tenant statistics.
func (q *Index) IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.IndexStats(ctx, req)
	}
	responses := make([]*stats.Stats, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.IndexStats(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	merged := stats.MergeStats(responses...)
	return &merged, nil
}

// Volume merges the per-tenant volumes, keeping the request's limit.
func (q *Index) Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	responses := make([]*logproto.VolumeResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.Volume(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	return seriesvolume.Merge(responses, req.Limit), nil
}

// Patterns merges the per-tenant pattern series.
func (q *Index) Patterns(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.Patterns(ctx, req)
	}
	responses := make([]*logproto.QueryPatternsResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.Patterns(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	return pattern.MergePatternResponses(responses), nil
}

// DetectedLabels merges the per-tenant HyperLogLog sketches and sorts the
// labels by cardinality.
func (q *Index) DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.DetectedLabels(ctx, req)
	}
	var all []*logproto.DetectedLabel
	for _, id := range tenantIDs {
		resp, err := q.Next.DetectedLabels(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		all = append(all, resp.DetectedLabels...)
	}
	merged, err := detected.MergeLabels(all)
	if err != nil {
		return nil, err
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Cardinality < merged[j].Cardinality })
	return &logproto.DetectedLabelsResponse{DetectedLabels: merged}, nil
}
