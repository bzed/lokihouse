package api

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	querier_limits "github.com/grafana/loki/v3/pkg/querier/limits"

	"github.com/bzed/lokihouse/internal/multitenant"
)

// tenantIndex answers the index and drilldown endpoints for the single tenant
// in the context, applying Loki's per-tenant checks (volume_enabled, the
// query time-range limits for detected_labels). Under multitenant.Index the
// checks therefore run once per tenant, so one tenant with volume disabled
// fails the whole request, as in Loki.
type tenantIndex struct{ a *API }

func (t tenantIndex) IndexStats(ctx context.Context, r *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	return t.a.Q.IndexStats(ctx, r)
}

func (t tenantIndex) Volume(ctx context.Context, r *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	if !t.a.Limits.VolumeEnabled(tenantID) {
		return nil, httpgrpc.Errorf(http.StatusNotFound, "not found")
	}
	return t.a.Q.Volume(ctx, r)
}

func (t tenantIndex) DetectedLabels(ctx context.Context, r *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	dl := *r // the per-tenant limits must not leak into the next tenant's request
	if dl.Start, dl.End, err = querier_limits.ValidateQueryTimeRangeLimits(ctx, tenantID, t.a.Limits, dl.Start, dl.End); err != nil {
		return nil, err
	}
	return t.a.Q.DetectedLabels(ctx, &dl)
}

func (t tenantIndex) Patterns(ctx context.Context, r *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	return t.a.Patterns.Query(ctx, r)
}

// index returns what the codec handler calls for the index and drilldown
// endpoints: the per-tenant adapter, fanned out when multi-tenant queries are on.
func (a *API) index() multitenant.IndexStore {
	var idx multitenant.IndexStore = tenantIndex{a: a}
	if a.MultiTenant {
		idx = &multitenant.Index{Next: idx}
	}
	return idx
}
