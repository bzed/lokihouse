// Package multitenant lets a single lokihouse instance answer queries whose
// X-Scope-OrgID names several tenants ("a|b"): one sub-query per tenant, each
// result labelled with __tenant_id__, merged the way Loki's multi-tenant
// querier does.
//
// Copied from grafana/loki@39c256e5 pkg/querier/multi_tenant_querier.go
// (SelectLogs, SelectSamples, Label, Series, removeTenantSelector,
// replaceMatchers, filterValuesByMatchers, sliceToSet, relabel and the
// defaultTenantLabel/retainExistingPrefix constants); modifications: wraps
// lokihouse's Store instead of Loki's querier.Querier; the store-chunk
// overrides (partitionChunkRefsByTenant) are omitted because lokihouse has no
// chunks; SelectLogs closes the iterators it opened when a later tenant fails,
// as Loki's SelectSamples does. index.go fans out the index and drilldown
// endpoints (IndexStats, Volume, Patterns, DetectedLabels) the same way.
package multitenant

import (
	"context"
	"fmt"
	"math/bits"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// defaultTenantLabel is the label Loki uses to qualify results per tenant
	// in a multi-tenant query.
	defaultTenantLabel = "__tenant_id__"
	// retainExistingPrefix prefixes a tenant's own __tenant_id__ value so it
	// survives relabelling (Loki's original__tenant_id__).
	retainExistingPrefix = "original_"
)

// Store is what lokihouse's ClickHouse querier offers per tenant: the log/sample
// selectors plus the label/series metadata endpoints.
type Store interface {
	logql.Querier
	Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)
	Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error)
}

// Querier answers queries whose X-Scope-OrgID names several tenants
// ("a|b"): one sub-query per tenant, each result labelled __tenant_id__.
// Single-tenant requests pass straight through to Next.
type Querier struct {
	Next Store
}

// SelectLogs fans out to Next per tenant, wrapping each iterator so every
// stream is labelled with its tenant, and merges them in the requested order.
func (q *Querier) SelectLogs(ctx context.Context, params logql.SelectLogParams) (_ iter.EntryIterator, returnErr error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.SelectLogs(ctx, params)
	}

	selector, err := params.LogSelector()
	if err != nil {
		return nil, err
	}
	matchedTenants, filteredMatchers := filterValuesByMatchers(defaultTenantLabel, tenantIDs, selector.Matchers()...)
	if err := syntax.ValidateMatchers(filteredMatchers); err != nil {
		return nil, err
	}
	updatedSelector, err := replaceMatchers(selector, filteredMatchers)
	if err != nil {
		return nil, err
	}

	// Update the plan so the downstream store sees the tenant-filtered selector.
	params.Plan = &plan.QueryPlan{AST: updatedSelector}
	params.Selector = updatedSelector.String()

	iters := make([]iter.EntryIterator, 0, len(matchedTenants))
	defer func() {
		if returnErr == nil {
			return
		}
		for _, opened := range iters {
			util.LogErrorWithContext(ctx, "closing per-tenant entry iterator after SelectLogs failed", opened.Close)
		}
	}()
	for id := range matchedTenants {
		singleContext := user.InjectOrgID(ctx, id)
		tenantIter, err := q.Next.SelectLogs(singleContext, params)
		if err != nil {
			return nil, err
		}
		iters = append(iters, newTenantEntryIterator(tenantIter, id))
	}
	return iter.NewSortEntryIterator(iters, params.Direction), nil
}

// SelectSamples fans out to Next per tenant and merges by the requested order.
func (q *Querier) SelectSamples(ctx context.Context, params logql.SelectSampleParams) (_ iter.SampleIterator, returnErr error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.SelectSamples(ctx, params)
	}

	matchedTenants, filteredMatchers, updatedExpr, err := removeTenantSelector(params, tenantIDs)
	if err != nil {
		return nil, err
	}
	if err := syntax.ValidateMatchers(filteredMatchers); err != nil {
		return nil, err
	}
	params.Plan = &plan.QueryPlan{AST: updatedExpr}
	params.Selector = updatedExpr.String()

	iters := make([]iter.SampleIterator, 0, len(matchedTenants))
	// If SelectSamples returns an error below, close every per-tenant iterator opened so far,
	// so neither a later tenant's error nor a rejected order leaks them.
	defer func() {
		if returnErr == nil {
			return
		}
		for _, opened := range iters {
			util.LogErrorWithContext(ctx, "closing per-tenant sample iterator after SelectSamples failed", opened.Close)
		}
	}()
	for id := range matchedTenants {
		singleContext := user.InjectOrgID(ctx, id)
		tenantIter, err := q.Next.SelectSamples(singleContext, params)
		if err != nil {
			return nil, err
		}
		iters = append(iters, newTenantSampleIterator(tenantIter, id))
	}

	switch params.Order {
	case logproto.SAMPLE_ORDER_BY_STREAM:
		return iter.NewStreamFirstSortSampleIterator(iters), nil
	case logproto.SAMPLE_ORDER_BY_TIMESTAMP:
		return iter.NewTimestampFirstSortSampleIterator(iters), nil
	default:
		return nil, fmt.Errorf("unknown sample order %v", params.Order)
	}
}

// Label returns label names/values across all tenants, adding __tenant_id__
// to the name list and special-casing its values.
func (q *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if req.Values && req.Name == defaultTenantLabel {
		return &logproto.LabelResponse{Values: tenantIDs}, nil
	}
	if len(tenantIDs) == 1 {
		return q.Next.Label(ctx, req)
	}
	responses := make([]*logproto.LabelResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		singleContext := user.InjectOrgID(ctx, id)
		resp, err := q.Next.Label(singleContext, req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	if !req.Values {
		responses = append(responses, &logproto.LabelResponse{Values: []string{defaultTenantLabel}})
	}
	return logproto.MergeLabelResponses(responses)
}

// Series returns series across all tenants, each labelled with __tenant_id__.
func (q *Querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.Series(ctx, req)
	}
	responses := make([]*logproto.SeriesResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		singleContext := user.InjectOrgID(ctx, id)
		resp, err := q.Next.Series(singleContext, req)
		if err != nil {
			return nil, err
		}
		for i := range resp.GetSeries() {
			s := &resp.Series[i]
			if s.Get(defaultTenantLabel) == "" {
				s.Labels = append(s.Labels, logproto.SeriesIdentifier_LabelsEntry{Key: defaultTenantLabel, Value: id})
			}
		}
		responses[i] = resp
	}
	return logproto.MergeSeriesResponses(responses)
}

// removeTenantSelector filters the tenant IDs by any __tenant_id__ matcher in
// the selector and returns the selector with that matcher removed.
func removeTenantSelector(params logql.SelectSampleParams, tenantIDs []string) (map[string]struct{}, []*labels.Matcher, syntax.Expr, error) {
	expr, err := params.Expr()
	if err != nil {
		return nil, nil, nil, err
	}
	selector, err := expr.Selector()
	if err != nil {
		return nil, nil, nil, err
	}
	matchedTenants, filteredMatchers := filterValuesByMatchers(defaultTenantLabel, tenantIDs, selector.Matchers()...)
	updatedExpr, err := replaceMatchers(expr, filteredMatchers)
	if err != nil {
		return nil, nil, nil, err
	}
	return matchedTenants, filteredMatchers, updatedExpr, nil
}

// replaceMatchers returns a copy of expr with its (single) stream selector
// replaced by matchers.
func replaceMatchers(expr syntax.Expr, matchers []*labels.Matcher) (syntax.Expr, error) {
	expr, err := syntax.Clone(expr)
	if err != nil {
		return nil, err
	}
	selectors := 0
	expr.Walk(func(e syntax.Expr) bool {
		if concrete, ok := e.(*syntax.MatchersExpr); ok {
			selectors++
			concrete.Mts = matchers
		}
		return true
	})
	if selectors > 1 {
		return nil, fmt.Errorf("multi-tenant queries do not support expressions with more than one stream selector, got %d", selectors)
	}
	return expr, nil
}

// filterValuesByMatchers applies matchers to the tenant IDs: it returns the
// set of IDs that match any __tenant_id__ matcher (all IDs if none is present)
// plus the matchers not targeting __tenant_id__.
func filterValuesByMatchers(idLabelName string, ids []string, matchers ...*labels.Matcher) (matchedIDs map[string]struct{}, unrelatedMatchers []*labels.Matcher) {
	unrelatedMatchers = make([]*labels.Matcher, 0, len(matchers))
	matchedIDs = sliceToSet(ids)
	for _, m := range matchers {
		switch m.Name {
		case idLabelName:
			for value := range matchedIDs {
				if !m.Matches(value) {
					delete(matchedIDs, value)
				}
			}
		case retainExistingPrefix + idLabelName:
			rewrittenM := *m
			rewrittenM.Name = idLabelName
			unrelatedMatchers = append(unrelatedMatchers, &rewrittenM)
		default:
			unrelatedMatchers = append(unrelatedMatchers, m)
		}
	}
	return matchedIDs, unrelatedMatchers
}

func sliceToSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}

// relabel rewrites a stream's labels to carry the tenant's __tenant_id__,
// prefixing any existing __tenant_id__ value so it is preserved.
type relabel struct {
	tenantID string
	cache    map[string]labels.Labels
}

func (r relabel) relabel(original string) string {
	lbls, ok := r.cache[original]
	if ok {
		return lbls.String()
	}
	// The labels come from our own iterator, which formats them with
	// labels.String; Loki ignores the parse error here too.
	lbls, _ = syntax.ParseLabels(original)
	builder := labels.NewBuilder(lbls).Del(defaultTenantLabel)
	if lbls.Has(defaultTenantLabel) {
		builder.Set(retainExistingPrefix+defaultTenantLabel, lbls.Get(defaultTenantLabel))
	}
	builder.Set(defaultTenantLabel, r.tenantID)
	lbls = builder.Labels()
	r.cache[original] = lbls
	return lbls.String()
}

// tenantEntryIterator wraps an entry iterator and labels each stream with the
// tenant ID (Loki's TenantEntryIterator, using the exported Loki iterator).
type tenantEntryIterator struct {
	iter.EntryIterator
	relabel
}

func newTenantEntryIterator(ei iter.EntryIterator, id string) *tenantEntryIterator {
	return &tenantEntryIterator{EntryIterator: ei, relabel: relabel{tenantID: id, cache: map[string]labels.Labels{}}}
}

func (i *tenantEntryIterator) Labels() string { return i.relabel.relabel(i.EntryIterator.Labels()) }

// tenantSampleIterator wraps a sample iterator, labelling each stream and
// mixing the tenant into StreamHash so same-labelled streams from different
// tenants do not merge (Loki's TenantSampleIterator behaviour).
type tenantSampleIterator struct {
	iter.SampleIterator
	relabel
	tenantHash uint64
}

func newTenantSampleIterator(si iter.SampleIterator, tenantID string) *tenantSampleIterator {
	return &tenantSampleIterator{
		SampleIterator: si,
		relabel:        relabel{tenantID: tenantID, cache: map[string]labels.Labels{}},
		tenantHash:     labels.StableHash(labels.FromStrings(defaultTenantLabel, tenantID)),
	}
}

func (i *tenantSampleIterator) Labels() string { return i.relabel.relabel(i.SampleIterator.Labels()) }

func (i *tenantSampleIterator) StreamHash() uint64 {
	return i.tenantHash ^ bits.RotateLeft64(i.SampleIterator.StreamHash(), 32)
}
