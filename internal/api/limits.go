// Package api query limits.
//
// Copied from grafana/loki@fbc3f077 pkg/querier/queryrange/limits.go
// (validateMaxEntriesLimits, validateMatchers, requiredLabelsErrTmpl,
// requiredNumberLabelsErrTmpl, maxSeriesErrTmpl), pkg/querier/queryrange/
// roundtrip.go (the matcher and entry-limit part of roundTripper.Do), the
// lookback and length part of pkg/querier/limits/validation.go
// ValidateQueryTimeRangeLimits, and the matcher-count check of
// pkg/querier/limits/validation.go ValidateQueryRequest; modifications: the
// two validate* helpers take the tenant list and the *validation.Overrides as
// parameters (Loki reads the tenant list from ctx itself); the lookback clamp
// and the max-query-length check use the smallest positive per-tenant value,
// as in the single-binary distributor; an entirely-looked-back range returns
// an empty result instead of an error, as in Loki's roundTripper.
package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	util_validation "github.com/grafana/loki/v3/pkg/util/validation"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	// maxSeriesErrTmpl is Loki's ErrMaxEntriesLimit message.
	maxSeriesErrTmpl            = "max entries limit per query exceeded, limit > max_entries_limit_per_query (%d > %d)"
	requiredLabelsErrTmpl       = "stream selector is missing required matchers [%s], labels present in the query were [%s]"
	requiredNumberLabelsErrTmpl = "stream selector has less label matchers than required: (present: [%s], number_present: %d, required_number_label_matchers: %d)"
	maxStreamsMatchersErrTmpl   = "max streams matchers per query exceeded, matchers-count > limit (%d > %d)"
)

// checkQuery applies Loki's query limits to expr over [start, end] with the
// entry limit: required labels and entry limits (query-frontend roundtrip),
// max_query_lookback and max_query_length (limits middleware) and
// max_streams_matchers_per_query (querier). It returns the start clamped to
// the lookback, and empty=true when the whole range lies before it (Loki
// answers with an empty result).
func (a *API) checkQuery(ctx context.Context, expr syntax.Expr, start, end time.Time, limit uint32) (time.Time, bool, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return start, false, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	var groups [][]*labels.Matcher
	switch e := expr.(type) {
	case syntax.SampleExpr:
		gs, _ := e.MatcherGroups() // Loki ignores the error here too ("handled later")
		for _, g := range gs {
			groups = append(groups, g.Matchers)
		}
	case syntax.LogSelectorExpr:
		if err := validateMaxEntriesLimits(ctx, a.Limits, tenants, limit); err != nil {
			return start, false, err
		}
		groups = append(groups, e.Matchers())
	}
	for _, m := range groups {
		if err := validateMatchers(ctx, a.Limits, tenants, m); err != nil {
			return start, false, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
		}
		for _, id := range tenants {
			if maxMatchers := a.Limits.MaxStreamsMatchersPerQuery(ctx, id); len(m) > maxMatchers {
				return start, false, httpgrpc.Errorf(http.StatusBadRequest, maxStreamsMatchersErrTmpl, len(m), maxMatchers)
			}
		}
	}
	lookback := util_validation.SmallestPositiveNonZeroDurationPerTenant(tenants, func(id string) time.Duration { return a.Limits.MaxQueryLookback(ctx, id) })
	if lookback > 0 {
		minStart := time.Now().Add(-lookback)
		if end.Before(minStart) {
			return start, true, nil
		}
		if start.Before(minStart) {
			start = minStart
		}
	}
	length := util_validation.SmallestPositiveNonZeroDurationPerTenant(tenants, func(id string) time.Duration { return a.Limits.MaxQueryLength(ctx, id) })
	if length > 0 {
		if l := timestamp.Time(end.UnixMilli()).Sub(timestamp.Time(start.UnixMilli())); l > length {
			return start, false, fmt.Errorf("%w: %w", logqlmodel.ErrMaxQueryLength,
				httpgrpc.Errorf(http.StatusBadRequest, util_validation.ErrQueryTooLong, l, model.Duration(length)))
		}
	}
	return start, false, nil
}

// emptyResult is Loki's NewEmptyResponse for a query: no streams, or an empty
// vector or matrix.
func emptyResult(expr syntax.Expr, instant bool) parser.Value {
	if _, ok := expr.(syntax.SampleExpr); ok {
		if instant {
			return promql.Vector{}
		}
		return promql.Matrix{}
	}
	return logqlmodel.Streams{}
}

// validateMaxEntriesLimits is Loki's roundTripper entry-limit check
// (pkg/querier/queryrange/limits.go), with the tenant list taken as a
// parameter.
func validateMaxEntriesLimits(ctx context.Context, limits *validation.Overrides, tenants []string, reqLimit uint32) error {
	maxEntriesLimit := util_validation.SmallestPositiveNonZeroIntPerTenant(tenants, func(id string) int { return limits.MaxEntriesLimitPerQuery(ctx, id) })
	if int(reqLimit) > maxEntriesLimit && maxEntriesLimit != 0 {
		return fmt.Errorf("%w: %w", logqlmodel.ErrMaxEntriesLimit,
			httpgrpc.Errorf(http.StatusBadRequest, maxSeriesErrTmpl, reqLimit, maxEntriesLimit))
	}
	return nil
}

// validateMatchers is Loki's roundTripper matcher check
// (pkg/querier/queryrange/limits.go), with the tenant list taken as a
// parameter.
func validateMatchers(ctx context.Context, limits *validation.Overrides, tenants []string, matchers []*labels.Matcher) error {
	actual := make(map[string]struct{}, len(matchers))
	var present []string
	for _, m := range matchers {
		actual[m.Name] = struct{}{}
		present = append(present, m.Name)
	}

	// Enforce RequiredLabels limit.
	for _, id := range tenants {
		required := limits.RequiredLabels(ctx, id)
		var missing []string
		for _, label := range required {
			if _, found := actual[label]; !found {
				missing = append(missing, label)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf(requiredLabelsErrTmpl, strings.Join(missing, ", "), strings.Join(present, ", "))
		}
	}

	// Enforce RequiredNumberLabels limit.
	requiredNumberLabels := util_validation.SmallestPositiveNonZeroIntPerTenant(tenants, func(id string) int { return limits.RequiredNumberLabels(ctx, id) })
	if requiredNumberLabels > 0 && len(present) < requiredNumberLabels {
		return fmt.Errorf(requiredNumberLabelsErrTmpl, strings.Join(present, ", "), len(present), requiredNumberLabels)
	}
	return nil
}
