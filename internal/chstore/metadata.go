package chstore

import (
	"context"
	"sort"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

func (q *Querier) streamsFor(ctx context.Context, selector string, from, through time.Time) ([]Stream, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	var ms []*labels.Matcher
	if selector != "" {
		sel, err := syntax.ParseLogSelector(selector, true)
		if err != nil {
			return nil, err
		}
		ms = sel.Matchers()
	}
	return ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
}

// Label returns the label names or values matching the request.
func (q *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	streams, err := q.streamsFor(ctx, req.Query, *req.Start, *req.End)
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) {
			if !req.Values {
				set[l.Name] = struct{}{}
			} else if l.Name == req.Name {
				set[l.Value] = struct{}{}
			}
		})
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return &logproto.LabelResponse{Values: out}, nil
}

// Series returns the time series matching the request.
func (q *Querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	groups := req.Groups
	if len(groups) == 0 {
		groups = []string{""}
	}
	seen := map[uint64]labels.Labels{}
	for _, g := range groups {
		streams, err := q.streamsFor(ctx, g, req.Start, req.End)
		if err != nil {
			return nil, err
		}
		for _, s := range streams {
			seen[s.ID] = s.Labels
		}
	}
	all := make([]labels.Labels, 0, len(seen))
	for _, ls := range seen {
		all = append(all, ls)
	}
	sort.Slice(all, func(i, j int) bool { return labels.Compare(all[i], all[j]) < 0 })
	resp := &logproto.SeriesResponse{}
	for _, ls := range all {
		var id logproto.SeriesIdentifier
		ls.Range(func(l labels.Label) {
			id.Labels = append(id.Labels, logproto.SeriesIdentifier_LabelsEntry{Key: l.Name, Value: l.Value})
		})
		resp.Series = append(resp.Series, id)
	}
	return resp, nil
}
