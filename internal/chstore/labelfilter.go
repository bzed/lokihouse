package chstore

import (
	"strings"

	bloomv1 "github.com/grafana/loki/v3/pkg/storage/bloom/v1"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// MetadataFilterSQL maps the label filters before the first parser stage of expr to
// conditions over structured_metadata, which the sm_keys and sm_vals text indexes
// answer. The filters come from Loki's bloom extractor
// (bloomv1.ExtractTestableLabelMatchers); they are ANDed.
//
// The fragment is a necessary condition: the pipeline re-applies the filters in Go. A
// filter on a name that a resolved stream carries as a stream label is not pushed (the
// label may satisfy the filter), nor is a name starting with "__", nor an unsupported
// filter (a regex that is not an alternation of literals, !=, comparisons).
func MetadataFilterSQL(expr syntax.LogSelectorExpr, streams []Stream) Fragment {
	streamLabels := map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) { streamLabels[l.Name] = struct{}{} })
	}
	var parts []Fragment
	for _, m := range bloomv1.ExtractTestableLabelMatchers(expr) {
		parts = append(parts, metadataMatcherSQL(m, streamLabels))
	}
	return and(parts)
}

func metadataMatcherSQL(m bloomv1.LabelMatcher, streamLabels map[string]struct{}) Fragment {
	pushable := func(key string) bool {
		_, isStream := streamLabels[key]
		return !isStream && !strings.HasPrefix(key, "__")
	}
	switch m := m.(type) {
	case bloomv1.KeyValueMatcher:
		if m.Value == "" || !pushable(m.Key) {
			return Fragment{}
		}
		return Fragment{SQL: "structured_metadata[?] = ?", Args: []any{m.Key, m.Value}}
	case bloomv1.KeyMatcher:
		if !pushable(m.Key) {
			return Fragment{}
		}
		return Fragment{SQL: "mapContainsKey(structured_metadata, ?)", Args: []any{m.Key}}
	case bloomv1.AndLabelMatcher:
		l, r := metadataMatcherSQL(m.Left, streamLabels), metadataMatcherSQL(m.Right, streamLabels)
		switch {
		case l.Empty():
			return r
		case r.Empty():
			return l
		}
		return Fragment{SQL: "(" + l.SQL + " AND " + r.SQL + ")", Args: append(l.Args, r.Args...)}
	case bloomv1.OrLabelMatcher:
		l, r := metadataMatcherSQL(m.Left, streamLabels), metadataMatcherSQL(m.Right, streamLabels)
		if l.Empty() || r.Empty() {
			return Fragment{}
		}
		return Fragment{SQL: "(" + l.SQL + " OR " + r.SQL + ")", Args: append(l.Args, r.Args...)}
	}
	return Fragment{}
}
