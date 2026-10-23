package chstore

import (
	"strings"

	bloomv1 "github.com/grafana/loki/v3/pkg/storage/bloom/v1"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/logql/log"
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

// extractedSuffix is what Loki appends to a label that clashes with a stream label.
const extractedSuffix = "_extracted"

// clashNames lists the stream label names that make a filter on name unsafe to push: name
// itself (the stream label may satisfy the filter) and, for <base>_extracted, base (a
// structured-metadata or parsed label named base is renamed to <base>_extracted when a
// stream carries base, so the stored key differs from the filtered name).
func clashNames(name string) []string {
	if base, ok := strings.CutSuffix(name, extractedSuffix); ok && base != "" {
		return []string{name, base}
	}
	return []string{name}
}

func clashesWithStream(name string, streamLabels map[string]struct{}) bool {
	for _, n := range clashNames(name) {
		if _, ok := streamLabels[n]; ok {
			return true
		}
	}
	return false
}

func metadataMatcherSQL(m bloomv1.LabelMatcher, streamLabels map[string]struct{}) Fragment {
	pushable := func(key string) bool {
		return !clashesWithStream(key, streamLabels) && !strings.HasPrefix(key, "__")
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

// minRunLen is the shortest run of a value worth a LIKE condition: the text index only
// answers needles of at least four characters.
const minRunLen = 4

// ParsedFilterSQL maps label equality filters after a bare `| json` or `| logfmt` to
// conditions on the line, which the line_text index answers. The value of such a label
// comes from the line, so the verbatim runs of the value must occur in it. Three
// cases need no run: the label can also come from structured metadata of that name
// (mapContainsKey), and a line that hides an ASCII letter or digit behind a \u escape
// (line_ascii_escape) may not contain the run.
//
// The fragment is a necessary condition: the pipeline re-applies the filters in Go. It
// walks the stages in order; line filters and label filters (which change no label)
// are passed over, any stage other than a parser ends the walk. Label filters before the
// first parser are MetadataFilterSQL's. A filter on a name that a resolved stream carries
// is not pushed, nor is a name starting with "__", a non-ASCII value, or a value with no
// run of at least four verbatim characters.
func ParsedFilterSQL(expr syntax.LogSelectorExpr, streams []Stream) Fragment {
	p, ok := expr.(*syntax.PipelineExpr)
	if !ok {
		return Fragment{}
	}
	streamLabels := map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) { streamLabels[l.Name] = struct{}{} })
	}
	var parts []Fragment
	parsed := false
walk:
	for _, st := range p.MultiStages {
		switch st := st.(type) {
		case *syntax.LineFilterExpr:
		case *syntax.LogfmtParserExpr:
			parsed = true
		case *syntax.LineParserExpr:
			if st.Op != syntax.OpParserTypeJSON || st.Param != "" {
				break walk
			}
			parsed = true
		case *syntax.LabelFilterExpr:
			if parsed {
				parts = append(parts, parsedLabelFilterSQL(st.LabelFilterer, streamLabels))
			}
		default:
			break walk
		}
	}
	return and(parts)
}

func parsedLabelFilterSQL(lf log.LabelFilterer, streamLabels map[string]struct{}) Fragment {
	if bf, ok := lf.(*log.BinaryLabelFilter); ok {
		l, r := parsedLabelFilterSQL(bf.Left, streamLabels), parsedLabelFilterSQL(bf.Right, streamLabels)
		if bf.And {
			switch {
			case l.Empty():
				return r
			case r.Empty():
				return l
			}
			return Fragment{SQL: "(" + l.SQL + " AND " + r.SQL + ")", Args: append(l.Args, r.Args...)}
		}
		if l.Empty() || r.Empty() {
			return Fragment{}
		}
		return Fragment{SQL: "(" + l.SQL + " OR " + r.SQL + ")", Args: append(l.Args, r.Args...)}
	}
	m := equalLabelMatcher(lf)
	if m == nil || m.Value == "" || strings.HasPrefix(m.Name, "__") {
		return Fragment{}
	}
	if clashesWithStream(m.Name, streamLabels) {
		return Fragment{}
	}
	runs, ok := verbatimRuns(m.Value)
	if !ok || len(runs) == 0 {
		return Fragment{}
	}
	var conds []string
	var args []any
	for _, r := range runs {
		conds = append(conds, "line LIKE ?")
		args = append(args, "%"+escapedLike(r)+"%")
	}
	lineCond := strings.Join(conds, " AND ")
	if len(runs) > 1 {
		lineCond = "(" + lineCond + ")"
	}
	return Fragment{
		SQL:  "(" + lineCond + " OR mapContainsKey(structured_metadata, ?) OR line_ascii_escape)",
		Args: append(args, m.Name),
	}
}

// verbatimRuns returns the maximal runs of at least minRunLen characters that JSON and
// logfmt encoders write unchanged. ok is false for a non-ASCII value (an encoder may
// escape any of its characters).
func verbatimRuns(v string) (runs []string, ok bool) {
	start := -1
	for i := 0; i <= len(v); i++ {
		if i < len(v) && v[i] >= 0x80 {
			return nil, false
		}
		if i < len(v) && isVerbatim(v[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 && i-start >= minRunLen {
			runs = append(runs, v[start:i])
		}
		start = -1
	}
	return runs, true
}

// isVerbatim reports whether JSON and logfmt encoders write c unchanged. Excluded:
// quotes, backslash, slash (JSON may escape it), <, > and & (Go escapes them), the
// apostrophe, the backtick and control characters.
func isVerbatim(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte(" _.,:;=+*#@!?()[]{}|~^$%-", c) >= 0
}
