package chstore

import (
	"strings"

	regexpSyntax "regexp/syntax"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// Fragment is a SQL query fragment with its arguments.
type Fragment struct {
	SQL  string
	Args []any
}

// Empty reports whether the fragment has no SQL.
func (f Fragment) Empty() bool { return f.SQL == "" }

func and(parts []Fragment) Fragment {
	var out Fragment
	var sqls []string
	for _, p := range parts {
		if p.Empty() {
			continue
		}
		sqls = append(sqls, p.SQL)
		out.Args = append(out.Args, p.Args...)
	}
	out.SQL = strings.Join(sqls, " AND ")
	return out
}

// StreamMatcherSQL turns equality matchers into SQL over streams.labels; regex matchers stay in Go.
func StreamMatcherSQL(ms []*labels.Matcher) (Fragment, []*labels.Matcher) {
	var parts []Fragment
	var goSide []*labels.Matcher
	for _, m := range ms {
		switch m.Type {
		case labels.MatchEqual:
			parts = append(parts, Fragment{"labels[?] = ?", []any{m.Name, m.Value}})
		case labels.MatchNotEqual:
			parts = append(parts, Fragment{"labels[?] != ?", []any{m.Name, m.Value}})
		default:
			goSide = append(goSide, m)
		}
	}
	return and(parts), goSide
}

// LeadingLineFilterSQL returns a necessary SQL condition for the literal line filters that
// precede every non-filter stage. Loki's pipeline re-applies all filters exactly.
func LeadingLineFilterSQL(expr syntax.LogSelectorExpr) Fragment {
	p, ok := expr.(*syntax.PipelineExpr)
	if !ok {
		return Fragment{}
	}
	var parts []Fragment
	for _, st := range p.MultiStages {
		lf, ok := st.(*syntax.LineFilterExpr)
		if !ok {
			break
		}
		// Walk Left to the root so parts end up in source order.
		var chain []*syntax.LineFilterExpr
		for cur := lf; cur != nil; cur = cur.Left {
			chain = append(chain, cur)
		}
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		for _, cur := range chain {
			parts = append(parts, lineFilterSQL(cur))
		}
	}
	return and(parts)
}

// lineFilterSQL handles one LineFilterExpr node including its Or chain (not its Left chain).
func lineFilterSQL(f *syntax.LineFilterExpr) Fragment {
	if f.Op != "" { // ip() and other functions: not pushed
		return Fragment{}
	}
	var alts []*syntax.LineFilterExpr
	for o := f; o != nil; o = o.Or {
		if o.Op != "" {
			return Fragment{}
		}
		alts = append(alts, o)
	}
	switch f.Ty {
	case log.LineMatchEqual:
		if len(alts) == 1 {
			return containsFragment(f.Match)
		}
		return containsAny(alts)
	case log.LineMatchNotEqual:
		var parts []Fragment
		for _, a := range alts {
			parts = append(parts, Fragment{"position(line, ?) = 0", []any{a.Match}})
		}
		return and(parts)
	case log.LineMatchRegexp:
		if len(alts) > 1 {
			return Fragment{}
		}
		return regexNecessary(f.Match)
	case log.LineMatchPattern:
		if len(alts) > 1 {
			return Fragment{}
		}
		return patternNecessary(f.Match)
	default:
		return Fragment{}
	}
}

// regexNecessary returns a necessary SQL condition for a |~ regex: every line the
// regex matches must satisfy it. It is NOT exact (Loki re-applies the real filter).
func regexNecessary(re string) Fragment {
	n, err := regexpSyntax.Parse(re, regexpSyntax.Perl)
	if err != nil {
		return Fragment{}
	}
	if hasFoldCase(n) {
		return Fragment{}
	}
	n = unwrap(n)
	if n.Op == regexpSyntax.OpLiteral {
		return litFragment(n.Rune)
	}
	if n.Op == regexpSyntax.OpConcat {
		var parts []Fragment
		for _, c := range n.Sub {
			c = unwrap(c)
			if c.Op != regexpSyntax.OpLiteral {
				continue
			}
			parts = append(parts, litFragment(c.Rune))
		}
		return and(parts)
	}
	if n.Op == regexpSyntax.OpAlternate {
		var sqls []string
		var args []any
		for _, c := range n.Sub {
			c = unwrap(c)
			if c.Op != regexpSyntax.OpLiteral {
				return Fragment{}
			}
			p := litFragment(c.Rune)
			if p.Empty() {
				return Fragment{}
			}
			sqls = append(sqls, p.SQL)
			args = append(args, p.Args...)
		}
		return Fragment{"(" + strings.Join(sqls, " OR ") + ")", args}
	}
	return Fragment{}
}

// patternNecessary returns a necessary SQL condition for a |> pattern: every line the
// pattern matches must satisfy it. It is NOT exact (Loki re-applies the real filter).
func patternNecessary(p string) Fragment {
	literals := extractPatternLiterals(p)
	if len(literals) == 0 {
		return Fragment{}
	}
	var likeParts []string
	for _, l := range literals {
		likeParts = append(likeParts, escapedLike(l))
	}
	like := "%" + strings.Join(likeParts, "%") + "%"
	return Fragment{"line LIKE ?", []any{like}}
}

// extractPatternLiterals splits a |> pattern into its non-capture literal pieces,
// dropping the <...> captures and anything inside them.
func extractPatternLiterals(p string) []string {
	var out []string
	var cur strings.Builder
	inCap := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		if !inCap && c == '<' {
			inCap = true
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			continue
		}
		if inCap {
			if c == '>' {
				inCap = false
			}
			continue
		}
		cur.WriteByte(c)
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// escapedLike escapes the LIKE wildcards in a literal piece.
func escapedLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// litFragment turns a literal rune run into a substring condition, skipping runs
// shorter than 2 bytes (they are too weak to be a useful pre-filter).
func litFragment(r []rune) Fragment {
	if len(r) < 2 {
		return Fragment{}
	}
	return containsFragment(string(r))
}

// containsFragment is true for lines containing s. It is LIKE '%s%' rather than
// position(line, s) > 0, which selects the same lines, because ClickHouse can
// answer LIKE from the line_text index and cannot answer position() from it.
func containsFragment(s string) Fragment {
	return Fragment{"line LIKE ?", []any{"%" + escapedLike(s) + "%"}}
}

// containsAny is true for lines containing any of the alternatives' matches.
func containsAny(alts []*syntax.LineFilterExpr) Fragment {
	sqls := make([]string, 0, len(alts))
	args := make([]any, 0, len(alts))
	for _, a := range alts {
		f := containsFragment(a.Match)
		sqls = append(sqls, f.SQL)
		args = append(args, f.Args...)
	}
	return Fragment{"(" + strings.Join(sqls, " OR ") + ")", args}
}

// hasFoldCase reports whether any node in the regexp tree carries the FoldCase flag.
func hasFoldCase(n *regexpSyntax.Regexp) bool {
	if n == nil {
		return false
	}
	if n.Flags&regexpSyntax.FoldCase != 0 {
		return true
	}
	for _, s := range n.Sub {
		if hasFoldCase(s) {
			return true
		}
	}
	return false
}

// unwrap peels OpCapture and OpEmptyMatch wrappers off a regexp node.
func unwrap(n *regexpSyntax.Regexp) *regexpSyntax.Regexp {
	for n != nil && (n.Op == regexpSyntax.OpCapture || n.Op == regexpSyntax.OpEmptyMatch) {
		if len(n.Sub) == 1 {
			n = n.Sub[0]
		} else {
			return n
		}
	}
	return n
}

// ExactLineFilterSQL returns SQL accepting exactly the lines the pipeline's
// filters accept, or ok=false if the pipeline has any stage other than line
// filters that are literal (|=, !=, with or-chains) or regex passing RegexExact
// (|~, !~).
func ExactLineFilterSQL(expr syntax.LogSelectorExpr) (Fragment, bool) {
	p, ok := expr.(*syntax.PipelineExpr)
	if !ok {
		return Fragment{}, true // no pipeline: no line filter
	}
	var parts []Fragment
	for _, st := range p.MultiStages {
		lf, ok := st.(*syntax.LineFilterExpr)
		if !ok {
			return Fragment{}, false // non-line-filter stage
		}
		// Walk Left to the root so parts end up in source order.
		var chain []*syntax.LineFilterExpr
		for cur := lf; cur != nil; cur = cur.Left {
			chain = append(chain, cur)
		}
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		for _, cur := range chain {
			f, ok := exactLineFilterSQL(cur)
			if !ok {
				return Fragment{}, false
			}
			parts = append(parts, f)
		}
	}
	return and(parts), true
}

// exactLineFilterSQL handles one LineFilterExpr node including its Or chain.
func exactLineFilterSQL(f *syntax.LineFilterExpr) (Fragment, bool) {
	if f.Op != "" { // ip() and other functions: not exact
		return Fragment{}, false
	}
	var alts []*syntax.LineFilterExpr
	for o := f; o != nil; o = o.Or {
		if o.Op != "" {
			return Fragment{}, false
		}
		alts = append(alts, o)
	}
	switch f.Ty {
	case log.LineMatchEqual:
		return containsAny(alts), true
	case log.LineMatchNotEqual:
		var parts []Fragment
		for _, a := range alts {
			parts = append(parts, Fragment{"position(line, ?) = 0", []any{a.Match}})
		}
		return and(parts), true
	case log.LineMatchRegexp:
		if len(alts) > 1 {
			return Fragment{}, false
		}
		if !RegexExact(f.Match) {
			return Fragment{}, false
		}
		// The necessary literals let the text index skip granules before match() runs.
		return and([]Fragment{regexNecessary(f.Match), {"match(line, ?)", []any{f.Match}}}), true
	case log.LineMatchNotRegexp:
		if len(alts) > 1 {
			return Fragment{}, false
		}
		if !RegexExact(f.Match) {
			return Fragment{}, false
		}
		return Fragment{"NOT match(line, ?)", []any{f.Match}}, true
	default:
		return Fragment{}, false
	}
}
