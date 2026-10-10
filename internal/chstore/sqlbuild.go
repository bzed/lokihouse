package chstore

import (
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

type Fragment struct {
	SQL  string
	Args []any
}

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
			return Fragment{"position(line, ?) > 0", []any{f.Match}}
		}
		var sqls []string
		var args []any
		for _, a := range alts {
			sqls = append(sqls, "position(line, ?) > 0")
			args = append(args, a.Match)
		}
		return Fragment{"(" + strings.Join(sqls, " OR ") + ")", args}
	case log.LineMatchNotEqual:
		var parts []Fragment
		for _, a := range alts {
			parts = append(parts, Fragment{"position(line, ?) = 0", []any{a.Match}})
		}
		return and(parts)
	default:
		return Fragment{}
	}
}
