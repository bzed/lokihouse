package chstore

import (
	"bytes"
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/regexp"
	"github.com/grafana/regexp/syntax"
)

// literalLeaf stands in for Loki's contains filter. It mirrors what
// parseRegexpFilter (pkg/logql/log/filter.go) hands to RegexSimplifier, but
// records the literal instead of filtering with it.
type literalLeaf struct {
	lit  []byte
	fold bool
}

func newLiteralLeaf(match []byte, caseInsensitive bool) log.MatcherFilterer {
	return literalLeaf{lit: match, fold: caseInsensitive}
}

func (l literalLeaf) Matches(test log.Checker) bool { return test.Test(l.lit, l.fold, false) }

func (l literalLeaf) Filter(line []byte) bool { return bytes.Contains(line, l.lit) }

func (l literalLeaf) ToStage() log.Stage {
	return log.NewStageFunc(nil, log.StageHints{}, func(_ int64, line []byte, lbs *log.LabelsBuilder) ([]byte, bool) {
		return line, l.Filter(line)
	})
}

// literalCollector records every literal Matches visits. Test returns false so
// that orFilter.Matches does not short-circuit and visits every leg.
type literalCollector struct {
	lits [][]byte
	fold bool
}

func (c *literalCollector) Test(line []byte, caseInsensitive, _ bool) bool {
	c.lits = append(c.lits, line)
	c.fold = c.fold || caseInsensitive
	return false
}

func (c *literalCollector) TestRegex(*regexp.Regexp) bool { return true }

// simplifiedRegexSQL returns SQL over the column line that accepts exactly the
// lines Loki's |~ re (match) or !~ re (!match) filter accepts, when Loki reduces
// re to literal substrings. ok is false when Loki would run a real regex.
func simplifiedRegexSQL(re string, match bool) (Fragment, bool) {
	reg, err := syntax.Parse(re, syntax.Perl)
	if err != nil {
		return Fragment{}, false
	}
	reg = reg.Simplify()
	f, ok := log.NewRegexSimplifier(newLiteralLeaf, newLiteralLeaf).Simplify(reg, false)
	if !ok {
		return Fragment{}, false
	}
	var c literalCollector
	if f.Matches(&c) || c.fold || len(c.lits) == 0 {
		return Fragment{}, false
	}
	sqls := make([]string, 0, len(c.lits))
	args := make([]any, 0, len(c.lits))
	for _, lit := range c.lits {
		frag := containsFragment(string(lit))
		sqls = append(sqls, frag.SQL)
		args = append(args, frag.Args...)
	}
	sql := "(" + strings.Join(sqls, " OR ") + ")"
	if !match {
		sql = "NOT " + sql
	}
	return Fragment{sql, args}, true
}
