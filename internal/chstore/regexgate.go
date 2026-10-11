package chstore

import (
	regexpSyntax "regexp/syntax"
)

// RegexExact reports whether Go's regexp and ClickHouse's match() agree on every
// possible input for the pattern re, so it can be pushed down as an *exact*
// filter (match(line, re)) rather than merely a necessary one.
//
// The gate is conservative: it walks the unsimplified parse tree and allows only
// ASCII-safe constructs. ASCII-only atoms never match a byte ≥ 0x80 in either
// engine, whether the line is valid UTF-8 or not, so UTF-8 decoding differences
// cannot change the outcome.
func RegexExact(re string) bool {
	n, err := regexpSyntax.Parse(re, regexpSyntax.Perl)
	if err != nil {
		return false
	}
	return regexExactNode(n)
}

// allowedOps are the structure/anchor operators that are safe under ASCII-only leaves.
var allowedOps = map[regexpSyntax.Op]struct{}{
	regexpSyntax.OpConcat:     {},
	regexpSyntax.OpAlternate:  {},
	regexpSyntax.OpCapture:    {},
	regexpSyntax.OpStar:       {},
	regexpSyntax.OpPlus:       {},
	regexpSyntax.OpQuest:      {},
	regexpSyntax.OpRepeat:     {},
	regexpSyntax.OpEmptyMatch: {},
	regexpSyntax.OpBeginText:  {},
	regexpSyntax.OpEndText:    {},
}

func regexExactNode(n *regexpSyntax.Regexp) bool {
	if n == nil {
		return false
	}
	// Any case-folding anywhere changes Go vs ClickHouse matching (e.g. (?i)).
	if n.Flags&regexpSyntax.FoldCase != 0 {
		return false
	}
	switch n.Op {
	case regexpSyntax.OpLiteral:
		return isASCIIRune(n.Rune)
	case regexpSyntax.OpCharClass:
		// A negated class shows up as ranges reaching 0x10FFFF, which the bound
		// check below already rejects.
		for _, r := range n.Rune {
			if !isASCIIRange(r) {
				return false
			}
		}
		return true
	default:
		if _, ok := allowedOps[n.Op]; !ok {
			return false
		}
		for _, s := range n.Sub {
			if !regexExactNode(s) {
				return false
			}
		}
		return true
	}
}

func isASCIIRune(r []rune) bool {
	for _, c := range r {
		if c >= 0x80 {
			return false
		}
	}
	return true
}

func isASCIIRange(r rune) bool {
	return r < 0x80
}
