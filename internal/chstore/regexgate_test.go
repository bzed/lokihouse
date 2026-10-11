package chstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexExact_Table(t *testing.T) {
	for re, want := range map[string]bool{
		"error":            true,
		"err(or)?|warn":     true,
		"^GET /api/[a-z]+":  true,
		`\d{3} ms$`:        true,
		"(?i)error":        false, // Go folds k→K (Kelvin), s→ſ
		"e.r":              false,
		"[^a]":             false,
		"größe":            false,
		`\bword\b`:         false,
		"(?m)^x":           false,
		"[":                false, // parse error
	} {
		require.Equal(t, want, RegexExact(re), re)
	}
}
