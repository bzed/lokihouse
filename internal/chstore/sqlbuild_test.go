package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestStreamMatcherSQL(t *testing.T) {
	ms, err := syntax.ParseMatchers(`{app="a", env!="prod", pod=~"x.*", ns!~"kube-.*"}`, true)
	require.NoError(t, err)
	f, goSide := StreamMatcherSQL(ms)
	require.Equal(t, "labels[?] = ? AND labels[?] != ?", f.SQL)
	require.Equal(t, []any{"app", "a", "env", "prod"}, f.Args)
	require.Len(t, goSide, 2)
}

func TestLeadingLineFilterSQL(t *testing.T) {
	cases := map[string]Fragment{
		`{a="b"}`:                      {},
		`{a="b"} |= "x"`:               {SQL: "position(line, ?) > 0", Args: []any{"x"}},
		`{a="b"} |= "x" != "y"`:        {SQL: "position(line, ?) > 0 AND position(line, ?) = 0", Args: []any{"x", "y"}},
		`{a="b"} |= "x" or "z"`:        {SQL: "(position(line, ?) > 0 OR position(line, ?) > 0)", Args: []any{"x", "z"}},
		`{a="b"} != "x" or "z"`:        {SQL: "position(line, ?) = 0 AND position(line, ?) = 0", Args: []any{"x", "z"}},
		`{a="b"} |~ "x"`:               {}, // regex never pushed in Phase 1
		`{a="b"} |= "x" | json |= "y"`: {SQL: "position(line, ?) > 0", Args: []any{"x"}},
		`{a="b"} | json |= "y"`:        {},
		`{a="b"} |= ip("10.0.0.0/8")`:  {},
		`{a="b"} |= "x" |~ "r" |= "y"`: {SQL: "position(line, ?) > 0 AND position(line, ?) > 0", Args: []any{"x", "y"}},
	}
	for q, want := range cases {
		t.Run(q, func(t *testing.T) {
			e, err := syntax.ParseLogSelector(q, true)
			require.NoError(t, err)
			require.Equal(t, want, LeadingLineFilterSQL(e))
		})
	}
}
