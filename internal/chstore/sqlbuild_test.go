package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestLeadingLineFilterSQL_RegexAndPattern(t *testing.T) {
	for q, want := range map[string]string{
		`{a="b"} |~ "error"`:              "line LIKE ?",
		`{a="b"} |~ "err(or)?: timeout"`:  "line LIKE ? AND line LIKE ?",
		`{a="b"} |~ "error|warn"`:         "(line LIKE ? OR line LIKE ?)",
		`{a="b"} |~ "(?i)error"`:          "",
		`{a="b"} |~ "e.r"`:                "",
		`{a="b"} !~ "error"`:              "",
		`{a="b"} |> "<_> GET <path> 200"`: "line LIKE ?",
		`{a="b"} |> "<_>"`:                "",
		`{a="b"} |= "x" or "y" |~ "z.*"`:  "(line LIKE ? OR line LIKE ?)",
	} {
		sel, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		require.Equal(t, want, LeadingLineFilterSQL(sel).SQL, q)
	}
}

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
		`{a="b"} |= "x"`:               {SQL: "line LIKE ?", Args: []any{"%x%"}},
		`{a="b"} |= "x" != "y"`:        {SQL: "line LIKE ? AND position(line, ?) = 0", Args: []any{"%x%", "y"}},
		`{a="b"} |= "x" or "z"`:        {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%x%", "%z%"}},
		`{a="b"} != "x" or "z"`:        {SQL: "position(line, ?) = 0 AND position(line, ?) = 0", Args: []any{"x", "z"}},
		`{a="b"} |~ "x"`:               {}, // regex never pushed in Phase 1
		`{a="b"} |= "x" | json |= "y"`: {SQL: "line LIKE ?", Args: []any{"%x%"}},
		`{a="b"} | json |= "y"`:        {},
		`{a="b"} |= ip("10.0.0.0/8")`:  {},
		`{a="b"} |= "x" |~ "r" |= "y"`: {SQL: "line LIKE ? AND line LIKE ?", Args: []any{"%x%", "%y%"}},
	}
	for q, want := range cases {
		t.Run(q, func(t *testing.T) {
			e, err := syntax.ParseLogSelector(q, true)
			require.NoError(t, err)
			require.Equal(t, want, LeadingLineFilterSQL(e))
		})
	}
}

func TestExactLineFilterSQL(t *testing.T) {
	exact := map[string]Fragment{
		`{a="b"}`:                    {},
		`{a="b"} |= "x" or "y"`:      {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%x%", "%y%"}},
		`{a="b"} != "x"`:             {SQL: "position(line, ?) = 0", Args: []any{"x"}},
		`{a="b"} |~ "ab(c|d)"`:       {SQL: "line LIKE ? AND match(line, ?)", Args: []any{"%ab%", "ab(c|d)"}},
		`{a="b"} !~ "ab"`:            {SQL: "NOT match(line, ?)", Args: []any{"ab"}},
		`{a="b"} |= "50%_\\" |= "y"`: {SQL: "(line LIKE ?) AND (line LIKE ?)", Args: []any{`%50\%\_\\%`, "%y%"}},
	}
	for q, want := range exact {
		e, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		got, ok := ExactLineFilterSQL(e)
		require.True(t, ok, q)
		require.Equal(t, want, got, q)
	}
	for _, q := range []string{
		`{a="b"} | json`,
		`{a="b"} |= ip("10.0.0.0/8")`,
		`{a="b"} |= "x" or ip("10.0.0.0/8")`,
		`{a="b"} |~ "x" or "y"`,
		`{a="b"} |~ "e.r"`,
		`{a="b"} !~ "e.r"`,
		`{a="b"} |> "<_> x"`,
	} {
		e, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		_, ok := ExactLineFilterSQL(e)
		require.False(t, ok, q)
	}
}

func TestLeadingLineFilterSQL_NotPushed(t *testing.T) {
	for _, q := range []string{
		`{a="b"} |~ "x" or "y"`,
		`{a="b"} |> "<_> x" or "<_> y"`,
		`{a="b"} |= "x" or ip("10.0.0.0/8")`,
	} {
		e, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		require.True(t, LeadingLineFilterSQL(e).Empty(), q)
	}
}
