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

// Go's (?i) folds k and s to U+212A and U+017F, which ILIKE does not: a run with
// k or s must not be pushed. Non-ASCII literals are not pushed either.
func TestLeadingLineFilterSQL_FoldCase(t *testing.T) {
	for _, c := range []struct {
		q    string
		sql  string
		args []any
	}{
		{`{a="b"} |~ "(?i)error"`, "line ILIKE ?", []any{"%error%"}},
		{`{a="b"} |~ "(?i)connection refused"`, "line ILIKE ?", []any{"%connection%"}},
		{`{a="b"} |~ "(?i)connection failed"`, "line ILIKE ? AND line ILIKE ?", []any{"%connection%", "%failed%"}},
		{`{a="b"} |~ "(?i)disk"`, "", nil},
		{`{a="b"} |~ "(?i)fehlerö"`, "", nil},
		{`{a="b"} |~ "(?i)err"`, "", nil},
		{`{a="b"} |~ "(?i)(error|fatal)"`, "(line ILIKE ? OR line ILIKE ?)", []any{"%error%", "%fatal%"}},
		{`{a="b"} |~ "(?i)(error|disk)"`, "", nil},
	} {
		sel, err := syntax.ParseLogSelector(c.q, true)
		require.NoError(t, err)
		f := LeadingLineFilterSQL(sel)
		require.Equal(t, c.sql, f.SQL, c.q)
		require.Equal(t, c.args, f.Args, c.q)
	}
}

func TestStreamMatcherSQL(t *testing.T) {
	ms, err := syntax.ParseMatchers(`{app="a", env!="prod", pod=~"x.*", ns!~"kube-.*"}`, true)
	require.NoError(t, err)
	f, goSide := StreamMatcherSQL(ms)
	require.Equal(t, "labels[?] = ? AND labels[?] != ?", f.SQL)
	require.Equal(t, []any{"app", "a", "env", "prod"}, f.Args)
	require.Len(t, goSide, 2)

	// Exact regexes are also evaluated in SQL; Go keeps re-checking all of them.
	ms, err = syntax.ParseMatchers(`{a=~"x|y", b!~"z|w", c=~"q.*"}`, true)
	require.NoError(t, err)
	f, goSide = StreamMatcherSQL(ms)
	require.Equal(t, "match(labels[?], ?) AND NOT match(labels[?], ?)", f.SQL)
	require.Equal(t, []any{"a", "^(?:x|y)$", "b", "^(?:z|w)$"}, f.Args)
	require.Len(t, goSide, 3)
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

func TestExactFilterSQL(t *testing.T) {
	exact := map[string]Fragment{
		`{a="b"}`:               {},
		`{a="b"} |= "x" or "y"`: {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%x%", "%y%"}},
		// Loki drops an empty operand of an or chain instead of keeping every line.
		`{a="b"} |= "" or "x"`:        {SQL: "(line LIKE ?)", Args: []any{"%x%"}},
		`{a="b"} |= "x" or "" or "y"`: {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%x%", "%y%"}},
		`{a="b"} |= "" or ""`:         {},
		`{a="b"} != "x"`:              {SQL: "position(line, ?) = 0", Args: []any{"x"}},
		`{a="b"} |~ "ab(c|d)"`:        {SQL: "line LIKE ? AND match(line, ?)", Args: []any{"%ab%", "ab(c|d)"}},
		`{a="b"} !~ "ab"`:             {SQL: "NOT match(line, ?)", Args: []any{"ab"}},
		`{a="b"} |= "50%_\\" |= "y"`:  {SQL: "(line LIKE ?) AND (line LIKE ?)", Args: []any{`%50\%\_\\%`, "%y%"}},
	}
	for q, want := range exact {
		e, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		got, names, ok := ExactFilterSQL(e)
		require.True(t, ok, q)
		require.Empty(t, names, q)
		require.Equal(t, want, got, q)
	}
	withLabels := []struct {
		query string
		want  Fragment
		names []string
	}{
		{`{a="b"} |= "x" | level="warn"`, Fragment{SQL: "(line LIKE ?) AND structured_metadata[?] = ?", Args: []any{"%x%", "level", "warn"}}, []string{"level"}},
		{`{a="b"} | level="warn" or level="info"`, Fragment{SQL: "(structured_metadata[?] = ? OR structured_metadata[?] = ?)", Args: []any{"level", "warn", "level", "info"}}, []string{"level"}},
		{`{a="b"} | level_extracted="warn"`, Fragment{SQL: "structured_metadata[?] = ?", Args: []any{"level_extracted", "warn"}}, []string{"level_extracted", "level"}},
		{`{a="b"} | level="warn" and code="7"`, Fragment{SQL: "(structured_metadata[?] = ? AND structured_metadata[?] = ?)", Args: []any{"level", "warn", "code", "7"}}, []string{"level", "code"}},
	}
	for _, tc := range withLabels {
		e, err := syntax.ParseLogSelector(tc.query, true)
		require.NoError(t, err)
		got, names, ok := ExactFilterSQL(e)
		require.True(t, ok, tc.query)
		require.Equal(t, tc.want, got, tc.query)
		require.Equal(t, tc.names, names, tc.query)
	}
	for _, q := range []string{
		`{a="b"} | json`,
		`{a="b"} |= ip("10.0.0.0/8")`,
		`{a="b"} |= "x" or ip("10.0.0.0/8")`,
		`{a="b"} |~ "x" or "y"`,
		`{a="b"} |~ "e.r"`,
		`{a="b"} !~ "e.r"`,
		`{a="b"} |> "<_> x"`,
		`{a="b"} | level=~"w.*"`,
		`{a="b"} | level!="x"`,
		`{a="b"} | level=""`,
		`{a="b"} | level="x" or level=~"y.*"`,
		`{a="b"} | __error__="x"`,
		`{a="b"} | json | level="x"`,
		`{a="b"} | label_format level="x" | level="x"`,
		`{a="b"} | level="x" | json`,
	} {
		e, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		_, _, ok := ExactFilterSQL(e)
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
