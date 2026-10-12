package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestMaxRangeFor(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected time.Duration
	}{
		{
			name:     "empty query",
			query:    "",
			expected: 0,
		},
		{
			name:     "invalid query",
			query:    "invalid",
			expected: 0,
		},
		{
			name:     "instant query",
			query:    `{app="a"}`,
			expected: 0,
		},
		{
			name:     "range query with 1m",
			query:    `sum(count_over_time({app="a"}[1m]))`,
			expected: time.Minute,
		},
		{
			name:     "range query with 5m",
			query:    `sum(count_over_time({app="a"}[5m]))`,
			expected: 5 * time.Minute,
		},
		{
			name:     "range query with 1h",
			query:    `sum(count_over_time({app="a"}[1h]))`,
			expected: time.Hour,
		},
		{
			name:     "multiple ranges - takes max",
			query:    `sum(count_over_time({app="a"}[1m])) or sum(count_over_time({app="b"}[5m]))`,
			expected: 5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maxRangeFor(tt.query)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalise(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
		valid    bool
	}{
		{
			name:     "simple selector",
			query:    `{app="a"}`,
			expected: `{app=~".+"}`,
			valid:    true,
		},
		{
			name:     "selector with multiple matchers",
			query:    `{app="a", env="prod"}`,
			expected: `{app=~".+"}`,
			valid:    true,
		},
		{
			name:     "metric query",
			query:    `sum(count_over_time({app="a"}[1m]))`,
			expected: `range:sum(count_over_time({app=~".+"}[1m]))`,
			valid:    true,
		},
		{
			name:     "invalid query",
			query:    "invalid",
			expected: "",
			valid:    false,
		},
		{
			name:     "query with newline",
			query:    "{app=\"a\"}\n{app=\"b\"}",
			expected: "",
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := normalise(tt.query)
			require.Equal(t, tt.valid, ok)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGoStrings(t *testing.T) {
	// This is a basic test - the function reads files from disk
	// We can't easily test it without creating temp files
	// But we can at least verify it doesn't panic
	result := goStrings("/nonexistent/file.go")
	require.Nil(t, result)
}

func TestGoStrings_File(t *testing.T) {
	p := filepath.Join(t.TempDir(), "q_test.go")
	src := "package x\nvar a = `{app=\"a\"}`\nvar b = `{app=\"a\", env=\"p\"}`\nvar c = `no braces`\n"
	require.NoError(t, os.WriteFile(p, []byte(src), 0o600))
	got := goStrings(p)
	require.ElementsMatch(t, []string{`{app="a"}`, `{app="a", env="p"}`}, got)
}

func TestMdLogQL(t *testing.T) {
	// This reads files from disk - can't easily test without temp files
	result := mdLogQL("/nonexistent/file.md")
	require.Nil(t, result)
}

func TestMdLogQL_Fences(t *testing.T) {
	p := filepath.Join(t.TempDir(), "q.md")
	src := "text\n```logql\n{app=\"a\"}\n```\nmore\n```go\nfmt.Println(\"ignored\")\n```\n```logql\nrate({app=\"b\"}[1m])\n```\n"
	require.NoError(t, os.WriteFile(p, []byte(src), 0o600))
	got := mdLogQL(p)
	require.ElementsMatch(t, []string{`{app="a"}`, `rate({app="b"}[1m])`}, got)
}

func TestParseExpr(t *testing.T) {
	// Test that syntax.ParseExpr works as expected
	e, err := syntax.ParseExpr(`{app="a"}`)
	require.NoError(t, err)
	require.NotNil(t, e)
}
