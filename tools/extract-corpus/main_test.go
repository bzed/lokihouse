package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractCorpus_Helpers(t *testing.T) {
	// Create a temporary directory with fake Loki files
	dir := t.TempDir()

	// Create a fake parser_test.go with LogQL queries
	parserDir := filepath.Join(dir, "pkg", "logql", "syntax")
	require.NoError(t, os.MkdirAll(parserDir, 0o755))
	parserContent := `package syntax_test

import "testing"

func TestParse(t *testing.T) {
	// Test cases with LogQL queries
	queries := []string{
		"{app=\"a\"} |= \"x\"",
		"count_over_time({app=\"a\"}[5m])",
	}
	for _, q := range queries {
		_ = q
	}
}
`
	require.NoError(t, os.WriteFile(filepath.Join(parserDir, "parser_test.go"), []byte(parserContent), 0o644))

	// Create a fake engine_test.go
	engineDir := filepath.Join(dir, "pkg", "logql")
	require.NoError(t, os.MkdirAll(engineDir, 0o755))
	engineContent := `package engine_test

import "testing"

func TestEngine(t *testing.T) {
	query := "rate({app=\"b\"}[1m])"
	_ = query
}
`
	require.NoError(t, os.WriteFile(filepath.Join(engineDir, "engine_test.go"), []byte(engineContent), 0o644))

	// Create a fake query doc
	docDir := filepath.Join(dir, "docs", "sources", "query")
	require.NoError(t, os.MkdirAll(docDir, 0o755))
	docContent := "# Query Examples\n\n```logql\nrate({app=\"b\"}[1m])\n```\n"
	require.NoError(t, os.WriteFile(filepath.Join(docDir, "q.md"), []byte(docContent), 0o644))

	// Test that the helper functions don't panic
	require.NotPanics(t, func() {
		// Test goStrings
		strings := goStrings(filepath.Join(dir, "pkg", "logql", "syntax", "parser_test.go"))
		require.NotEmpty(t, strings)

		// Test mdLogQL
		mdStrings := mdLogQL(filepath.Join(dir, "docs", "sources", "query", "q.md"))
		require.NotEmpty(t, mdStrings)
	})
}
