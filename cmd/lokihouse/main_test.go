package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun_Version(t *testing.T) {
	var out, errb bytes.Buffer
	require.Equal(t, 0, run([]string{"-version"}, &out, &errb))
	require.Contains(t, out.String(), "lokihouse, version")
}

func TestRun_UsageError(t *testing.T) {
	var out, errb bytes.Buffer
	require.Equal(t, 2, run([]string{"-no-such-flag"}, &out, &errb))
	require.Contains(t, errb.String(), "no-such-flag")
}

func TestRun_ConfigError(t *testing.T) {
	var out, errb bytes.Buffer
	p := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(p, []byte("nonsense_key: 1\n"), 0o600))
	require.Equal(t, 1, run([]string{"-config.file", p}, &out, &errb))
	require.Contains(t, errb.String(), "nonsense_key")
}
