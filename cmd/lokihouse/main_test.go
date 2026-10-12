package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newFlagSet returns the lokihouse flag set so the drift tests can iterate
// over the registered flags.
func newFlagSet(stderr io.Writer) (*flag.FlagSet, *struct {
	configFile  string
	expandEnv   bool
	showVersion bool
}) {
	fs := flag.NewFlagSet("lokihouse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	f := &struct {
		configFile  string
		expandEnv   bool
		showVersion bool
	}{}
	fs.StringVar(&f.configFile, "config.file", "lokihouse.yaml", "configuration file to load")
	fs.BoolVar(&f.expandEnv, "config.expand-env", false, "expand ${VAR} and ${VAR:-default} in the configuration file")
	fs.BoolVar(&f.showVersion, "version", false, "print version information and exit")
	return fs, f
}

// TestManpageDocumentsEveryFlag ensures the manpage lists every registered flag.
func TestManpageDocumentsEveryFlag(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "man", "lokihouse.1.md"))
	require.NoError(t, err)
	fs, _ := newFlagSet(io.Discard)
	fs.VisitAll(func(f *flag.Flag) {
		require.Contains(t, string(b), "**-"+f.Name+"**", "flag -%s missing from docs/man/lokihouse.1.md", f.Name)
	})
}

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
