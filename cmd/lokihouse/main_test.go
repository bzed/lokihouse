package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// newFlagSet returns the lokihouse flag set so the drift tests can iterate
// over the registered flags.
func newFlagSet(stderr io.Writer) (*flag.FlagSet, *struct {
	configFile  string
	expandEnv   bool
	showVersion bool
	migrate     bool
}) {
	fs := flag.NewFlagSet("lokihouse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	f := &struct {
		configFile  string
		expandEnv   bool
		showVersion bool
		migrate     bool
	}{}
	fs.StringVar(&f.configFile, "config.file", "lokihouse.yaml", "configuration file to load")
	fs.BoolVar(&f.expandEnv, "config.expand-env", false, "expand ${VAR} and ${VAR:-default} in the configuration file")
	fs.BoolVar(&f.showVersion, "version", false, "print version information and exit")
	fs.BoolVar(&f.migrate, "schema.migrate", false, "migrate the ClickHouse schema to the current version and exit")
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

func TestRun_SchemaMigrateFlag(t *testing.T) {
	conn, db := chtest.NewV1(t)
	o := chtest.Options("default")
	topo := chtest.Topology()
	p := filepath.Join(t.TempDir(), "lokihouse.yaml")
	y := fmt.Sprintf(`server: {http_listen_address: 127.0.0.1, http_listen_port: 0}
clickhouse:
  addr: [%q]
  database: %s
  username: %q
  password: %q
  cluster: %q
  replicated: %t
`, o.Addr[0], db, o.Auth.Username, o.Auth.Password, topo.Cluster, topo.Replicated)
	require.NoError(t, os.WriteFile(p, []byte(y), 0o600))

	var out, errb bytes.Buffer
	require.Equal(t, 1, run([]string{"-config.file", p}, &out, &errb), "a normal start refuses a version 1 database")
	require.Contains(t, errb.String(), "lokihouse -schema.migrate")

	errb.Reset()
	require.Equal(t, 0, run([]string{"-config.file", p, "-schema.migrate"}, &out, &errb), errb.String())
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count() FROM system.columns WHERE database = ? AND table = ? AND name = 'retention_days'", db, topo.Local("logs")).Scan(&n))
	require.Zero(t, n)
}

func TestRun_SchemaMigrateError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "lokihouse.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse: {addr: [\"127.0.0.1:1\"], dial_timeout: 200ms}\n"), 0o600))
	var out, errb bytes.Buffer
	require.Equal(t, 1, run([]string{"-config.file", p, "-schema.migrate"}, &out, &errb))
}
