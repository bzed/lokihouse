//go:build oracle

package oracle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// StartLokihouse builds bin/lokihouse, starts it against a fresh ClickHouse
// database with the extra YAML appended to the generated config, and waits
// for /ready.
func StartLokihouse(t *testing.T, extra string) string {
	return StartLokihouseLimits(t, extra, "")
}

// StartLokihouseLimits is StartLokihouse with limits (indented YAML lines)
// appended inside limits_config.
func StartLokihouseLimits(t *testing.T, extra, limits string) string {
	url, _ := startLokihouse(t, chAddr(), chtest.Topology(), extra, limits)
	return url
}

// StartLokihouseOn is StartLokihouse against the ClickHouse server at addr
// with topology topo instead of the test server.
func StartLokihouseOn(t *testing.T, addr []string, topo chstore.Topology, extra string) string {
	url, _ := StartLokihouseDBOn(t, addr, topo, extra)
	return url
}

// StartLokihouseDBOn is StartLokihouseOn that also returns the name of the
// database the instance runs on.
func StartLokihouseDBOn(t *testing.T, addr []string, topo chstore.Topology, extra string) (url, db string) {
	return startLokihouse(t, addr, topo, extra, "")
}

func startLokihouse(t *testing.T, addr []string, topo chstore.Topology, extra, limits string) (string, string) {
	// Build the binary from the repository root (go run sets cwd to the package dir).
	repo, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(filepath.Join(repo, "bin"), 0o755)
	binPath := filepath.Join(repo, "bin", "lokihouse")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/lokihouse")
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building lokihouse: %v\n%s", err, out)
	}

	// Fresh database.
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_oracle_" + hex.EncodeToString(b)
	ctx := context.Background()
	opts := chtest.Options("default")
	opts.Addr = addr
	opts.Settings = topo.Settings()
	admin, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := chstore.Migrate(ctx, admin, db, topo); err != nil {
		_ = admin.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		_ = chstore.DropDatabase(ctx, admin, db, topo)
		_ = admin.Close()
	})

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "lokihouse.yaml")
	port := freePort(t)
	// Push-down mode for the oracle run; default "on" (Tier A where eligible).
	pushdown := os.Getenv("LOKIHOUSE_ORACLE_PUSHDOWN")
	if pushdown == "" {
		pushdown = "on"
	}
	cfg := fmt.Sprintf(`auth_enabled: true
server: {http_listen_port: %d}
clickhouse:
  addr: %v
  database: %q
  username: default
  cluster: %q
  replicated: %t
query:
  pushdown: %q
  querier: {multi_tenant_queries_enabled: true}
limits_config:
  max_query_series: 100000
  volume_enabled: true
`, port, addr, db, topo.Cluster, topo.Replicated, pushdown) + limits + extra
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	run := exec.Command(binPath, "-config.file", cfgPath)
	logf, _ := os.Create(filepath.Join(dir, "lokihouse.log"))
	run.Stdout, run.Stderr = logf, logf
	if err := run.Start(); err != nil {
		t.Fatalf("start lokihouse: %v", err)
	}
	t.Cleanup(func() {
		_ = run.Process.Kill()
		_, _ = run.Process.Wait()
	})

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	if err := waitForHTTP(url+"/ready", 30*time.Second); err != nil {
		readLog(t, filepath.Join(dir, "lokihouse.log"))
		t.Fatalf("lokihouse /ready: %v", err)
	}
	return url, db
}

func chAddr() []string {
	if v := os.Getenv("LOKIHOUSE_TEST_CH_ADDR"); v != "" {
		return []string{v}
	}
	return []string{"localhost:9000"}
}

// repoRoot walks up from the current directory to the directory containing go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}

var _ = http.StatusOK
