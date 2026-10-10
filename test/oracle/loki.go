//go:build oracle

package oracle

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const lokiCommit = "fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1"

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// lokiBin returns the absolute path to the built loki binary, building it from
// the pinned commit once.
func lokiBin(t *testing.T) string {
	bin := envOr("LOKIHOUSE_LOKI_BIN", filepath.Join(".cache", "loki-bin", "loki"))
	if ab, err := filepath.Abs(bin); err == nil {
		bin = ab
	}
	if fileExists(bin) {
		return bin
	}
	src := envOr("LOKIHOUSE_LOKI_SRC", filepath.Join(".cache", "loki-src"))
	if ab, err := filepath.Abs(src); err == nil {
		src = ab
	}
	if !dirExists(src) {
		cloneLoki(t, src)
	}
	_ = os.MkdirAll(filepath.Dir(bin), 0o755)
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/loki")
	cmd.Dir = src
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building loki from %s: %v\n%s", src, err, out)
	}
	return bin
}

func cloneLoki(t *testing.T, dir string) {
	_ = os.MkdirAll(filepath.Dir(dir), 0o755)
	cmd := exec.Command("git", "clone", "https://github.com/grafana/loki", dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cloning loki: %v\n%s", err, out)
	}
	cmd = exec.Command("git", "checkout", lokiCommit)
	cmd.Dir = dir
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("checking out %s: %v\n%s", lokiCommit, err, out)
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// StartLoki builds (once) and starts Loki on free ports with the oracle config.
func StartLoki(t *testing.T) string {
	bin := lokiBin(t)
	dir := t.TempDir()
	httpPort, grpcPort := freePort(t), freePort(t)
	cfg := strings.ReplaceAll(lokiConfigTemplate, "${DIR}", dir)
	cfgPath := filepath.Join(dir, "loki-config.yaml")
	require := func(err error, msg string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
	}
	require(os.WriteFile(cfgPath, []byte(cfg), 0o600), "write loki config")

	cmd := exec.Command(bin,
		"-config.file", cfgPath,
		"-config.expand-env=true",
		fmt.Sprintf("-server.http-listen-port=%d", httpPort),
		fmt.Sprintf("-server.grpc-listen-port=%d", grpcPort),
	)
	// cwd must be writable (Loki creates ring/positions files); use the temp dir.
	cmd.Dir = dir
	logf, _ := os.Create(filepath.Join(dir, "loki.log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	require(cmd.Start(), "start loki")
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	if err := waitForHTTP(url+"/ready", 120*time.Second); err != nil {
		readLog(t, filepath.Join(dir, "loki.log"))
		t.Fatalf("loki /ready: %v", err)
	}
	return url
}

func readLog(t *testing.T, p string) {
	b, err := os.ReadFile(p)
	if err == nil {
		t.Logf("loki log tail:\n%s", tail(string(b), 3000))
	}
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func waitForHTTP(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

const lokiConfigTemplate = `
auth_enabled: true
server:
  http_listen_port: 0
  grpc_listen_port: 0

common:
  path_prefix: ${DIR}
  replication_factor: 1
  ring:
    kvstore: {store: inmemory}
  storage:
    filesystem: {chunks_directory: ${DIR}/chunks, rules_directory: ${DIR}/rules}
schema_config:
  configs:
    - from: 2020-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index: {prefix: index_, period: 24h}
query_range:
  cache_results: false
  parallelise_shardable_queries: false
limits_config:
  split_queries_by_interval: 0
  # Keep both stores' series limits identical (100k >> any fixture cardinality).
  # Loki's JoinSampleVector treats 0 as a hard zero cap, not "unlimited" (a
  # Loki quirk), so a large value is used instead of 0.
  max_query_series: 100000
analytics:
  reporting_enabled: false
`
