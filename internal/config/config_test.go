package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
)

func TestLoad_DefaultsAndOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
auth_enabled: true
server: {http_listen_port: 3101}
clickhouse: {addr: ["ch:9000"], database: logs}
limits_config: {max_line_size: 1024}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.True(t, c.AuthEnabled)
	require.Equal(t, 3101, c.Server.HTTPListenPort)
	require.Equal(t, []string{"ch:9000"}, c.ClickHouse.Addr)
	require.EqualValues(t, 1024, c.LimitsConfig.MaxLineSize)
	require.Equal(t, 7*24*time.Hour, time.Duration(c.LimitsConfig.RejectOldSamplesMaxAge)) // Loki default kept
	require.Equal(t, chstore.Topology{}, c.Topology())                                     // single node by default
}

func TestLoad_Compactor(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
compactor: {delete_request_cancel_period: 2h}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, 2*time.Hour, c.Compactor.DeleteRequestCancelPeriod)
	require.Equal(t, 10*time.Minute, c.Compactor.CompactionInterval) // default kept
	require.Equal(t, 24*time.Hour, c.Compactor.DeleteMaxInterval)    // default kept
}

func TestLoad_Cluster(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse:
  addr: ["ch1:9000", "ch3:9000"]
  cluster: lokihouse
  settings: {insert_quorum: 2, max_execution_time: 60}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, chstore.Topology{Cluster: "lokihouse", Replicated: true}, c.Topology())
	s := c.CHSettings()
	require.EqualValues(t, 2, s["insert_quorum"]) // explicit setting wins over the topology default
	require.EqualValues(t, 60, s["max_execution_time"])
	require.EqualValues(t, 1, s["distributed_foreground_insert"])
}

func TestLoad_RetentionStreamMatchersPopulated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 48h
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Len(t, c.LimitsConfig.StreamRetention, 1)
	require.NotEmpty(t, c.LimitsConfig.StreamRetention[0].Matchers, "Validate must populate matchers")
}

func TestLoad_InvalidRetentionStreamRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 1h
`), 0o600))
	_, err := Load(p, false)
	require.ErrorContains(t, err, "retention period must be >= 24h")
}

func TestLoad_RulerDisabledByDefault(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.False(t, c.RulerEnabled())
	require.Equal(t, "rules-temp", c.Ruler.RulePath)    // lokihouse default (Loki: /rules)
	require.Equal(t, time.Minute, c.Ruler.PollInterval) // Loki default kept
	require.Equal(t, "local", c.Ruler.Evaluation.Mode)  // Loki default kept
}

func TestLoad_Ruler(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
ruler:
  external_url: http://grafana.example
  alertmanager_url: http://am:9093
  poll_interval: 5s
  remote_write:
    enabled: true
    clients:
      prom: {url: "http://prom:9090/api/v1/write"}
ruler_storage:
  backend: filesystem
  filesystem: {dir: /var/lib/lokihouse/rules}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.True(t, c.RulerEnabled())
	require.Equal(t, "http://grafana.example", c.Ruler.ExternalURL.String())
	require.Equal(t, "http://am:9093", c.Ruler.AlertmanagerURL)
	require.Equal(t, 5*time.Second, c.Ruler.PollInterval)
	require.Equal(t, time.Minute, c.Ruler.EvaluationInterval) // Loki default kept
	require.Equal(t, "http://prom:9090/api/v1/write", c.Ruler.RemoteWrite.Clients["prom"].URL.String())
	require.Equal(t, "filesystem", c.RulerStorage.Backend)
	require.Equal(t, "/var/lib/lokihouse/rules", c.RulerStorage.Filesystem.Directory)
}

func TestLoad_RulerRejectsUnsupportedModes(t *testing.T) {
	for name, tc := range map[string]struct{ ruler, want string }{
		"sharding":          {"enable_sharding: true", "enable_sharding"},
		"remote evaluation": {"evaluation: {mode: remote}", "evaluation.mode"},
		"remote write without clients": {
			"remote_write: {enabled: true}", "no clients",
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte("ruler: {"+tc.ruler+"}\nruler_storage: {backend: filesystem, filesystem: {dir: /tmp/r}}\n"), 0o600))
			_, err := Load(p, false)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoad_ServerTimeoutDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	// dskit server defaults, as in Loki.
	require.Equal(t, 30*time.Second, c.Server.HTTPServerReadTimeout)
	require.Equal(t, time.Duration(0), c.Server.HTTPServerReadHeaderTimeout)
	require.Equal(t, 30*time.Second, c.Server.HTTPServerWriteTimeout)
	require.Equal(t, 120*time.Second, c.Server.HTTPServerIdleTimeout)
	require.Equal(t, 30*time.Second, c.Server.GracefulShutdownTimeout)
}

func TestLoad_ServerTimeouts(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`server:
  http_server_read_timeout: 1s
  http_server_read_header_timeout: 2s
  http_server_write_timeout: 3s
  http_server_idle_timeout: 4s
  graceful_shutdown_timeout: 5s
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second},
		[]time.Duration{c.Server.HTTPServerReadTimeout, c.Server.HTTPServerReadHeaderTimeout, c.Server.HTTPServerWriteTimeout, c.Server.HTTPServerIdleTimeout, c.Server.GracefulShutdownTimeout})
}

func TestLoad_UnknownKeyRejected(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, key string }{
		"top level": {"auth_enabld: false\n", "auth_enabld"},
		"nested":    {"server:\n  http_listen_prot: 3100\n", "http_listen_prot"},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte(tc.yaml), 0o600))
			_, err := Load(p, false)
			require.ErrorContains(t, err, tc.key)
		})
	}
}

func TestLoad_ExpandEnv(t *testing.T) {
	t.Setenv("LH_TEST_DB", "fromenv")
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  database: ${LH_TEST_DB}\n  username: ${LH_TEST_USER:-reader}\n"), 0o600))

	c, err := Load(p, true)
	require.NoError(t, err)
	require.Equal(t, "fromenv", c.ClickHouse.Database)
	require.Equal(t, "reader", c.ClickHouse.Username)

	c, err = Load(p, false) // without the flag the text is taken literally, as in Loki
	require.NoError(t, err)
	require.Equal(t, "${LH_TEST_DB}", c.ClickHouse.Database)
}

func TestLoad_PasswordFile(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte("s3cret\n"), 0o600))
	p := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+pw+"\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "s3cret", c.ClickHouse.Password, "trailing newline is stripped")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password: x\n  password_file: "+pw+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password and password_file")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+filepath.Join(dir, "missing")+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password_file")
}

func TestLoad_Logging(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "info", c.Server.LogLevel.String())
	require.Equal(t, "logfmt", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_level: debug, log_format: json}\n"), 0o600))
	c, err = Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "debug", c.Server.LogLevel.String())
	require.Equal(t, "json", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_format: xml}\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "log_format")
}

func TestLoad_ExampleConfig(t *testing.T) {
	_, err := Load(filepath.Join("..", "..", "lokihouse.example.yaml"), false)
	require.NoError(t, err)
}

func TestLoad_PackagedConfig(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", t.TempDir())
	c, err := Load(filepath.Join("..", "..", "debian", "lokihouse.yaml"), true)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", c.Server.HTTPListenAddress, "the package listens on localhost by default")
}
