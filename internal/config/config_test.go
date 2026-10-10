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
	c, err := Load(p)
	require.NoError(t, err)
	require.True(t, c.AuthEnabled)
	require.Equal(t, 3101, c.Server.HTTPListenPort)
	require.Equal(t, []string{"ch:9000"}, c.ClickHouse.Addr)
	require.EqualValues(t, 1024, c.LimitsConfig.MaxLineSize)
	require.Equal(t, 7*24*time.Hour, time.Duration(c.LimitsConfig.RejectOldSamplesMaxAge)) // Loki default kept
	require.Equal(t, chstore.Topology{}, c.Topology())                                    // single node by default
}

func TestLoad_Cluster(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse:
  addr: ["ch1:9000", "ch3:9000"]
  cluster: lokihouse
  settings: {insert_quorum: 2, max_execution_time: 60}
`), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.Equal(t, chstore.Topology{Cluster: "lokihouse", Replicated: true}, c.Topology())
	s := c.CHSettings()
	require.EqualValues(t, 2, s["insert_quorum"]) // explicit setting wins over the topology default
	require.EqualValues(t, 60, s["max_execution_time"])
	require.EqualValues(t, 1, s["distributed_foreground_insert"])
}
