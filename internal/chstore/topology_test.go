package chstore_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
)

func TestTopology_SingleNode(t *testing.T) {
	var topo chstore.Topology
	require.Equal(t, "", topo.OnCluster())
	require.Equal(t, "logs", topo.Local("logs"))
	require.Equal(t, "ReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Empty(t, topo.Settings())
}

func TestTopology_ReplicatedCluster(t *testing.T) {
	topo := chstore.Topology{Cluster: "lokihouse", Replicated: true}
	require.Equal(t, "ON CLUSTER `lokihouse`", topo.OnCluster())
	require.Equal(t, "logs_local", topo.Local("logs"))
	require.Equal(t, "ReplicatedReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Equal(t, 1, topo.Settings()["distributed_foreground_insert"])
	require.Equal(t, "auto", topo.Settings()["insert_quorum"])

	topo.ReplicaPath, topo.ReplicaName = "/clickhouse/tables/{shard}/{database}/{table}", "{replica}"
	require.Equal(t, "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
		topo.Engine("ReplacingMergeTree"))
}

func TestTopology_ShardedWithoutReplication(t *testing.T) {
	topo := chstore.Topology{Cluster: "c"}
	require.Equal(t, "ReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Equal(t, "streams_local", topo.Local("streams"))
	require.NotContains(t, topo.Settings(), "insert_quorum")
}

func TestRenderSchema_SingleNode(t *testing.T) {
	stmts, err := chstore.RenderSchema("db", chstore.Topology{})
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	all := strings.Join(stmts, "\n")
	require.Contains(t, all, "CREATE TABLE IF NOT EXISTS `db`.logs (")
	require.NotContains(t, all, "ON CLUSTER")
	require.NotContains(t, all, "Distributed")
}

func TestRenderSchema_Cluster(t *testing.T) {
	stmts, err := chstore.RenderSchema("db", chstore.Topology{Cluster: "lokihouse", Replicated: true})
	require.NoError(t, err)
	require.Len(t, stmts, 4)
	require.Contains(t, stmts[0], "CREATE TABLE IF NOT EXISTS `db`.logs_local ON CLUSTER `lokihouse` (")
	require.Contains(t, stmts[0], "ENGINE = ReplicatedReplacingMergeTree")
	require.Contains(t, stmts[2], "ENGINE = Distributed('lokihouse', 'db', 'logs_local', cityHash64(tenant, stream_id))")
	require.Contains(t, stmts[3], "ENGINE = Distributed('lokihouse', 'db', 'streams_local', cityHash64(tenant, stream_id))")
}
