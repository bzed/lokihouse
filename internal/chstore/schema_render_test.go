package chstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderSchema_NoRetention(t *testing.T) {
	stmts, err := RenderSchema("db", Topology{})
	require.NoError(t, err)
	all := strings.Join(stmts, "\n")
	require.NotContains(t, all, "retention_days")
	require.NotContains(t, all, "TTL")
	require.Contains(t, all, "PARTITION BY toDate(ts)")
	require.Contains(t, all, "PARTITION BY day")
	require.Contains(t, all, "PARTITION BY toDate(hour)")
}

func TestRenderSchema_Suffix(t *testing.T) {
	stmts, err := renderSchema("db", Topology{Cluster: "c"}, "_migrate")
	require.NoError(t, err)
	all := strings.Join(stmts, "\n")
	require.Contains(t, all, "CREATE TABLE IF NOT EXISTS `db`.logs_migrate_local")
	require.Contains(t, all, "CREATE TABLE IF NOT EXISTS `db`.logs_migrate ON CLUSTER")

	stmts, err = renderSchema("db", Topology{}, "_migrate")
	require.NoError(t, err)
	require.Contains(t, strings.Join(stmts, "\n"), "CREATE TABLE IF NOT EXISTS `db`.logs_migrate (")
}
