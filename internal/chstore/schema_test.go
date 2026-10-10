package chstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestMigrate_CreatesTablesIdempotently(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	require.NoError(t, chstore.Migrate(ctx, conn, db, chtest.Topology())) // second run must not fail

	var n uint64
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name IN ('logs','streams')", db).Scan(&n))
	require.Equal(t, uint64(2), n)
}

func TestMigrate_UnknownClusterFails(t *testing.T) {
	conn, _ := chtest.New(t)
	err := chstore.Migrate(context.Background(), conn, "lh_never_created", chstore.Topology{Cluster: "no_such_cluster"})
	require.ErrorContains(t, err, `cluster "no_such_cluster" not found in system.clusters`)
}

// A cluster database opened without clickhouse.cluster would insert into Distributed
// tables without foreground inserts or quorum, silently losing read-your-writes.
func TestMigrate_ClusterDatabaseNeedsClusterConfig(t *testing.T) {
	requireCluster(t)
	conn, db := chtest.New(t)
	err := chstore.Migrate(context.Background(), conn, db, chstore.Topology{})
	require.ErrorContains(t, err, "set clickhouse.cluster")
}
