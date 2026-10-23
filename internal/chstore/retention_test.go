package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestRetentionPrimitives_RejectUnknownTable(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	topo := chtest.Topology()
	d := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	_, err := chstore.DayPartitions(ctx, conn, db, topo, "delete_requests")
	require.ErrorContains(t, err, "has no retention")
	require.ErrorContains(t, chstore.DropDay(ctx, conn, db, topo, "delete_requests", d), "has no retention")
	require.ErrorContains(t, chstore.DeleteDays(ctx, conn, db, topo, "delete_requests", "t", []uint64{1}, []time.Time{d}), "has no retention")
}

func TestRetentionPrimitives_EmptyInputIsNoop(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	d := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	days, err := chstore.DaysWithRows(ctx, conn, "t", nil, d)
	require.NoError(t, err)
	require.Empty(t, days)
	require.NoError(t, chstore.DeleteDays(ctx, conn, db, chtest.Topology(), "logs", "t", nil, []time.Time{d}))
	require.NoError(t, chstore.DeleteDays(ctx, conn, db, chtest.Topology(), "logs", "t", []uint64{1}, nil))
}

// A dead connection surfaces as errors, never panics or silent success.
func TestRetentionPrimitives_ConnectionErrors(t *testing.T) {
	conn, db := chtest.New(t)
	require.NoError(t, conn.Close())
	ctx := context.Background()
	topo := chtest.Topology()
	d := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	_, err := chstore.DayPartitions(ctx, conn, db, topo, "logs")
	require.Error(t, err)
	require.Error(t, chstore.DropDay(ctx, conn, db, topo, "logs", d))
	require.Error(t, chstore.StreamsUpTo(ctx, conn, d, func(chstore.StreamRef) error { return nil }))
	_, err = chstore.DaysWithRows(ctx, conn, "t", []uint64{1}, d)
	require.Error(t, err)
	require.Error(t, chstore.DeleteDays(ctx, conn, db, topo, "logs", "t", []uint64{1}, []time.Time{d}))
	require.Error(t, chstore.ApplyDeletedMask(ctx, conn, db, topo, "logs"))
}

func TestMigrateV2_ConnectionErrors(t *testing.T) {
	conn, db := chtest.New(t)
	require.NoError(t, conn.Close())
	ctx := context.Background()
	require.Error(t, chstore.MigrateSchema(ctx, conn, db, chtest.Topology(), log.NewNopLogger()))
	require.Error(t, chstore.Migrate(ctx, conn, db, chtest.Topology()))
}

func TestMigrateV2_MeasuresDiskSpace(t *testing.T) {
	conn, db := chtest.NewV1(t)
	ctx := context.Background()
	need, free, err := chstore.DiskSpace(ctx, conn, db, chtest.Topology(), "logs")
	require.NoError(t, err)
	require.Positive(t, need)
	require.Positive(t, free)
}

func TestRenderSchema_ReplicaPathIsUniquePerSuffix(t *testing.T) {
	topo := chstore.Topology{Replicated: true, ReplicaPath: "/ch/{shard}/lokihouse", ReplicaName: "{replica}"}
	plain, err := chstore.RenderSchema("db", topo)
	require.NoError(t, err)
	require.Contains(t, plain[0], "'/ch/{shard}/lokihouse'")
	mig, err := chstore.RenderSchemaWithSuffix("db", topo, "_migrate")
	require.NoError(t, err)
	require.Contains(t, mig[0], "'/ch/{shard}/lokihouse_migrate'")
}

func TestOpenConnectionFailureIsReported(t *testing.T) {
	c, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}, DialTimeout: 200 * time.Millisecond})
	require.NoError(t, err)
	require.Error(t, chstore.Migrate(context.Background(), c, "x", chstore.Topology{}))
}
