package chstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// snapshot returns every stored row of logs, streams and stream_stats_hourly in a
// retention_days-free, comparable form. Stats are summed per key: v1 kept one row
// per retention_days, which a merge adds up.
func snapshot(t *testing.T, conn driver.Conn) (logs, streams, stats []string) {
	t.Helper()
	ctx := context.Background()
	collect := func(query string) []string {
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			require.NoError(t, rows.Scan(&s))
			out = append(out, s)
		}
		require.NoError(t, rows.Err())
		return out
	}
	logs = collect(`SELECT toString((tenant, stream_id, ts, line, structured_metadata)) AS r FROM logs ORDER BY r`)
	streams = collect(`SELECT toString((tenant, day, stream_id, labels, labels_str)) AS r FROM streams ORDER BY r`)
	stats = collect(`SELECT toString((tenant, stream_id, hour, e, b, mn, mx)) AS r FROM (
		SELECT tenant, stream_id, hour, sum(entries) AS e, sum(bytes) AS b, min(min_ts) AS mn, max(max_ts) AS mx
		FROM stream_stats_hourly GROUP BY tenant, stream_id, hour) ORDER BY r`)
	return logs, streams, stats
}

func columns(t *testing.T, conn driver.Conn, db, table string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position", db, table)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	require.NoError(t, rows.Err())
	return out
}

func requireNoMigrateTables(t *testing.T, conn driver.Conn, db string) {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count() FROM system.tables WHERE database = ? AND name LIKE '%\\_migrate%'", db).Scan(&n))
	require.Zero(t, n, "no leftover *_migrate table")
}

func TestMigrate_RefusesV1(t *testing.T) {
	conn, db := chtest.NewV1(t)
	before, _, _ := snapshot(t, conn)
	err := chstore.Migrate(context.Background(), conn, db, chtest.Topology())
	require.True(t, errors.Is(err, chstore.ErrSchemaV1), "got %v", err)
	require.ErrorContains(t, err, "lokihouse -schema.migrate")
	after, _, _ := snapshot(t, conn)
	require.Equal(t, before, after, "a refused migration writes nothing")
	require.Contains(t, columns(t, conn, db, chtest.Topology().Local("logs")), "retention_days")
}

func TestMigrateV2_CopiesEverything(t *testing.T) {
	conn, db := chtest.NewV1(t)
	topo := chtest.Topology()
	ctx := context.Background()
	logs, streams, stats := snapshot(t, conn)
	require.NotEmpty(t, logs)
	require.NotEmpty(t, stats)

	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))

	for _, tbl := range []string{"logs", "streams", "stream_stats_hourly"} {
		require.NotContains(t, columns(t, conn, db, topo.Local(tbl)), "retention_days", tbl)
	}
	gl, gs, gst := snapshot(t, conn)
	require.Equal(t, logs, gl)
	require.Equal(t, streams, gs)
	require.Equal(t, stats, gst)
	requireNoMigrateTables(t, conn, db)

	// The statistics view was recreated: a new push is counted.
	w := &chstore.Writer{Conn: conn}
	ts := chtest.V1Day.Add(5 * time.Hour)
	require.NoError(t, w.Write(ctx, "a", []logproto.Stream{{Labels: `{app="new"}`, Entries: []logproto.Entry{{Timestamp: ts, Line: "x"}}}}))
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT sum(entries) FROM stream_stats_hourly WHERE tenant = 'a' AND hour = toStartOfHour(?)", ts).Scan(&n))
	require.Positive(t, n)
	var viaNew uint64
	_, id, err := chstore.StreamIdentity(`{app="new"}`)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRow(ctx, "SELECT sum(entries) FROM stream_stats_hourly WHERE tenant = 'a' AND stream_id = ?", id).Scan(&viaNew))
	require.Equal(t, uint64(1), viaNew)
}

func TestMigrateV2_Idempotent(t *testing.T) {
	conn, db := chtest.NewV1(t)
	topo := chtest.Topology()
	ctx := context.Background()
	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))
	logs, streams, stats := snapshot(t, conn)
	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))
	gl, gs, gst := snapshot(t, conn)
	require.Equal(t, logs, gl)
	require.Equal(t, streams, gs)
	require.Equal(t, stats, gst)
}

// A migration killed after creating (and partly filling) logs_migrate must finish on
// the next run with no lost and no duplicated rows.
func TestMigrateV2_ResumesAfterCrash(t *testing.T) {
	conn, db := chtest.NewV1(t)
	topo := chtest.Topology()
	ctx := context.Background()
	logs, streams, stats := snapshot(t, conn)

	local := topo.Local("logs")
	mig := topo.Local("logs_migrate")
	onc := topo.OnCluster()
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s %s AS %s.%s", db, mig, onc, db, local)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.%s (tenant, stream_id, ts, line, structured_metadata, retention_days) SELECT tenant, stream_id, ts, line, structured_metadata, retention_days FROM %s.%s WHERE stream_id = 1",
		db, mig, db, local)))

	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))

	gl, gs, gst := snapshot(t, conn)
	require.Equal(t, logs, gl)
	require.Equal(t, streams, gs)
	require.Equal(t, stats, gst)
	var n, u uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(), uniqExact(tenant, stream_id, ts, entry_hash) FROM logs").Scan(&n, &u))
	require.Equal(t, n, u, "no duplicated rows")
	requireNoMigrateTables(t, conn, db)
}

// A crash after EXCHANGE TABLES leaves the old data in logs_migrate: the next run drops it.
func TestMigrateV2_DropsOldDataAfterExchange(t *testing.T) {
	conn, db := chtest.NewV1(t)
	topo := chtest.Topology()
	ctx := context.Background()
	logs, streams, stats := snapshot(t, conn)
	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))

	mig := topo.Local("logs_migrate")
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s %s AS %s.%s", db, mig, topo.OnCluster(), db, topo.Local("logs"))))

	require.NoError(t, chstore.MigrateV2(ctx, conn, db, topo, log.NewNopLogger()))
	requireNoMigrateTables(t, conn, db)
	gl, gs, gst := snapshot(t, conn)
	require.Equal(t, logs, gl)
	require.Equal(t, streams, gs)
	require.Equal(t, stats, gst)
}

func TestCheckFreeSpace(t *testing.T) {
	require.ErrorContains(t, chstore.CheckFreeSpace(100, 99), "not enough free disk space")
	require.NoError(t, chstore.CheckFreeSpace(100, 100))
	require.NoError(t, chstore.CheckFreeSpace(0, 0))
}
