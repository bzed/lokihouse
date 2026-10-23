package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
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

func TestMigrate_CreatesMetadataIndexes(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	rows, err := conn.Query(ctx, "SELECT name FROM system.data_skipping_indices WHERE database = ? AND table = ? ORDER BY name",
		db, chtest.Topology().Local("logs"))
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got = append(got, s)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"line_ascii_escape_idx", "line_text", "sm_keys", "sm_vals"}, got)

	// The Distributed table (on a cluster) carries the column too.
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE line_ascii_escape").Scan(&n))
}

func TestLineASCIIEscapeFlag(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	lines := []string{
		`{"l":"\u0065rror"}`,
		`{"l":"\u004A"}`,
		`{"l":"a\u003cb"}`,
		`{"l":"\u00e9"}`,
		`{"l":"plain"}`,
	}
	var entries []logproto.Entry
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i, l := range lines {
		entries = append(entries, logproto.Entry{Timestamp: base.Add(time.Duration(i) * time.Second), Line: l})
	}
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="a"}`, Entries: entries}}))

	rows, err := conn.Query(ctx, "SELECT line_ascii_escape FROM logs ORDER BY ts")
	require.NoError(t, err)
	defer rows.Close()
	var got []bool
	for rows.Next() {
		var b bool
		require.NoError(t, rows.Scan(&b))
		got = append(got, b)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []bool{true, true, false, false, false}, got)
}
