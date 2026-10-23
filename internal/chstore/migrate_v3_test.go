package chstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func queryStrings(t *testing.T, conn driver.Conn, query string, args ...any) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, args...)
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

// shape lists the columns and skipping indexes of table in db, comparable across databases.
func shape(t *testing.T, conn driver.Conn, db, table string) (cols, idx []string) {
	t.Helper()
	cols = queryStrings(t, conn, "SELECT toString((name, type, default_kind, default_expression)) FROM system.columns WHERE database = ? AND table = ? ORDER BY name", db, table)
	idx = queryStrings(t, conn, "SELECT toString((name, type_full, expr)) FROM system.data_skipping_indices WHERE database = ? AND table = ? ORDER BY name", db, table)
	return cols, idx
}

func TestMigrate_RefusesV2(t *testing.T) {
	conn, db := chtest.NewV2(t)
	topo := chtest.Topology()
	err := chstore.Migrate(context.Background(), conn, db, topo)
	require.True(t, errors.Is(err, chstore.ErrSchemaV2), "got %v", err)
	require.ErrorContains(t, err, "lokihouse -schema.migrate")
	_, idx := shape(t, conn, db, topo.Local("logs"))
	require.Len(t, idx, 1, "a refused migration changes nothing")
	require.Contains(t, idx[0], "line_text")
}

func TestMigrateSchema_V2ToCurrent(t *testing.T) {
	conn, db := chtest.NewV2(t)
	topo := chtest.Topology()
	ctx := context.Background()

	require.NoError(t, chstore.MigrateSchema(ctx, conn, db, topo, log.NewNopLogger()))
	require.NoError(t, chstore.Migrate(ctx, conn, db, topo))

	require.Equal(t, []string{"s1 line 0", "s1 line 1", "s1 line 2", "s2 line 0", "s2 line 1", "s2 line 2"},
		queryStrings(t, conn, "SELECT line FROM logs WHERE line_ascii_escape = false ORDER BY line"))

	fresh, freshDB := chtest.New(t)
	tables := []string{topo.Local("logs")}
	if topo.Cluster != "" {
		tables = append(tables, "logs")
	}
	for _, tbl := range tables {
		wantCols, wantIdx := shape(t, fresh, freshDB, tbl)
		gotCols, gotIdx := shape(t, conn, db, tbl)
		require.Equal(t, wantCols, gotCols, tbl)
		require.Equal(t, wantIdx, gotIdx, tbl)
	}

	w := &chstore.Writer{Conn: conn}
	ts := chtest.V2Day.Add(time.Hour)
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="s3"}`, Entries: []logproto.Entry{{
		Timestamp: ts, Line: "new", StructuredMetadata: []logproto.LabelAdapter{{Name: "trace_id", Value: "x"}},
	}}}}))
	require.Equal(t, []string{"new"}, queryStrings(t, conn, "SELECT line FROM logs WHERE structured_metadata['trace_id'] = 'x'"))
}

func TestMigrateSchema_Idempotent(t *testing.T) {
	conn, db := chtest.NewV2(t)
	topo := chtest.Topology()
	ctx := context.Background()
	require.NoError(t, chstore.MigrateSchema(ctx, conn, db, topo, log.NewNopLogger()))
	require.NoError(t, chstore.MigrateSchema(ctx, conn, db, topo, log.NewNopLogger()))
}

// Rows stored before the migration are not back-filled, but ClickHouse computes a
// materialized column that a part lacks when it is read: the escape flag is right for them.
func TestMigrateSchema_OldRowsHaveTheFlag(t *testing.T) {
	conn, db := chtest.NewV2(t)
	topo := chtest.Topology()
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	line := string(rune(92)) + "u0065rror"
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="s1"}`, Entries: []logproto.Entry{{Timestamp: chtest.V2Day.Add(time.Hour), Line: line}}}}))

	require.NoError(t, chstore.MigrateSchema(ctx, conn, db, topo, log.NewNopLogger()))
	require.Equal(t, []string{line}, queryStrings(t, conn, "SELECT line FROM logs WHERE line_ascii_escape"))
}
