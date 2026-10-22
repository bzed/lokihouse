package chstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// ErrSchemaV1 is returned (wrapped) by Migrate for a database that still has
// schema version 1 (a retention_days column); run MigrateV2 on it first.
var ErrSchemaV1 = errors.New("schema version 1: stop all lokihouse instances and run lokihouse -schema.migrate")

const migrateSuffix = "_migrate"

// v2Columns are the columns every schema version 2 table copies from version 1.
var v2Columns = map[string]string{
	"logs":                "tenant, stream_id, ts, line, structured_metadata",
	"streams":             "tenant, day, stream_id, labels, labels_str",
	"stream_stats_hourly": "tenant, stream_id, hour, entries, bytes, min_ts, max_ts",
}

// hasRetentionDays reports whether the storage table of name has the version 1 retention_days column.
func hasRetentionDays(ctx context.Context, conn driver.Conn, db string, topo Topology, name string) (bool, error) {
	var n uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database = ? AND table = ? AND name = 'retention_days'",
		db, topo.Local(name)).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("look up schema version of %s: %w", name, err)
	}
	return n > 0, nil
}

// checkFreeSpace refuses a copy that needs more bytes than the smallest disk has free.
func checkFreeSpace(need, free uint64) error {
	if free < need {
		return fmt.Errorf("not enough free disk space to copy the table: need %d bytes, the fullest disk has %d free", need, free)
	}
	return nil
}

// MigrateV2 migrates a schema version 1 database to version 2 (no retention_days) and
// then creates anything missing. It copies each table into <table>_migrate, exchanges
// the two and drops the old data. It is resumable after a crash and a no-op on a version
// 2 database. All other lokihouse instances must be stopped.
func MigrateV2(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error {
	for _, name := range []string{"logs", "streams", "stream_stats_hourly"} {
		v1, err := hasRetentionDays(ctx, conn, db, topo, name)
		if err != nil {
			return err
		}
		if v1 {
			err = migrateTable(ctx, conn, db, topo, name, logger)
		} else {
			// A crash after EXCHANGE TABLES leaves the old data in <name>_migrate.
			err = dropTable(ctx, conn, db, topo, name+migrateSuffix)
		}
		if err == nil {
			err = refreshDistributed(ctx, conn, db, topo, name)
		}
		if err != nil {
			return fmt.Errorf("migrate table %s: %w", name, err)
		}
	}
	return Migrate(ctx, conn, db, topo)
}

func migrateTable(ctx context.Context, conn driver.Conn, db string, topo Topology, name string, logger log.Logger) error {
	if name == "stream_stats_hourly" || name == "logs" {
		// The view reads logs and writes the statistics table; it must not follow the exchange.
		mv := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s SYNC", quoteIdent(db), quoteIdent(topo.Local("stream_stats_hourly")+"_mv"), topo.OnCluster())
		if err := conn.Exec(ctx, mv); err != nil {
			return fmt.Errorf("drop statistics view: %w", err)
		}
	}
	if err := dropTable(ctx, conn, db, topo, name+migrateSuffix); err != nil {
		return err
	}
	if err := createMigrateTable(ctx, conn, db, topo, name); err != nil {
		return err
	}
	need, free, err := diskSpace(ctx, conn, db, topo, name)
	if err != nil {
		return err
	}
	if err := checkFreeSpace(need, free); err != nil {
		return err
	}

	target := name + migrateSuffix // the Distributed table on a cluster, else the table itself
	cols := v2Columns[name]
	cctx := ctx
	if topo.Cluster != "" {
		cctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"parallel_distributed_insert_select": 2}))
	}
	cctx = clickhouse.Context(cctx, clickhouse.WithSettings(clickhouse.Settings{"throw_on_max_partitions_per_insert_block": 0}))
	insert := fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s",
		quoteIdent(db), quoteIdent(target), cols, cols, quoteIdent(db), quoteIdent(name))
	if err := conn.Exec(cctx, insert); err != nil {
		return fmt.Errorf("copy rows: %w", err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", quoteIdent(db), quoteIdent(target))).Scan(&rows); err != nil {
		return fmt.Errorf("count copied rows: %w", err)
	}

	exch := fmt.Sprintf("EXCHANGE TABLES %s.%s AND %s.%s %s", quoteIdent(db), quoteIdent(topo.Local(name)),
		quoteIdent(db), quoteIdent(topo.Local(name+migrateSuffix)), topo.OnCluster())
	if err := conn.Exec(ctx, exch); err != nil {
		return fmt.Errorf("exchange tables: %w", err)
	}
	if err := dropTable(ctx, conn, db, topo, name+migrateSuffix); err != nil {
		return err
	}
	_ = level.Info(logger).Log("msg", "migrated table", "table", name, "rows", rows)
	return nil
}

// createMigrateTable creates <name>_migrate (and its Distributed table) with the version 2 DDL.
func createMigrateTable(ctx context.Context, conn driver.Conn, db string, topo Topology, name string) error {
	return createTables(ctx, conn, db, topo, migrateSuffix, name, true)
}

// refreshDistributed recreates the Distributed table of name when it still has the version 1
// columns: a Distributed table copies the columns of its storage table when it is created,
// so it does not follow EXCHANGE TABLES.
func refreshDistributed(ctx context.Context, conn driver.Conn, db string, topo Topology, name string) error {
	if topo.Cluster == "" {
		return nil
	}
	var n uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database = ? AND table = ? AND name = 'retention_days'", db, name).Scan(&n)
	if err != nil {
		return fmt.Errorf("look up columns of %s: %w", name, err)
	}
	if n == 0 {
		return nil
	}
	if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s SYNC", quoteIdent(db), quoteIdent(name), topo.OnCluster())); err != nil {
		return fmt.Errorf("drop stale %s: %w", name, err)
	}
	return createTables(ctx, conn, db, topo, "", name, false)
}

// createTables runs the DDL of name with the given table name suffix: its storage table
// when withLocal, and on a cluster its Distributed table.
func createTables(ctx context.Context, conn driver.Conn, db string, topo Topology, suffix, name string, withLocal bool) error {
	stmts, err := renderSchema(db, topo, suffix)
	if err != nil {
		return err
	}
	prefix := "CREATE TABLE IF NOT EXISTS " + quoteIdent(db) + "."
	local, dist := topo.Local(name+suffix), name+suffix
	n, want := 0, btoi(withLocal)+btoi(topo.Cluster != "")
	for _, s := range stmts {
		isLocal := withLocal && strings.Contains(s, prefix+local+" ")
		isDist := topo.Cluster != "" && strings.Contains(s, prefix+dist+" ")
		if !isLocal && !isDist {
			continue
		}
		if err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("create %s%s: %w", name, suffix, err)
		}
		n++
	}
	if n != want {
		return fmt.Errorf("create %s%s: rendered %d statements, want %d", name, suffix, n, want)
	}
	return nil
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// dropTable drops the storage table of name and, on a cluster, its Distributed table.
func dropTable(ctx context.Context, conn driver.Conn, db string, topo Topology, name string) error {
	names := []string{name}
	if topo.Cluster != "" {
		names = []string{name, topo.Local(name)}
	}
	for _, n := range names {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s SYNC", quoteIdent(db), quoteIdent(n), topo.OnCluster())); err != nil {
			return fmt.Errorf("drop %s: %w", n, err)
		}
	}
	return nil
}

// diskSpace returns the bytes the storage table of name occupies (the most of any
// replica) and the free bytes of the fullest disk of any node.
func diskSpace(ctx context.Context, conn driver.Conn, db string, topo Topology, name string) (need, free uint64, err error) {
	src := func(table string) string {
		if topo.Cluster == "" {
			return "system." + table
		}
		return fmt.Sprintf("clusterAllReplicas(%s, system, %s)", sqlString(topo.Cluster), table)
	}
	err = conn.QueryRow(ctx, fmt.Sprintf(`SELECT coalesce(max(b), 0) FROM
		(SELECT hostName() AS h, sum(bytes_on_disk) AS b FROM %s WHERE database = ? AND table = ? AND active GROUP BY h)`, src("parts")),
		db, topo.Local(name)).Scan(&need)
	if err != nil {
		return 0, 0, fmt.Errorf("measure %s: %w", name, err)
	}
	if err = conn.QueryRow(ctx, fmt.Sprintf("SELECT min(free_space) FROM %s", src("disks"))).Scan(&free); err != nil {
		return 0, 0, fmt.Errorf("measure free disk space: %w", err)
	}
	return need, free, nil
}
