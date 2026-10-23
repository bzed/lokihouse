package chstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// ErrSchemaV2 is returned (wrapped) by Migrate for a database that still has schema
// version 2 (a logs table without line_ascii_escape); run MigrateSchema on it first.
var ErrSchemaV2 = errors.New("schema version 2: stop all lokihouse instances and run lokihouse -schema.migrate")

// The version 3 additions to logs. They must equal the DDL in schema.sql
// (TestMigrateSchema_V2ToCurrent compares a migrated and a fresh database).
const (
	v3Column = `line_ascii_escape Bool MATERIALIZED match(line, '\\\\u00(3[0-9]|4[1-9a-fA-F]|5[0-9aA]|6[1-9a-fA-F]|7[0-9aA])')`
	v3Alter  = "ADD COLUMN IF NOT EXISTS " + v3Column +
		", ADD INDEX IF NOT EXISTS sm_keys mapKeys(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1" +
		", ADD INDEX IF NOT EXISTS sm_vals mapValues(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1" +
		", ADD INDEX IF NOT EXISTS line_ascii_escape_idx line_ascii_escape TYPE minmax GRANULARITY 1"
)

// hasColumn reports whether table of db has the column.
func hasColumn(ctx context.Context, conn driver.Conn, db, table, column string) (bool, error) {
	var n uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database = ? AND table = ? AND name = ?", db, table, column).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("look up column %s of %s: %w", column, table, err)
	}
	return n > 0, nil
}

// v3Pending reports whether logs exists but lacks the version 3 column, in its
// storage table or (on a cluster) its Distributed table.
func v3Pending(ctx context.Context, conn driver.Conn, db string, topo Topology) (bool, error) {
	var exists uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", db, topo.Local("logs")).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("look up logs table: %w", err)
	}
	if exists == 0 {
		return false, nil
	}
	tables := []string{topo.Local("logs")}
	if topo.Cluster != "" {
		tables = append(tables, "logs")
	}
	for _, tbl := range tables {
		ok, err := hasColumn(ctx, conn, db, tbl, "line_ascii_escape")
		if err != nil {
			return false, err
		}
		if !ok {
			return true, nil
		}
	}
	return false, nil
}

// MigrateV3 migrates a schema version 2 database to version 3: it adds the structured-metadata
// text indexes and the line_ascii_escape column to logs. Nothing is copied or re-indexed;
// only parts written or merged afterwards carry the new indexes. A no-op on a version 3
// database. All other lokihouse instances must be stopped.
func MigrateV3(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error {
	pending, err := v3Pending(ctx, conn, db, topo)
	if err != nil || !pending {
		return err
	}
	actx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"alter_sync": 2}))
	stmt := fmt.Sprintf("ALTER TABLE %s.%s %s %s", quoteIdent(db), quoteIdent(topo.Local("logs")), topo.OnCluster(), v3Alter)
	if err := conn.Exec(actx, stmt); err != nil {
		return fmt.Errorf("migrate table logs: %w", err)
	}
	if topo.Cluster != "" {
		stmt = fmt.Sprintf("ALTER TABLE %s.%s %s ADD COLUMN IF NOT EXISTS %s", quoteIdent(db), quoteIdent("logs"), topo.OnCluster(), v3Column)
		if err := conn.Exec(actx, stmt); err != nil {
			return fmt.Errorf("migrate distributed table logs: %w", err)
		}
	}
	_ = level.Info(logger).Log("msg", "migrated table", "table", "logs", "to", 3)
	return nil
}

// MigrateSchema brings a database of any older schema version to the current one and
// creates anything missing: version 1 tables are copied (MigrateV2, with the current DDL),
// version 2 tables are altered (MigrateV3). All other lokihouse instances must be stopped.
func MigrateSchema(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error {
	if err := MigrateV2(ctx, conn, db, topo, logger); err != nil {
		return err
	}
	if err := MigrateV3(ctx, conn, db, topo, logger); err != nil {
		return err
	}
	return Migrate(ctx, conn, db, topo)
}
