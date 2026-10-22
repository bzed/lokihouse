package chstore

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed schema.sql
var schemaSQL string

var (
	schemaTmpl = template.Must(template.New("schema").Funcs(template.FuncMap{"lit": sqlString}).Parse(schemaSQL))
	stmtSplit  = regexp.MustCompile(`;\s*\n`)
)

type schemaData struct {
	Topology
	DB     string
	Suffix string // appended to every table name; "" except while migrating (see migrate_v2.go)
}

func (d schemaData) DBName() string { return quoteIdent(d.DB) }

// Table is the name of the table that physically stores name's rows.
func (d schemaData) Table(name string) string { return d.Local(name + d.Suffix) }

// Dist is the name of the Distributed table of name (only rendered on a cluster).
func (d schemaData) Dist(name string) string { return name + d.Suffix }

// Engine is Topology.Engine with the Keeper path made unique per suffix, so a
// _migrate table never shares replication state with the table it replaces.
func (d schemaData) Engine(base string, params ...string) string {
	t := d.Topology
	if t.ReplicaPath != "" {
		t.ReplicaPath += d.Suffix
	}
	return t.Engine(base, params...)
}

// RenderSchema returns the DDL statements for db under topo, in execution order.
func RenderSchema(db string, topo Topology) ([]string, error) { return renderSchema(db, topo, "") }

func renderSchema(db string, topo Topology, suffix string) ([]string, error) {
	var buf bytes.Buffer
	if err := schemaTmpl.Execute(&buf, schemaData{Topology: topo, DB: db, Suffix: suffix}); err != nil {
		return nil, err
	}
	var out []string
	for _, s := range stmtSplit.Split(buf.String(), -1) {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out, nil
}

// Migrate creates all lokihouse tables in db. Safe to run repeatedly,
// and from several lokihouse instances at once.
func Migrate(ctx context.Context, conn driver.Conn, db string, topo Topology) error {
	if topo.Cluster != "" {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.clusters WHERE cluster = ?", topo.Cluster).Scan(&n); err != nil {
			return fmt.Errorf("look up cluster: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("clickhouse cluster %q not found in system.clusters (check remote_servers)", topo.Cluster)
		}
	}
	stmts, err := RenderSchema(db, topo)
	if err != nil {
		return err
	}
	stmts = append([]string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s %s", quoteIdent(db), topo.OnCluster())}, stmts...)
	for _, stmt := range stmts {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, stmt)
		}
	}
	if topo.Cluster == "" {
		// CREATE … IF NOT EXISTS is a no-op on an existing cluster database. Without the
		// cluster settings, pushes would reach Distributed tables asynchronously.
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = 'logs' AND engine = 'Distributed'", db).Scan(&n); err != nil {
			return fmt.Errorf("check topology: %w", err)
		}
		if n > 0 {
			return fmt.Errorf("database %s holds Distributed tables: set clickhouse.cluster", quoteIdent(db))
		}
	}
	return nil
}

// DropDatabase removes db on every node of the topology. Used by tests.
func DropDatabase(ctx context.Context, conn driver.Conn, db string, topo Topology) error {
	return conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s %s SYNC", quoteIdent(db), topo.OnCluster()))
}
