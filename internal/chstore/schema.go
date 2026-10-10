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
	schemaTmpl = template.Must(template.New("schema").Funcs(template.FuncMap{"lit": sqlString, "ttl": ttlExpr}).Parse(schemaSQL))
	stmtSplit  = regexp.MustCompile(`;\s*\n`)
)

// ttlExpr is the expiry of a row whose UTC day (as Date32) is day; see schema.sql.
func ttlExpr(day string) string {
	return "toDateTime(least(" + day + " + toIntervalDay(if(retention_days = 0, 65535, retention_days)), toDate32('2106-01-01')), 'UTC')"
}

type schemaData struct {
	Topology
	DB string
}

func (d schemaData) DBName() string { return quoteIdent(d.DB) }

// RenderSchema returns the DDL statements for db under topo, in execution order.
func RenderSchema(db string, topo Topology) ([]string, error) {
	var buf bytes.Buffer
	if err := schemaTmpl.Execute(&buf, schemaData{Topology: topo, DB: db}); err != nil {
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
