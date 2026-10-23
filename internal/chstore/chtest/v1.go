package chtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/bzed/lokihouse/internal/chstore"
)

// v1Data is the template data of testdata/schema_v1.sql.
type v1Data struct {
	chstore.Topology
	DB string
}

func (d v1Data) DBName() string { return "`" + d.DB + "`" }

// V1Day is the first UTC day of the rows NewV1 inserts; they span three days.
// It is relative to today so the seeded rows stay inside the 7 day retention of the
// version 1 TTL; a fixed date would let ClickHouse expire them mid-test.
var V1Day = time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -3)

// NewV1 creates a fresh database with the schema version 1 tables (a
// retention_days column, a TTL, retention partitions) and seeds it for the
// tenants "a" and "b": two streams each (retention_days 0 and 7), three rows
// per stream and day over the three days from V1Day. The database is dropped
// (on every node) when the test ends. Used by the schema migration tests.
func NewV1(t testing.TB) (driver.Conn, string) {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_v1_" + hex.EncodeToString(b)
	ctx := context.Background()
	topo := Topology()

	_, file, _, _ := runtime.Caller(0)
	src, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "testdata", "schema_v1.sql"))
	if err != nil {
		t.Fatalf("read v1 schema: %v", err)
	}
	tmpl, err := template.New("v1").Funcs(template.FuncMap{
		"lit": func(s string) string { return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'" },
		"ttl": func(day string) string {
			return "toDateTime(least(" + day + " + toIntervalDay(if(retention_days = 0, 65535, retention_days)), toDate32('2106-01-01')), 'UTC')"
		},
	}).Parse(string(src))
	if err != nil {
		t.Fatalf("parse v1 schema: %v", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, v1Data{Topology: topo, DB: db}); err != nil {
		t.Fatalf("render v1 schema: %v", err)
	}

	admin, err := clickhouse.Open(Options("default"))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() {
		_ = chstore.DropDatabase(ctx, admin, db, topo)
		_ = admin.Close()
	})
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE `%s` %s", db, topo.OnCluster())); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for _, s := range regexp.MustCompile(`;\s*\n`).Split(buf.String(), -1) {
		if s = strings.TrimSpace(s); s != "" {
			if err := admin.Exec(ctx, s); err != nil {
				t.Fatalf("create v1 table: %v\n%s", err, s)
			}
		}
	}
	conn, err := clickhouse.Open(Options(db))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	seedV1(t, conn)
	return conn, db
}

func seedV1(t testing.TB, conn driver.Conn) {
	t.Helper()
	ctx := context.Background()
	logs, err := conn.PrepareBatch(ctx, "INSERT INTO logs (tenant, stream_id, ts, line, structured_metadata, retention_days)")
	if err != nil {
		t.Fatalf("prepare logs: %v", err)
	}
	streams, err := conn.PrepareBatch(ctx, "INSERT INTO streams (tenant, day, stream_id, labels, labels_str, retention_days)")
	if err != nil {
		t.Fatalf("prepare streams: %v", err)
	}
	for _, tenant := range []string{"a", "b"} {
		for sid, ret := range map[uint64]uint16{1: 0, 2: 7} {
			lbl := fmt.Sprintf(`{app="s%d"}`, sid)
			for d := range 3 {
				day := V1Day.AddDate(0, 0, d)
				if err := streams.Append(tenant, day, sid, map[string]string{"app": fmt.Sprintf("s%d", sid)}, lbl, ret); err != nil {
					t.Fatalf("append stream: %v", err)
				}
				for i := range 3 {
					ts := day.Add(time.Duration(i)*time.Hour + time.Duration(i))
					line := fmt.Sprintf("%s s%d d%d i%d", tenant, sid, d, i)
					if err := logs.Append(tenant, sid, ts, line, map[string]string{"k": fmt.Sprint("v", i)}, ret); err != nil {
						t.Fatalf("append log: %v", err)
					}
				}
			}
		}
	}
	if err := streams.Send(); err != nil {
		t.Fatalf("insert streams: %v", err)
	}
	if err := logs.Send(); err != nil {
		t.Fatalf("insert logs: %v", err)
	}
}
