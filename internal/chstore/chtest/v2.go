package chtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"

	"github.com/bzed/lokihouse/internal/chstore"
)

// V2Day is the UTC day of the rows NewV2 inserts.
var V2Day = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// NewV2 creates a fresh database with the schema version 2 tables (no
// structured-metadata indexes, no line_ascii_escape column) and seeds it for
// the tenant "t1": two streams ({app="s1"}, {app="s2"}), three rows each. The
// database is dropped (on every node) when the test ends. Used by the schema
// migration tests.
func NewV2(t testing.TB) (driver.Conn, string) {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_v2_" + hex.EncodeToString(b)
	ctx := context.Background()
	topo := Topology()

	_, file, _, _ := runtime.Caller(0)
	src, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "testdata", "schema_v2.sql"))
	if err != nil {
		t.Fatalf("read v2 schema: %v", err)
	}
	stmts, err := chstore.RenderTemplate(string(src), db, topo)
	if err != nil {
		t.Fatalf("render v2 schema: %v", err)
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
	for _, s := range stmts {
		if err := admin.Exec(ctx, s); err != nil {
			t.Fatalf("create v2 table: %v\n%s", err, s)
		}
	}
	conn, err := clickhouse.Open(Options(db))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	w := &chstore.Writer{Conn: conn}
	for _, app := range []string{"s1", "s2"} {
		var entries []logproto.Entry
		for i := range 3 {
			entries = append(entries, logproto.Entry{Timestamp: V2Day.Add(time.Duration(i) * time.Minute), Line: fmt.Sprintf("%s line %d", app, i)})
		}
		if err := w.Write(ctx, "t1", []logproto.Stream{{Labels: fmt.Sprintf(`{app=%q}`, app), Entries: entries}}); err != nil {
			t.Fatalf("seed v2: %v", err)
		}
	}
	return conn, db
}
