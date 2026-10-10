// Package chtest provides throwaway ClickHouse databases for tests.
package chtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/bzed/lokihouse/internal/chstore"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Topology is the layout tests run against: a single server by default,
// or the cluster named by LOKIHOUSE_TEST_CH_CLUSTER (see test/cluster/up.sh).
func Topology() chstore.Topology {
	c := os.Getenv("LOKIHOUSE_TEST_CH_CLUSTER")
	return chstore.Topology{Cluster: c, Replicated: c != "" && os.Getenv("LOKIHOUSE_TEST_CH_REPLICATED") != "0"}
}

// Options returns connection options for the test server with the given default database.
func Options(db string) *clickhouse.Options {
	return &clickhouse.Options{
		Addr: []string{env("LOKIHOUSE_TEST_CH_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{
			Database: db,
			Username: env("LOKIHOUSE_TEST_CH_USER", "default"),
			Password: os.Getenv("LOKIHOUSE_TEST_CH_PASSWORD"),
		},
		Settings: Topology().Settings(),
	}
}

// New creates and migrates a fresh database, dropped (on every node) when the test ends.
func New(t *testing.T) (driver.Conn, string) {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_test_" + hex.EncodeToString(b)
	ctx := context.Background()
	topo := Topology()

	admin, err := clickhouse.Open(Options("default"))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("clickhouse not reachable (set LOKIHOUSE_TEST_CH_ADDR or start: podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9): %v", err)
	}
	if err := chstore.Migrate(ctx, admin, db, topo); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	conn, err := clickhouse.Open(Options(db))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = chstore.DropDatabase(ctx, admin, db, topo)
		_ = admin.Close()
	})
	return conn, db
}
