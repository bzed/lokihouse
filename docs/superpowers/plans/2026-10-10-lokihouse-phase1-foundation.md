# Lokihouse Phase 1 (Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go service, `lokihouse`, that accepts Loki pushes (proto, JSON, OTLP) into ClickHouse and answers `query`, `query_range`, `labels`, `label values` and `series` with byte-for-byte Loki-equal results. A differential test suite against a real Loki built from the same commit proves the equality. The service runs unchanged on a single ClickHouse server or on a ClickHouse cluster (sharded, replicated, or both).

**Architecture:** Import Loki's own packages (`logql`, `logql/syntax`, `logql/log`, `loghttp`, `loghttp/push`, `util/marshal`, `validation`) at a pinned commit. Implement Loki's `logql.Querier` interface on top of ClickHouse ("Tier B"). ClickHouse resolves tenant, stream selector, time range and leading literal line filters. Loki's pipeline and evaluator do everything else, so semantics are identical by construction. SQL push-down of whole queries ("Tier A") is Phase 2 and out of scope here. On a cluster, rows live in `Replicated*MergeTree` `_local` tables and are reached through `Distributed` tables that keep the plain names `logs`/`streams`. Query code is therefore topology-agnostic; only DDL and mutations go through `chstore.Topology`.

**Tech Stack:** Go 1.27, `github.com/grafana/loki/v3` @ `fbc3f0770b8c`, `github.com/ClickHouse/clickhouse-go/v2` v2.49.0, ClickHouse ≥ 26.9 (local server, or podman `docker.io/clickhouse/clickhouse-server:26.9`), `github.com/stretchr/testify`. For cluster tests: a podman 2-shard × 2-replica ClickHouse 26.9 cluster with ClickHouse Keeper (Task 12).

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`

## Global Constraints

- Module path: `github.com/bzed/lokihouse`; binary name `lokihouse`; license AGPL-3.0-only (file `LICENSE` copied from Loki).
- Loki pinned at commit `fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1` (`github.com/grafana/loki/v3 v3.0.0-20261010092436-fbc3f0770b8c`). Never bump it inside a feature task.
- `go.mod` must carry all of Loki's `replace` directives (listed in Task 1) or the build fails.
- Containers: **podman** only, never docker.
- ClickHouse for tests: env `LOKIHOUSE_TEST_CH_ADDR` (default `localhost:9000`), user `LOKIHOUSE_TEST_CH_USER` (default `default`), password `LOKIHOUSE_TEST_CH_PASSWORD` (default empty). Tests create and drop their own database `lh_test_<random>`. The server is disposable: tests may create and drop databases freely.
- Timestamps are stored as `DateTime64(9, 'UTC')`; Go side always uses `time.Time` in UTC with nanoseconds.
- `stream_id` is **always** `labels.StableHash(ls)` of the canonical (sorted) stream labels. This equals Loki's stream hash and drives tie-break ordering.
- Limits come from Loki's `validation.Limits` / `validation.Overrides` with Loki's flag defaults. The `limits_config` YAML block is Loki-compatible.
- Code copied from Loki keeps its original copyright header and gets `// Copied from grafana/loki@fbc3f077 <path>; modifications: <list>` on top.
- No operators, Helm, rings, ingesters, caches, or Loki-side sharding. Horizontal scale and HA come from ClickHouse alone.
- ClickHouse topology is configuration, not a code path:
  - `clickhouse.cluster` empty: plain tables `logs`/`streams`.
  - `clickhouse.cluster` set: `<table>_local` tables are created `ON CLUSTER`. They are `Replicated*MergeTree` unless `clickhouse.replicated: false`. `Distributed` tables named `logs`/`streams` sit on top, sharded by `cityHash64(tenant, stream_id)`.
  - Query and insert SQL names only `logs`/`streams`. DDL and mutations use `Topology.OnCluster()` and `Topology.Local()`.
- Every ClickHouse connection carries `Topology.Settings()`, overlaid with `clickhouse.settings` from the config:
  - `distributed_foreground_insert=1`
  - `insert_quorum='auto'` when replicated

  A push is acknowledged only once all shards hold the rows and a replica quorum has them.
- Never set `skip_unavailable_shards`. A missing shard must be a query error, not a silently partial result.
- Never write `IN (SELECT … FROM <Distributed table>)`. ClickHouse rejects it by default. Pass stream ids as a literal list (Phase 1), or use `streams_local`, which is co-located by the shared sharding key.

## Review Focus

1. **Duplicate entries** (same stream, same ts, same line and metadata, pushed twice): Loki returns them once. Owner: Task 6 (`TestSelectLogs_DedupesIdenticalEntries`).
2. **Unordered / non-canonical stream labels in push** (`{b="2",a="1"}` vs `{a="1", b="2"}`): one stream, one `stream_id`, labels printed canonically. Owner: Task 3 (`TestStreamIdentity_CanonicalOrder`).
3. **Entries exactly at query `start` / `end` boundaries**: `start` inclusive, `end` exclusive, as in Loki's chunk iterators. Owner: Task 6 (`TestSelectLogs_TimeBoundaries`).
4. **Service-name and log-level discovery on push** (`service_name`, `detected_level`): Loki adds them by default, so lokihouse must too, or every stream differs from the oracle. Owner: Task 4 (`TestPush_DiscoversServiceNameAndLevel`).
5. **Tenant isolation and missing `X-Scope-OrgID`**: with `auth_enabled: true` the request gets 401 `no org id`. With `false` the tenant is `fake`. Data never crosses tenants. Owner: Task 8 (`TestAPI_TenantIsolation`).
6. **Query immediately after push on a replicated cluster**: the entry is visible, whichever replica serves the read, because the push is acked after quorum and all shards. Owner: Task 12 (`TestCluster_ReadAfterPush`).
7. **Duplicates and stream metadata across shards**: all rows of one (tenant, stream), in both `logs` and `streams`, land on one shard. This keeps dedup and the label/series results correct. Owner: Task 12 (`TestCluster_StreamColocation`), plus the whole suite under `make test-cluster`.

---

## File Structure

```
go.mod / go.sum                         module + Loki pin + replaces
Makefile                                build, test, oracle targets
LICENSE                                 AGPL-3.0 (from Loki)
cmd/lokihouse/main.go                   flag/config parsing, wiring, HTTP server
internal/config/config.go               YAML config (server, clickhouse, auth_enabled, limits_config)
internal/config/config_test.go
internal/chstore/schema.sql             embedded DDL (templated by Topology)
internal/chstore/schema.go              RenderSchema(), Migrate(), DropDatabase()
internal/chstore/topology.go            Topology: single node / ON CLUSTER / Replicated / Distributed
internal/chstore/cluster_test.go        cluster-only tests (skipped on single node)
internal/chstore/chtest/chtest.go       test helper: throwaway database per test
internal/chstore/stream.go              StreamIdentity(): canonical labels + stream_id
internal/chstore/writer.go              Writer.Write(): insert logs + streams rows
internal/chstore/sqlbuild.go            matchers / line filters → SQL fragments (pure)
internal/chstore/streams.go             ResolveStreams(): selector → candidate streams
internal/chstore/querier.go             logql.Querier: SelectLogs / SelectSamples
internal/chstore/rowiter.go             row → entry iterator with dedup
internal/chstore/metadata.go            Labels / LabelValues / Series
internal/ingest/distcopy/*.go           ported Loki distributor validation + field detection
internal/ingest/handler.go              /loki/api/v1/push, /otlp/v1/logs, /api/prom/push
internal/api/api.go                     router + tenant middleware
internal/api/query.go                   query, query_range
internal/api/metadata.go                labels, label values, series
internal/api/misc.go                    ready, buildinfo
test/oracle/                            differential harness vs real Loki
test/oracle/corpus/*.logql              query corpus
test/cluster/                           podman 2x2 ClickHouse cluster + Keeper for test-cluster/oracle-cluster
```

---

### Task 1: Module bootstrap with pinned Loki import

**Files:**
- Create: `go.mod`, `Makefile`, `LICENSE`, `internal/lokidep/lokidep_test.go`, `.gitignore`

**Interfaces:**
- Consumes: nothing
- Produces: a buildable module; `make test` runs `go test ./...`

- [ ] **Step 1: Write the failing smoke test**

`internal/lokidep/lokidep_test.go`:
```go
package lokidep

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

// Guards that the pinned Loki LogQL parser is importable and behaves as expected.
func TestLokiParserImport(t *testing.T) {
	e, err := syntax.ParseExpr(`sum by (app) (rate({app="a"} |= "x" | json [5m]))`)
	require.NoError(t, err)
	require.Equal(t, `sum by (app)(rate({app="a"} |= "x" | json[5m]))`, e.String())
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/lokidep/...`
Expected: FAIL with `go: cannot find main module` (no go.mod yet).

- [ ] **Step 3: Create go.mod with the Loki pin and replaces**

```bash
go mod init github.com/bzed/lokihouse
cat >> go.mod <<'EOF'

replace github.com/Azure/azure-sdk-for-go => github.com/Azure/azure-sdk-for-go v68.0.0+incompatible
replace github.com/Azure/azure-sdk-for-go/sdk/storage/azblob => github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.0
replace github.com/Azure/azure-storage-blob-go => github.com/MasslessParticle/azure-storage-blob-go v0.14.1-0.20240322194317-344980fda573
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85
replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.3.1-0.20260921102813-8c522db348af
replace github.com/grafana/regexp => github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853
replace github.com/influxdata/go-syslog/v3 => github.com/leodido/go-syslog/v4 v4.6.0
replace github.com/thanos-io/objstore => github.com/grafana/objstore v0.0.0-20260721130030-becd33cc63ff
replace github.com/twmb/franz-go => github.com/twmb/franz-go v1.21.7
replace github.com/twmb/franz-go/pkg/kadm => github.com/twmb/franz-go/pkg/kadm v1.18.0
replace github.com/twmb/franz-go/pkg/kmsg => github.com/twmb/franz-go/pkg/kmsg v1.13.1
replace github.com/grafana/loki/pkg/push => github.com/grafana/loki/pkg/push v0.0.0-20261010092436-fbc3f0770b8c
EOF
GOFLAGS=-mod=mod go get github.com/grafana/loki/v3@fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1 \
  github.com/ClickHouse/clickhouse-go/v2@v2.49.0 github.com/stretchr/testify@latest
go mod tidy
```

`Makefile`:
```make
LOKI_COMMIT := fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1
.PHONY: build test oracle
build:
	go build -o bin/lokihouse ./cmd/lokihouse
test:
	go test ./...
oracle:
	go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
```

`.gitignore`: `bin/`, `.cache/`.
LICENSE: `curl -fsSL https://raw.githubusercontent.com/grafana/loki/fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1/LICENSE -o LICENSE`

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/lokidep/...` (the first build takes ~2 min)
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum Makefile LICENSE .gitignore internal/lokidep
git commit -m "build: bootstrap module importing Loki at pinned commit"
```

---

### Task 2: ClickHouse schema, cluster topology, migrations and test helper

**Files:**
- Create: `internal/chstore/schema.sql`, `internal/chstore/schema.go`, `internal/chstore/topology.go`, `internal/chstore/chtest/chtest.go`, `internal/chstore/schema_test.go`, `internal/chstore/topology_test.go`

**Interfaces:**
- Consumes: Task 1 module
- Produces:
  - `type Topology struct{ Cluster string; Replicated bool; ReplicaPath, ReplicaName string }`. The zero value means a single server with plain MergeTree tables. Its methods are:
    - `OnCluster() string`: "" or ``ON CLUSTER `c` ``
    - `Local(name) string`: `name` or `name_local`
    - `Engine(base) string`: `base` or `Replicated<base>[(path, name)]`
    - `Settings() clickhouse.Settings`: session settings every connection must carry
  - `func RenderSchema(db string, topo Topology) ([]string, error)` (pure)
  - `func Migrate(ctx context.Context, conn driver.Conn, db string, topo Topology) error`. It is idempotent, and safe when several lokihouse instances start at once.
  - `func DropDatabase(ctx context.Context, conn driver.Conn, db string, topo Topology) error`
  - `func chtest.Topology() chstore.Topology`. It reads `LOKIHOUSE_TEST_CH_CLUSTER`, which is empty by default. With a cluster set, tables are replicated unless `LOKIHOUSE_TEST_CH_REPLICATED=0`.
  - `func chtest.Options(db string) *clickhouse.Options` includes `Topology().Settings()`
  - `func chtest.New(t *testing.T) (conn driver.Conn, db string)` returns a connection whose default database is a fresh, migrated database. The database is dropped on cleanup, on every node.
  - Tables:
    - single server: `logs`, `streams`
    - cluster: `logs_local`, `streams_local` (the data), plus `Distributed` tables `logs` and `streams` sharded by `cityHash64(tenant, stream_id)`

  All query and insert code (Tasks 3–11) names only `logs` and `streams`, so it runs unchanged on both topologies.

Why this layout: `ReplacingMergeTree` only collapses duplicates that sit on the same shard, and `stream_id IN (SELECT … FROM streams_local)` is only correct if a stream's rows and its `streams` rows are co-located. Both tables therefore use the **same** sharding key. On ClickHouse 26.9, `Replicated*MergeTree` without arguments uses the server's `default_replica_path` (`/clickhouse/tables/{uuid}/{shard}`), and `ON CLUSTER` propagates the table UUID. No Keeper path configuration is needed unless the operator wants one. All of this was prototyped on a 2-shard × 2-replica podman cluster: replication, co-location, `FINAL` through `Distributed`, async inserts with quorum, the text index, `DELETE … ON CLUSTER` and an MV on `logs_local`.

- [ ] **Step 1: Write the failing tests**

`internal/chstore/topology_test.go` (pure, no server):
```go
package chstore_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
)

func TestTopology_SingleNode(t *testing.T) {
	var topo chstore.Topology
	require.Equal(t, "", topo.OnCluster())
	require.Equal(t, "logs", topo.Local("logs"))
	require.Equal(t, "ReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Empty(t, topo.Settings())
}

func TestTopology_ReplicatedCluster(t *testing.T) {
	topo := chstore.Topology{Cluster: "lokihouse", Replicated: true}
	require.Equal(t, "ON CLUSTER `lokihouse`", topo.OnCluster())
	require.Equal(t, "logs_local", topo.Local("logs"))
	require.Equal(t, "ReplicatedReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Equal(t, 1, topo.Settings()["distributed_foreground_insert"])
	require.Equal(t, "auto", topo.Settings()["insert_quorum"])

	topo.ReplicaPath, topo.ReplicaName = "/clickhouse/tables/{shard}/{database}/{table}", "{replica}"
	require.Equal(t, "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
		topo.Engine("ReplacingMergeTree"))
}

func TestTopology_ShardedWithoutReplication(t *testing.T) {
	topo := chstore.Topology{Cluster: "c"}
	require.Equal(t, "ReplacingMergeTree", topo.Engine("ReplacingMergeTree"))
	require.Equal(t, "streams_local", topo.Local("streams"))
	require.NotContains(t, topo.Settings(), "insert_quorum")
}

func TestRenderSchema_SingleNode(t *testing.T) {
	stmts, err := chstore.RenderSchema("db", chstore.Topology{})
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	all := strings.Join(stmts, "\n")
	require.Contains(t, all, "CREATE TABLE IF NOT EXISTS `db`.logs (")
	require.NotContains(t, all, "ON CLUSTER")
	require.NotContains(t, all, "Distributed")
}

func TestRenderSchema_Cluster(t *testing.T) {
	stmts, err := chstore.RenderSchema("db", chstore.Topology{Cluster: "lokihouse", Replicated: true})
	require.NoError(t, err)
	require.Len(t, stmts, 4)
	require.Contains(t, stmts[0], "CREATE TABLE IF NOT EXISTS `db`.logs_local ON CLUSTER `lokihouse` (")
	require.Contains(t, stmts[0], "ENGINE = ReplicatedReplacingMergeTree")
	require.Contains(t, stmts[2], "ENGINE = Distributed('lokihouse', 'db', 'logs_local', cityHash64(tenant, stream_id))")
	require.Contains(t, stmts[3], "ENGINE = Distributed('lokihouse', 'db', 'streams_local', cityHash64(tenant, stream_id))")
}
```

`internal/chstore/schema_test.go` (needs a server; follows `chtest.Topology()`):
```go
package chstore_test

import (
	"context"
	"testing"

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
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/chstore/ -run 'TestMigrate|TestTopology|TestRenderSchema'`
Expected: FAIL, `undefined: chstore.Topology`

- [ ] **Step 3: Implement**

`internal/chstore/schema.sql`. Statements are separated by `;` at the end of a line. `{{if .Cluster}}` adds the `Distributed` tables.
```sql
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "logs"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant              LowCardinality(String),
  stream_id           UInt64,
  ts                  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  line                String CODEC(ZSTD(3)),
  structured_metadata Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  line_hash           UInt64 MATERIALIZED cityHash64(line),
  retention_days      UInt16 DEFAULT 31,
  INDEX line_text line TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY toDate(ts)
ORDER BY (tenant, stream_id, ts, line_hash)
TTL toDateTime(ts) + toIntervalDay(retention_days);

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "streams"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant     LowCardinality(String),
  day        Date,
  stream_id  UInt64,
  labels     Map(LowCardinality(String), String),
  labels_str String
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY toYYYYMM(day)
ORDER BY (tenant, day, stream_id);
{{if .Cluster}}
CREATE TABLE IF NOT EXISTS {{.DBName}}.logs{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.logs_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'logs_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.streams{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.streams_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'streams_local', cityHash64(tenant, stream_id));
{{end}}
```

`internal/chstore/topology.go`:
```go
package chstore

import (
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Topology describes how lokihouse tables are laid out on ClickHouse.
// The zero value is a single server with plain MergeTree tables.
type Topology struct {
	Cluster     string // remote_servers cluster name; empty = single server
	Replicated  bool   // use Replicated*MergeTree engines (needs Keeper)
	ReplicaPath string // optional Keeper path; empty = server's default_replica_path
	ReplicaName string // optional replica name; empty = server's default_replica_name
}

// OnCluster returns the ON CLUSTER clause for DDL and mutations, or "".
func (t Topology) OnCluster() string {
	if t.Cluster == "" {
		return ""
	}
	return "ON CLUSTER " + quoteIdent(t.Cluster)
}

// Local returns the name of the table that physically stores rows.
// On a cluster that is <name>_local; <name> itself is the Distributed table.
func (t Topology) Local(name string) string {
	if t.Cluster == "" {
		return name
	}
	return name + "_local"
}

// Engine returns the storage engine clause for a MergeTree-family engine.
func (t Topology) Engine(base string) string {
	if !t.Replicated {
		return base
	}
	if t.ReplicaPath == "" {
		return "Replicated" + base
	}
	return "Replicated" + base + "(" + sqlString(t.ReplicaPath) + ", " + sqlString(t.ReplicaName) + ")"
}

// Settings returns the session settings every lokihouse connection needs
// for this topology: pushes are acknowledged only once all shards and an
// insert quorum of replicas hold the rows, so a query right after a push sees them.
func (t Topology) Settings() clickhouse.Settings {
	s := clickhouse.Settings{}
	if t.Cluster != "" {
		s["distributed_foreground_insert"] = 1
		s["distributed_ddl_output_mode"] = "throw"
	}
	if t.Replicated {
		s["insert_quorum"] = "auto"
		s["insert_quorum_parallel"] = 1
	}
	return s
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
```
Values in `Settings()` go over the native protocol as strings, so `"auto"` is valid for `insert_quorum`. If clickhouse-go v2.49 rejects the type, wrap it with `clickhouse.CustomSetting{Value: "auto"}`.

`internal/chstore/schema.go`:
```go
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
	return nil
}

// DropDatabase removes db on every node of the topology. Used by tests.
func DropDatabase(ctx context.Context, conn driver.Conn, db string, topo Topology) error {
	return conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s %s SYNC", quoteIdent(db), topo.OnCluster()))
}
```

`internal/chstore/chtest/chtest.go`:
```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/chstore/ -run 'TestMigrate|TestTopology|TestRenderSchema' -v`
Expected: PASS. The topology and render tests need no server. The migrate tests run against the single local server. Task 12 runs the same tests against the podman cluster.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore
git commit -m "feat(chstore): schema with single-node/cluster topology, migrations, test database helper"
```

---

### Task 3: Stream identity and the ClickHouse writer

**Files:**
- Create: `internal/chstore/stream.go`, `internal/chstore/writer.go`, `internal/chstore/writer_test.go`

**Interfaces:**
- Consumes: `chtest.New`, tables from Task 2
- Produces:
  - `func StreamIdentity(lbls string) (labels.Labels, uint64, error)` parses with `syntax.ParseLabels` and returns sorted labels plus `labels.StableHash`
  - `type Writer struct{ Conn driver.Conn }`
  - `func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream, retentionDays func(labels.Labels) uint16) error` inserts every entry into `logs` and one `streams` row per (stream, UTC day touched). A nil `retentionDays` means 31.

- [ ] **Step 1: Write the failing tests**

`internal/chstore/writer_test.go`:
```go
package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestStreamIdentity_CanonicalOrder(t *testing.T) {
	a, ha, err := chstore.StreamIdentity(`{b="2", a="1"}`)
	require.NoError(t, err)
	b, hb, err := chstore.StreamIdentity(`{a="1",b="2"}`)
	require.NoError(t, err)
	require.Equal(t, ha, hb)
	require.Equal(t, `{a="1", b="2"}`, a.String())
	require.Equal(t, a.String(), b.String())
	require.Equal(t, labels.StableHash(labels.FromStrings("a", "1", "b", "2")), ha)
}

func TestWriter_RoundTrip(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	// Yesterday 23:59:59.123456789 UTC: recent enough for the 31-day TTL (ClickHouse drops
	// already-expired rows at insert time), and the second entry crosses midnight.
	ts := time.Now().UTC().Truncate(24 * time.Hour).Add(-time.Second + 123456789)
	err := w.Write(ctx, "t1", []logproto.Stream{{
		Labels: `{job="x", app="y"}`,
		Entries: []logproto.Entry{
			{Timestamp: ts, Line: "hello", StructuredMetadata: logproto.FromLabelsToLabelAdapters(labels.FromStrings("trace_id", "abc"))},
			{Timestamp: ts.Add(2 * time.Second), Line: "next day"},
		},
	}}, nil)
	require.NoError(t, err)

	var got time.Time
	var line, trace string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT ts, line, structured_metadata['trace_id'] FROM logs WHERE tenant='t1' ORDER BY ts LIMIT 1").Scan(&got, &line, &trace))
	require.True(t, ts.Equal(got), "nanosecond precision preserved: %v vs %v", ts, got)
	require.Equal(t, "hello", line)
	require.Equal(t, "abc", trace)

	var days uint64
	var lstr string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(), any(labels_str) FROM streams FINAL WHERE tenant='t1'").Scan(&days, &lstr))
	require.Equal(t, uint64(2), days, "one streams row per UTC day touched")
	require.Equal(t, `{app="y", job="x"}`, lstr)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/chstore/ -run 'TestStreamIdentity|TestWriter'`
Expected: FAIL, `undefined: chstore.StreamIdentity`

- [ ] **Step 3: Implement**

`internal/chstore/stream.go`:
```go
package chstore

import (
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// StreamIdentity canonicalises a stream label string and returns its Loki-compatible hash.
func StreamIdentity(lbls string) (labels.Labels, uint64, error) {
	ls, err := syntax.ParseLabels(lbls)
	if err != nil {
		return labels.EmptyLabels(), 0, err
	}
	return ls, labels.StableHash(ls), nil
}
```

`internal/chstore/writer.go`:
```go
package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

const defaultRetentionDays = 31

type Writer struct {
	Conn driver.Conn
}

type streamDay struct {
	id  uint64
	day time.Time
}

// Write inserts entries into logs and registers each stream for every UTC day it touches.
func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream, retentionDays func(labels.Labels) uint16) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert": 1, "wait_for_async_insert": 1,
	}))
	logs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO logs (tenant, stream_id, ts, line, structured_metadata, retention_days)")
	if err != nil {
		return fmt.Errorf("prepare logs batch: %w", err)
	}
	defer logs.Abort()
	strs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO streams (tenant, day, stream_id, labels, labels_str)")
	if err != nil {
		return fmt.Errorf("prepare streams batch: %w", err)
	}
	defer strs.Abort()

	seen := map[streamDay]struct{}{}
	for _, s := range streams {
		ls, id, err := StreamIdentity(s.Labels)
		if err != nil {
			return fmt.Errorf("stream labels %q: %w", s.Labels, err)
		}
		ret := uint16(defaultRetentionDays)
		if retentionDays != nil {
			ret = retentionDays(ls)
		}
		lmap := ls.Map()
		lstr := ls.String()
		for _, e := range s.Entries {
			md := make(map[string]string, len(e.StructuredMetadata))
			for _, kv := range e.StructuredMetadata {
				md[kv.Name] = kv.Value
			}
			ts := e.Timestamp.UTC()
			if err := logs.Append(tenant, id, ts, e.Line, md, ret); err != nil {
				return err
			}
			key := streamDay{id: id, day: ts.Truncate(24 * time.Hour)}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				if err := strs.Append(tenant, key.day, id, lmap, lstr); err != nil {
					return err
				}
			}
		}
	}
	if err := logs.Send(); err != nil {
		return fmt.Errorf("insert logs: %w", err)
	}
	return strs.Send()
}
```

Note: `streams` is written after `logs`, so a reader never sees a stream without data, only data whose stream is briefly missing. That is invisible because queries resolve streams first.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/chstore/ -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add internal/chstore
git commit -m "feat(chstore): canonical stream identity and batch writer"
```

---

### Task 4: Ingest: port Loki distributor validation and field detection, push handler

**Files:**
- Create: `internal/ingest/distcopy/validator.go`, `internal/ingest/distcopy/field_detection.go`, `internal/ingest/distcopy/process.go`, `internal/ingest/handler.go`, `internal/ingest/handler_test.go`

**Interfaces:**
- Consumes: `chstore.Writer`, Loki `push.ParseRequest`, `push.ParseLokiRequest`, `push.NewOTLPRequestParser(false)`, `validation.Overrides`
- Produces:
  - `func distcopy.Prepare(ctx context.Context, limits *validation.Overrides, tenant string, req *logproto.InternalPushRequest, now time.Time) (streams []logproto.Stream, validationErr error)` returns validated, normalised streams with discovered fields applied. `validationErr` is non-nil when any entry or stream was rejected, and its message matches Loki's text.
  - `type ingest.Handler struct{ Writer *chstore.Writer; Limits *validation.Overrides; Logger log.Logger }`
  - `func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request)` serves `/loki/api/v1/push`, `/api/prom/push` and `/otlp/v1/logs`. The tenant comes from the request context (`tenant.TenantID`), which Task 8's middleware injects. It returns 204 on success and 400 with Loki's error body when anything was rejected, writing the accepted entries anyway, as Loki does.

Porting instructions. These are *verbatim copies*. Keep the logic identical and delete only what is listed.
1. Copy `pkg/distributor/validator.go` → `distcopy/validator.go`, package `distcopy`. Replace the embedded `Limits` interface type with `*validation.Overrides`; it implements every method used. Delete `reportDiscardedData*` bodies (make them no-ops) and the `usageTracker` field.
2. Copy `pkg/distributor/field_detection.go` → `distcopy/field_detection.go` unchanged except the package name.
3. Into `distcopy/process.go`, copy these from `pkg/distributor/distributor.go`: `processStreamEntries`, `normalizeMetadataGroup`, `normalizeStructuredMetadata`, `parseStreamLabels`, `metadataGroup`, `newMetadataGroup`, `duplicateTimestampTracker` and its methods. Turn every `(d *Distributor)` method into a function on a small `type preparer struct{ v *Validator; logger log.Logger }`. Remove rate-limiting, `writeFailuresManager`, metrics, the usage tracker, ingester/ring and Kafka code paths. Keep validation error aggregation exactly as Loki does it: the `validationErrors` multierror and the final `fmt.Errorf(validation.ErrFormat...)` style message.
4. `Prepare` builds `validationContext` with `v.getValidationContextForTime(now, tenant)`, loops streams like the top of `Distributor.PushWithResolver` (the code that calls `parseStreamLabels` at `distributor.go:699` and `processStreamEntries`), and converts every kept `InternalStreamAdapter` with `.FlatView()` into `logproto.Stream`.

- [ ] **Step 1: Write the failing handler tests**

`internal/ingest/handler_test.go`:
```go
package ingest_test

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
)

func defaultLimits(t *testing.T) *validation.Overrides {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func push(t *testing.T, h http.Handler, tenant, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(user.InjectOrgID(req.Context(), tenant))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPush_DiscoversServiceNameAndLevel(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(ts) + `","level=error msg=boom"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	var lstr, lvl string
	ctx := context.Background()
	require.NoError(t, conn.QueryRow(ctx, "SELECT labels_str FROM streams WHERE tenant='t1'").Scan(&lstr))
	require.Equal(t, `{app="web", service_name="web"}`, lstr)
	require.NoError(t, conn.QueryRow(ctx, "SELECT structured_metadata['detected_level'] FROM logs WHERE tenant='t1'").Scan(&lvl))
	require.Equal(t, "error", lvl)
}

func TestPush_RejectsTooOldAndKeepsRest(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	old := time.Now().Add(-30 * 24 * time.Hour).UnixNano()
	ok := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(old) + `","old"],["` + itoa(ok) + `","new"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "timestamp too old")

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant='t1'").Scan(&n))
	require.Equal(t, uint64(1), n)
}

func TestPush_RejectsInvalidLabels(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	rec := push(t, h, "t1", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/ingest/...`
Expected: FAIL, `undefined: ingest.Handler`

- [ ] **Step 3: Port distcopy (steps 1–4 above) and write the handler**

`internal/ingest/handler.go`:
```go
package ingest

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/ingest/distcopy"
)

const (
	maxRecvMsgSize      = 100 << 20 // Loki's distributor default (-distributor.max-recv-msg-size)
	maxDecompressedSize = 5000 << 20
)

type Handler struct {
	Writer *chstore.Writer
	Limits *validation.Overrides
	Logger log.Logger
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	parser, format := push.RequestParser(push.ParseLokiRequest), "loki"
	if strings.HasPrefix(r.URL.Path, "/otlp/") {
		parser, format = push.NewOTLPRequestParser(false), "otlp"
	}
	req, _, err := push.ParseRequest(h.Logger, tenantID, maxRecvMsgSize, maxDecompressedSize, r,
		h.Limits, nil, parser, nil, nil, "", format)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	streams, verr := distcopy.Prepare(r.Context(), h.Limits, tenantID, req, time.Now())
	if len(streams) > 0 {
		if err := h.Writer.Write(r.Context(), tenantID, streams, nil); err != nil {
			level.Error(h.Logger).Log("msg", "write failed", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if verr != nil {
		http.Error(w, verr.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

If `push.ParseRequest` panics on a nil `tenantConfigs`, `tracker` or `streamResolver`, pass `&runtime.TenantConfigs{}` (`runtime.NewTenantConfigs(nil)`) and a resolver struct that returns `h.Limits.RetentionPeriod(tenantID)`, its hours string and `""` for the policy.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/ingest/... -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add internal/ingest
git commit -m "feat(ingest): Loki-compatible push (JSON/proto/OTLP) with ported validation and field detection"
```

---

### Task 5: SQL builder for selectors and leading line filters (pure)

**Files:**
- Create: `internal/chstore/sqlbuild.go`, `internal/chstore/sqlbuild_test.go`

**Interfaces:**
- Consumes: Loki `syntax.LogSelectorExpr`, `*labels.Matcher`, `syntax.LineFilterExpr`
- Produces:
  - `type Fragment struct{ SQL string; Args []any }`
  - `func StreamMatcherSQL(ms []*labels.Matcher) (sql Fragment, goSide []*labels.Matcher)`: equality and inequality matchers become SQL on `labels[...]`. Regex matchers are returned in `goSide` and evaluated in Go (spec §7.2).
  - `func LeadingLineFilterSQL(expr syntax.LogSelectorExpr) Fragment` builds an AND of the *case-sensitive literal* `|=` / `!=` filters, `or` chains included, that come before any other stage. It returns an empty fragment when there are none.

Semantics. Loki treats a missing label as `""`, and ClickHouse `labels['x']` on a missing key also returns `''`, so `=` and `!=` map directly. Pushed filters are only ever necessary conditions, because Loki's pipeline re-applies them.

- [ ] **Step 1: Write the failing tests**

`internal/chstore/sqlbuild_test.go`:
```go
package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestStreamMatcherSQL(t *testing.T) {
	ms, err := syntax.ParseMatchers(`{app="a", env!="prod", pod=~"x.*", ns!~"kube-.*"}`, true)
	require.NoError(t, err)
	f, goSide := StreamMatcherSQL(ms)
	require.Equal(t, "labels[?] = ? AND labels[?] != ?", f.SQL)
	require.Equal(t, []any{"app", "a", "env", "prod"}, f.Args)
	require.Len(t, goSide, 2)
}

func TestLeadingLineFilterSQL(t *testing.T) {
	cases := map[string]Fragment{
		`{a="b"}`:                                 {},
		`{a="b"} |= "x"`:                          {SQL: "position(line, ?) > 0", Args: []any{"x"}},
		`{a="b"} |= "x" != "y"`:                   {SQL: "position(line, ?) > 0 AND position(line, ?) = 0", Args: []any{"x", "y"}},
		`{a="b"} |= "x" or "z"`:                   {SQL: "(position(line, ?) > 0 OR position(line, ?) > 0)", Args: []any{"x", "z"}},
		`{a="b"} != "x" or "z"`:                   {SQL: "position(line, ?) = 0 AND position(line, ?) = 0", Args: []any{"x", "z"}},
		`{a="b"} |~ "x"`:                          {}, // regex never pushed in Phase 1
		`{a="b"} |= "x" | json |= "y"`:            {SQL: "position(line, ?) > 0", Args: []any{"x"}},
		`{a="b"} | json |= "y"`:                   {},
		`{a="b"} |= ip("10.0.0.0/8")`:             {},
		`{a="b"} |= "x" |~ "r" |= "y"`:            {SQL: "position(line, ?) > 0 AND position(line, ?) > 0", Args: []any{"x", "y"}},
	}
	for q, want := range cases {
		t.Run(q, func(t *testing.T) {
			e, err := syntax.ParseLogSelector(q, true)
			require.NoError(t, err)
			require.Equal(t, want, LeadingLineFilterSQL(e))
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/chstore/ -run 'TestStreamMatcherSQL|TestLeadingLineFilterSQL'`
Expected: FAIL, undefined functions

- [ ] **Step 3: Implement**

`internal/chstore/sqlbuild.go`:
```go
package chstore

import (
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

type Fragment struct {
	SQL  string
	Args []any
}

func (f Fragment) Empty() bool { return f.SQL == "" }

func and(parts []Fragment) Fragment {
	var out Fragment
	var sqls []string
	for _, p := range parts {
		if p.Empty() {
			continue
		}
		sqls = append(sqls, p.SQL)
		out.Args = append(out.Args, p.Args...)
	}
	out.SQL = strings.Join(sqls, " AND ")
	return out
}

// StreamMatcherSQL turns equality matchers into SQL over streams.labels; regex matchers stay in Go.
func StreamMatcherSQL(ms []*labels.Matcher) (Fragment, []*labels.Matcher) {
	var parts []Fragment
	var goSide []*labels.Matcher
	for _, m := range ms {
		switch m.Type {
		case labels.MatchEqual:
			parts = append(parts, Fragment{"labels[?] = ?", []any{m.Name, m.Value}})
		case labels.MatchNotEqual:
			parts = append(parts, Fragment{"labels[?] != ?", []any{m.Name, m.Value}})
		default:
			goSide = append(goSide, m)
		}
	}
	return and(parts), goSide
}

// LeadingLineFilterSQL returns a necessary SQL condition for the literal line filters that
// precede every non-filter stage. Loki's pipeline re-applies all filters exactly.
func LeadingLineFilterSQL(expr syntax.LogSelectorExpr) Fragment {
	p, ok := expr.(*syntax.PipelineExpr)
	if !ok {
		return Fragment{}
	}
	var parts []Fragment
	for _, st := range p.MultiStages {
		lf, ok := st.(*syntax.LineFilterExpr)
		if !ok {
			break
		}
		for cur := lf; cur != nil; cur = cur.Left {
			parts = append(parts, lineFilterSQL(cur))
		}
	}
	return and(parts)
}

// lineFilterSQL handles one LineFilterExpr node including its Or chain (not its Left chain).
func lineFilterSQL(f *syntax.LineFilterExpr) Fragment {
	if f.Op != "" { // ip() and other functions: not pushed
		return Fragment{}
	}
	var alts []*syntax.LineFilterExpr
	for o := f; o != nil; o = o.Or {
		if o.Op != "" {
			return Fragment{}
		}
		alts = append(alts, o)
	}
	switch f.Ty {
	case log.LineMatchEqual:
		if len(alts) == 1 {
			return Fragment{"position(line, ?) > 0", []any{f.Match}}
		}
		var sqls []string
		var args []any
		for _, a := range alts {
			sqls = append(sqls, "position(line, ?) > 0")
			args = append(args, a.Match)
		}
		return Fragment{"(" + strings.Join(sqls, " OR ") + ")", args}
	case log.LineMatchNotEqual:
		var parts []Fragment
		for _, a := range alts {
			parts = append(parts, Fragment{"position(line, ?) = 0", []any{a.Match}})
		}
		return and(parts)
	default:
		return Fragment{}
	}
}
```

The order of the `Left` chain is not guaranteed to be source order. If the multi-filter test case fails only on order, sort `parts` back into source order by walking `Left` to its root first and then appending forward.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/chstore/ -run 'TestStreamMatcherSQL|TestLeadingLineFilterSQL' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/sqlbuild*.go
git commit -m "feat(chstore): SQL fragments for stream matchers and leading literal line filters"
```

---

### Task 6: `logql.Querier` on ClickHouse: SelectLogs and SelectSamples

**Files:**
- Create: `internal/chstore/streams.go`, `internal/chstore/rowiter.go`, `internal/chstore/querier.go`, `internal/chstore/querier_test.go`

**Interfaces:**
- Consumes: `StreamMatcherSQL`, `LeadingLineFilterSQL`, `Writer`, `chtest.New`
- Produces:
  - `type Stream struct{ ID uint64; Labels labels.Labels }`
  - `func ResolveStreams(ctx context.Context, conn driver.Conn, tenant string, ms []*labels.Matcher, from, through time.Time) ([]Stream, error)`
  - `type Querier struct{ Conn driver.Conn }` implements `logql.Querier`
  - Row ordering contract. FORWARD is `ORDER BY ts ASC, stream_id ASC, line_hash`. BACKWARD is `ORDER BY ts DESC, stream_id ASC, line_hash`. This matches Loki's `lessAscending` / `lessDescending`.
  - Dedup contract. Rows with equal (ts, stream_id, line, structured metadata) are yielded once.
  - Time contract: `ts >= Start AND ts < End`.

- [ ] **Step 1: Write the failing tests**

`internal/chstore/querier_test.go`:
```go
package chstore_test

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// Yesterday 12:00 UTC. Must stay within the 31-day TTL: ClickHouse drops already-expired rows at insert time.
var t0 = time.Now().UTC().Truncate(24 * time.Hour).Add(-12 * time.Hour)

func seed(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams, nil))
}

func engine(t *testing.T, q logql.Querier) *logql.QueryEngine {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return logql.NewEngine(logql.EngineOpts{}, q, o, log.NewNopLogger())
}

func run(t *testing.T, ng *logql.QueryEngine, qs string, start, end time.Time, step time.Duration, dir logproto.Direction, limit uint32) logqlmodel.Result {
	p, err := logql.NewLiteralParams(qs, start, end, step, 0, dir, limit, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	res, err := ng.Query(p).Exec(ctx)
	require.NoError(t, err)
	return res
}

func TestSelectLogs_FilterAndDirection(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: t0, Line: "keep 1"}, {Timestamp: t0.Add(time.Second), Line: "drop"}, {Timestamp: t0.Add(2 * time.Second), Line: "keep 2"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep other app"}}})
	seed(t, w, "t2", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep foreign tenant"}}})

	ng := engine(t, &chstore.Querier{Conn: conn})
	res := run(t, ng, `{app="a"} |= "keep"`, t0, t0.Add(time.Minute), 0, logproto.BACKWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="a"}`, streams[0].Labels)
	require.Equal(t, []string{"keep 2", "keep 1"}, lines(streams[0]))
}

func TestSelectLogs_TimeBoundaries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(-time.Nanosecond), Line: "before"}, {Timestamp: t0, Line: "start"},
		{Timestamp: t0.Add(time.Minute - time.Nanosecond), Line: "last"}, {Timestamp: t0.Add(time.Minute), Line: "end"}}})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"start", "last"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectLogs_DedupesIdenticalEntries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	s := logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "same"}}}
	seed(t, w, "t1", s)
	seed(t, w, "t1", s)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"same"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectLogs_RegexSelectorAndParser(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="api-1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}, {Timestamp: t0.Add(time.Second), Line: `{"status":200}`}}},
		logproto.Stream{Labels: `{app="web"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}}})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app=~"api-.*"} | json | status="500"`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="api-1", status="500"}`, streams[0].Labels)
}

func TestSelectSamples_CountOverTime(t *testing.T) {
	conn, _ := chtest.New(t)
	var es []logproto.Entry
	for i := 0; i < 10; i++ {
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * 10 * time.Second), Line: "x"})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `sum(count_over_time({app="a"}[1m]))`, t0.Add(time.Minute), t0.Add(2*time.Minute), time.Minute, logproto.FORWARD, 0)
	m := res.Data.(promql.Matrix)
	require.Len(t, m, 1)
	// entries at t0+0s..90s; Loki windows are left-open: (t0, t0+1m] = 10s..60s, (t0+1m, t0+2m] = 70s..90s
	require.Equal(t, 6.0, m[0].Floats[0].F)
	require.Equal(t, 3.0, m[0].Floats[1].F)
}

func lines(s logproto.Stream) []string {
	var out []string
	for _, e := range s.Entries {
		out = append(out, e.Line)
	}
	return out
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/chstore/ -run 'TestSelect'`
Expected: FAIL, `undefined: chstore.Querier`

- [ ] **Step 3: Implement**

`internal/chstore/streams.go`:
```go
package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
)

type Stream struct {
	ID     uint64
	Labels labels.Labels
}

// ResolveStreams returns the streams of tenant matching ms that have data on any UTC day in [from, through].
func ResolveStreams(ctx context.Context, conn driver.Conn, tenant string, ms []*labels.Matcher, from, through time.Time) ([]Stream, error) {
	where, goSide := StreamMatcherSQL(ms)
	q := "SELECT stream_id, any(labels) FROM streams WHERE tenant = ? AND day >= toDate(?) AND day <= toDate(?)"
	args := []any{tenant, from.UTC(), through.UTC()}
	if !where.Empty() {
		q += " AND " + where.SQL
		args = append(args, where.Args...)
	}
	q += " GROUP BY stream_id"
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("resolve streams: %w", err)
	}
	defer rows.Close()
	var out []Stream
	for rows.Next() {
		var id uint64
		var m map[string]string
		if err := rows.Scan(&id, &m); err != nil {
			return nil, err
		}
		ls := labels.FromMap(m)
		ok := true
		for _, gm := range goSide {
			if !gm.Matches(ls.Get(gm.Name)) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, Stream{ID: id, Labels: ls})
		}
	}
	return out, rows.Err()
}
```

`internal/chstore/rowiter.go`:
```go
package chstore

import (
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

type row struct {
	streamID uint64
	ts       time.Time
	line     string
	md       labels.Labels
}

// rowReader streams rows from ClickHouse, dropping exact duplicates (Loki merge-iterator semantics).
type rowReader struct {
	rows      driver.Rows
	cur       row
	groupTS   time.Time
	groupSID  uint64
	groupSeen map[string]struct{}
	err       error
}

func (r *rowReader) next() bool {
	for r.rows.Next() {
		var md map[string]string
		if err := r.rows.Scan(&r.cur.streamID, &r.cur.ts, &r.cur.line, &md); err != nil {
			r.err = err
			return false
		}
		r.cur.md = labels.FromMap(md)
		if !r.cur.ts.Equal(r.groupTS) || r.cur.streamID != r.groupSID || r.groupSeen == nil {
			r.groupTS, r.groupSID, r.groupSeen = r.cur.ts, r.cur.streamID, map[string]struct{}{}
		}
		key := r.cur.line + "\xff" + r.cur.md.String()
		if _, dup := r.groupSeen[key]; dup {
			continue
		}
		r.groupSeen[key] = struct{}{}
		return true
	}
	r.err = r.rows.Err()
	return false
}

func (r *rowReader) close() error { return r.rows.Close() }

func mdAdapters(ls labels.Labels) []logproto.LabelAdapter { return logproto.FromLabelsToLabelAdapters(ls) }

// sortedIDs is used to build deterministic IN lists.
func sortedIDs(ss []Stream) []uint64 {
	ids := make([]uint64, len(ss))
	for i, s := range ss {
		ids[i] = s.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
```

`internal/chstore/querier.go`:
```go
package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cespare/xxhash/v2"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

type Querier struct {
	Conn driver.Conn
}

var _ logql.Querier = (*Querier)(nil)

// openRows resolves streams and opens an ordered row cursor for the selector.
func (q *Querier) openRows(ctx context.Context, sel syntax.LogSelectorExpr, req interface {
	GetStart() time.Time
	GetEnd() time.Time
}, dir logproto.Direction) (*rowReader, map[uint64]Stream, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, sel.Matchers(), req.GetStart(), req.GetEnd())
	if err != nil || len(streams) == 0 {
		return nil, nil, err
	}
	byID := make(map[uint64]Stream, len(streams))
	for _, s := range streams {
		byID[s.ID] = s
	}
	order := "ts ASC"
	if dir == logproto.BACKWARD {
		order = "ts DESC"
	}
	sql := "SELECT stream_id, ts, line, structured_metadata FROM logs WHERE tenant = ? AND stream_id IN ? AND ts >= ? AND ts < ?"
	args := []any{tenantID, sortedIDs(streams), req.GetStart().UTC(), req.GetEnd().UTC()}
	if lf := LeadingLineFilterSQL(sel); !lf.Empty() {
		sql += " AND " + lf.SQL
		args = append(args, lf.Args...)
	}
	sql += " ORDER BY " + order + ", stream_id ASC, line_hash"
	rows, err := q.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("select logs: %w", err)
	}
	return &rowReader{rows: rows}, byID, nil
}

func (q *Querier) SelectLogs(ctx context.Context, p logql.SelectLogParams) (iter.EntryIterator, error) {
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	pipeline, err := sel.Pipeline()
	if err != nil {
		return nil, err
	}
	rr, byID, err := q.openRows(ctx, sel, p.QueryRequest, p.Direction)
	if err != nil || rr == nil {
		return iter.NoopEntryIterator, err
	}
	return &entryIter{rr: rr, byID: byID, pipeline: pipeline, sp: map[uint64]log.StreamPipeline{}}, nil
}

type entryIter struct {
	rr       *rowReader
	byID     map[uint64]Stream
	pipeline log.Pipeline
	sp       map[uint64]log.StreamPipeline
	cur      logproto.Entry
	lbls     string
	hash     uint64
}

func (it *entryIter) Next() bool {
	for it.rr.next() {
		r := it.rr.cur
		sp, ok := it.sp[r.streamID]
		if !ok {
			sp = it.pipeline.ForStream(it.byID[r.streamID].Labels)
			it.sp[r.streamID] = sp
		}
		line, res, matches := sp.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !matches {
			continue
		}
		it.cur = logproto.Entry{
			Timestamp:          r.ts,
			Line:               line,
			StructuredMetadata: mdAdapters(res.StructuredMetadata()),
			Parsed:             mdAdapters(res.Parsed()),
		}
		it.lbls, it.hash = res.String(), r.streamID
		return true
	}
	return false
}

func (it *entryIter) At() logproto.Entry  { return it.cur }
func (it *entryIter) Labels() string      { return it.lbls }
func (it *entryIter) StreamHash() uint64  { return it.hash }
func (it *entryIter) Err() error          { return it.rr.err }
func (it *entryIter) Close() error        { return it.rr.close() }

func (q *Querier) SelectSamples(ctx context.Context, p logql.SelectSampleParams) (iter.SampleIterator, error) {
	expr, err := p.Expr()
	if err != nil {
		return nil, err
	}
	extractor, err := expr.Extractor()
	if err != nil {
		return nil, err
	}
	if extractor == nil {
		return iter.NoopSampleIterator, nil
	}
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	rr, byID, err := q.openRows(ctx, sel, p.SampleQueryRequest, logproto.FORWARD)
	if err != nil || rr == nil {
		return iter.NoopSampleIterator, err
	}
	defer rr.close()
	series := map[string]*logproto.Series{}
	ex := map[uint64]log.StreamSampleExtractor{}
	for rr.next() {
		r := rr.cur
		se, ok := ex[r.streamID]
		if !ok {
			se = extractor.ForStream(byID[r.streamID].Labels)
			ex[r.streamID] = se
		}
		s, ok := se.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !ok {
			continue
		}
		key := s.Labels.String()
		ser, found := series[key]
		if !found {
			ser = &logproto.Series{Labels: key, StreamHash: r.streamID}
			series[key] = ser
		}
		ser.Samples = append(ser.Samples, logproto.Sample{
			Timestamp: r.ts.UnixNano(), Value: s.Value, Hash: xxhash.Sum64String(key + r.line),
		})
	}
	if rr.err != nil {
		return nil, rr.err
	}
	out := make([]logproto.Series, 0, len(series))
	for _, s := range series {
		out = append(out, *s)
	}
	return iter.NewTimestampFirstMultiSeriesIterator(out), nil
}
```

Implementation notes:
- `*logproto.QueryRequest` and `*logproto.SampleQueryRequest` both have generated `GetStart()`/`GetEnd()` getters (verified at the pinned commit).
- If the compiler reports `SampleQueryRequest` has no `Direction`, keep FORWARD as written.
- `SelectSamples` buffers all series in memory. That is correct but unbounded, and Phase 2 replaces it with SQL push-down. Do not optimise it here.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/chstore/ -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add internal/chstore
git commit -m "feat(chstore): logql.Querier over ClickHouse with Loki ordering and dedup semantics"
```

---

### Task 7: Metadata: labels, label values, series

**Files:**
- Create: `internal/chstore/metadata.go`, `internal/chstore/metadata_test.go`

**Interfaces:**
- Consumes: `ResolveStreams`, `syntax.ParseMatchers`, `syntax.ParseLogSelector`
- Produces:
  - `func (q *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)`. With `req.Values == false` it returns sorted distinct label names. Otherwise it returns sorted distinct values of `req.Name`, optionally filtered by `req.Query` (a stream selector).
  - `func (q *Querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error)` returns the union over `req.Groups` (each a selector). Each series lists its labels sorted by name. Series are ordered by labels string.

- [ ] **Step 1: Write the failing test**

`internal/chstore/metadata_test.go`:
```go
package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestLabelAndSeries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="a", env="dev"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", env="prod"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	seed(t, &chstore.Writer{Conn: conn}, "t2",
		logproto.Stream{Labels: `{secret="1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := t0.Add(-time.Hour), t0.Add(time.Hour)

	names, err := q.Label(ctx, &logproto.LabelRequest{Start: &start, End: &end})
	require.NoError(t, err)
	require.Equal(t, []string{"app", "env"}, names.Values)

	vals, err := q.Label(ctx, &logproto.LabelRequest{Name: "app", Values: true, Start: &start, End: &end, Query: `{env="prod"}`})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, vals.Values)

	ser, err := q.Series(ctx, &logproto.SeriesRequest{Start: start, End: end, Groups: []string{`{app=~"a|b"}`}})
	require.NoError(t, err)
	require.Len(t, ser.Series, 2)
	require.Equal(t, "app", ser.Series[0].Labels[0].Key)
	require.Equal(t, "a", ser.Series[0].Labels[0].Value)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/chstore/ -run TestLabelAndSeries`
Expected: FAIL, `q.Label undefined`

- [ ] **Step 3: Implement**

`internal/chstore/metadata.go`:
```go
package chstore

import (
	"context"
	"sort"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

func (q *Querier) streamsFor(ctx context.Context, selector string, from, through time.Time) ([]Stream, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	var ms []*labels.Matcher
	if selector != "" {
		sel, err := syntax.ParseLogSelector(selector, true)
		if err != nil {
			return nil, err
		}
		ms = sel.Matchers()
	}
	return ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
}

func (q *Querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	streams, err := q.streamsFor(ctx, req.Query, *req.Start, *req.End)
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) {
			if !req.Values {
				set[l.Name] = struct{}{}
			} else if l.Name == req.Name {
				set[l.Value] = struct{}{}
			}
		})
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return &logproto.LabelResponse{Values: out}, nil
}

func (q *Querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	groups := req.Groups
	if len(groups) == 0 {
		groups = []string{""}
	}
	seen := map[uint64]labels.Labels{}
	for _, g := range groups {
		streams, err := q.streamsFor(ctx, g, req.Start, req.End)
		if err != nil {
			return nil, err
		}
		for _, s := range streams {
			seen[s.ID] = s.Labels
		}
	}
	all := make([]labels.Labels, 0, len(seen))
	for _, ls := range seen {
		all = append(all, ls)
	}
	sort.Slice(all, func(i, j int) bool { return labels.Compare(all[i], all[j]) < 0 })
	resp := &logproto.SeriesResponse{}
	for _, ls := range all {
		var id logproto.SeriesIdentifier
		ls.Range(func(l labels.Label) {
			id.Labels = append(id.Labels, logproto.SeriesIdentifier_LabelsEntry{Key: l.Name, Value: l.Value})
		})
		resp.Series = append(resp.Series, id)
	}
	return resp, nil
}
```

ClickHouse can do this aggregation natively. Moving the label-name and label-value distinct sets into SQL (`arrayJoin(mapKeys(labels))`) is a Phase 3 optimisation that will run against `stream_stats_hourly`. Keep it in Go here so regex matchers stay exact.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/chstore/ -run TestLabelAndSeries -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/metadata*.go
git commit -m "feat(chstore): labels, label values and series from streams table"
```

---

### Task 8: HTTP API: tenant middleware, query, query_range, labels, series, ready, buildinfo

**Files:**
- Create: `internal/api/api.go`, `internal/api/query.go`, `internal/api/metadata.go`, `internal/api/misc.go`, `internal/api/api_test.go`

**Interfaces:**
- Consumes: `chstore.Querier` (`logql.Querier`, `Label`, `Series`), `ingest.Handler`, Loki `loghttp.ParseRangeQuery`, `loghttp.ParseInstantQuery`, `loghttp.ParseLabelQuery`, `loghttp.ParseAndValidateSeriesQuery`, `marshal.WriteQueryResponseJSON`, `marshal.WriteLabelResponseJSON`, `marshal.WriteSeriesResponseJSON`, `httpreq.ExtractEncodingFlagsFromProto`/`httpreq.ExtractEncodingFlags`
- Produces:
  - `type API struct{ Engine *logql.QueryEngine; Q *chstore.Querier; Push http.Handler; AuthEnabled bool }`
  - `func (a *API) Router() http.Handler` with routes:
    `POST /loki/api/v1/push`, `POST /api/prom/push`, `POST /otlp/v1/logs`,
    `GET|POST /loki/api/v1/query`, `GET|POST /loki/api/v1/query_range`,
    `GET /loki/api/v1/labels`, `GET /loki/api/v1/label/{name}/values`, `GET|POST /loki/api/v1/series`,
    `GET /ready`, `GET /loki/api/v1/status/buildinfo`
  - Tenant rule. With `AuthEnabled`, a missing `X-Scope-OrgID` gets 401 with body `no org id`. Without it the tenant is `fake`. Use `user.InjectOrgID`.

- [ ] **Step 1: Write the failing tests**

`internal/api/api_test.go`:
```go
package api_test

import (
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
)

func newServer(t *testing.T, auth bool) *httptest.Server {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	q := &chstore.Querier{Conn: conn}
	a := &api.API{
		Engine:      logql.NewEngine(logql.EngineOpts{}, q, o, log.NewNopLogger()),
		Q:           q,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()},
		AuthEnabled: auth,
	}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, u, tenant, body string) (int, string) {
	req, _ := http.NewRequest(method, u, strings.NewReader(body))
	if tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b := new(strings.Builder)
	_, _ = io.Copy(b, resp.Body)
	return resp.StatusCode, b.String()
}

func TestAPI_TenantIsolation(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	ts := strconv.FormatInt(now.UnixNano(), 10)
	code, body := do(t, "POST", srv.URL+"/loki/api/v1/push", "", `{"streams":[]}`)
	require.Equal(t, http.StatusUnauthorized, code)
	require.Contains(t, body, "no org id")

	code, body = do(t, "POST", srv.URL+"/loki/api/v1/push", "a", `{"streams":[{"stream":{"app":"x"},"values":[["`+ts+`","from a"]]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)

	qs := url.Values{"query": {`{app="x"}`}, "start": {strconv.FormatInt(now.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	_, body = do(t, "GET", srv.URL+"/loki/api/v1/query_range?"+qs.Encode(), "b", "")
	var r struct{ Data struct{ Result []any } }
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	require.Empty(t, r.Data.Result)

	_, body = do(t, "GET", srv.URL+"/loki/api/v1/query_range?"+qs.Encode(), "a", "")
	require.Contains(t, body, "from a")
	require.Contains(t, body, `"resultType":"streams"`)
}

func TestAPI_LabelsAndReady(t *testing.T) {
	srv := newServer(t, false)
	code, _ := do(t, "GET", srv.URL+"/ready", "", "")
	require.Equal(t, http.StatusOK, code)
	code, body := do(t, "GET", srv.URL+"/loki/api/v1/labels", "", "")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"status":"success"}`, onlyStatus(body))
}

func onlyStatus(b string) string {
	var m map[string]any
	_ = json.Unmarshal([]byte(b), &m)
	out, _ := json.Marshal(map[string]any{"status": m["status"]})
	return string(out)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/api/...`
Expected: FAIL, `undefined: api.API`

- [ ] **Step 3: Implement**

`internal/api/api.go`:
```go
package api

import (
	"net/http"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"

	"github.com/bzed/lokihouse/internal/chstore"
)

type API struct {
	Engine      *logql.QueryEngine
	Q           *chstore.Querier
	Push        http.Handler
	AuthEnabled bool
}

func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	t := a.tenant
	mux.Handle("POST /loki/api/v1/push", t(a.Push))
	mux.Handle("POST /api/prom/push", t(a.Push))
	mux.Handle("POST /otlp/v1/logs", t(a.Push))
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/query", t(http.HandlerFunc(a.instantQuery)))
		mux.Handle(m+" /loki/api/v1/query_range", t(http.HandlerFunc(a.rangeQuery)))
		mux.Handle(m+" /loki/api/v1/series", t(http.HandlerFunc(a.series)))
	}
	mux.Handle("GET /loki/api/v1/labels", t(http.HandlerFunc(a.labels)))
	mux.Handle("GET /loki/api/v1/label/{name}/values", t(http.HandlerFunc(a.labelValues)))
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /loki/api/v1/status/buildinfo", buildInfo)
	return mux
}

func (a *API) tenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Scope-OrgID")
		if id == "" {
			if a.AuthEnabled {
				http.Error(w, "no org id", http.StatusUnauthorized)
				return
			}
			id = "fake"
		}
		next.ServeHTTP(w, r.WithContext(user.InjectOrgID(r.Context(), id)))
	})
}

func writeError(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}
```

`internal/api/query.go`:
```go
package api

import (
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	"github.com/grafana/loki/v3/pkg/util/marshal"
)

func (a *API) rangeQuery(w http.ResponseWriter, r *http.Request) {
	rq, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	p, err := logql.NewLiteralParams(rq.Query, rq.Start, rq.End, rq.Step, rq.Interval, rq.Direction, rq.Limit, rq.Shards, nil)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	a.exec(w, r, p)
}

func (a *API) instantQuery(w http.ResponseWriter, r *http.Request) {
	iq, err := loghttp.ParseInstantQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	p, err := logql.NewLiteralParams(iq.Query, iq.Ts, iq.Ts, 0, 0, iq.Direction, iq.Limit, iq.Shards, nil)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	a.exec(w, r, p)
}

func (a *API) exec(w http.ResponseWriter, r *http.Request, p logql.Params) {
	res, err := a.Engine.Query(p).Exec(r.Context())
	if err != nil {
		writeError(w, err, statusFor(err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := marshal.WriteQueryResponseJSON(res.Data, res.Warnings, res.Statistics, w, httpreq.ExtractEncodingFlags(r)); err != nil {
		writeError(w, err, http.StatusInternalServerError)
	}
}
```

Add to `internal/api/api.go` (it reuses Loki's exact error → status mapping, which is exported at the pinned commit):
```go
import serverutil "github.com/grafana/loki/v3/pkg/util/server"

func statusFor(err error) int {
	code, _ := serverutil.ClientHTTPStatusAndError(err)
	return code
}
```
For error *bodies*, use `serverutil.WriteError(err, w)` instead of `http.Error` in `exec`, `label` and `series`, so the response text matches Loki's.

`internal/api/metadata.go`:
```go
package api

import (
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/util/marshal"
)

func (a *API) labels(w http.ResponseWriter, r *http.Request) { a.label(w, r) }

func (a *API) labelValues(w http.ResponseWriter, r *http.Request) { a.label(w, r) }

func (a *API) label(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	if name := r.PathValue("name"); name != "" {
		req.Name, req.Values = name, true
	}
	resp, err := a.Q.Label(r.Context(), req)
	if err != nil {
		writeError(w, err, statusFor(err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteLabelResponseJSON(resp.Values, w)
}

func (a *API) series(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseAndValidateSeriesQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	resp, err := a.Q.Series(r.Context(), req)
	if err != nil {
		writeError(w, err, statusFor(err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteSeriesResponseJSON(resp.Series, w)
}
```

Loki's `ParseLabelQuery` reads the name via `mux.Vars(r)["name"]` (gorilla/mux, verified). The manual override above after parsing is therefore required, and correct.

`internal/api/misc.go`:
```go
package api

import (
	"encoding/json"
	"net/http"
)

// Version is set at build time with -ldflags "-X github.com/bzed/lokihouse/internal/api.Version=...".
var Version = "dev"

func buildInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"version": Version, "revision": "", "branch": "", "buildUser": "", "buildDate": "", "goVersion": ""})
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/api/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/api
git commit -m "feat(api): Loki HTTP query, label, series, push routes with tenant handling"
```

---

### Task 9: Config and `lokihouse` binary

**Files:**
- Create: `internal/config/config.go`, `internal/config/config_test.go`, `cmd/lokihouse/main.go`, `lokihouse.example.yaml`

**Interfaces:**
- Consumes: everything above
- Produces:
  - `type Config struct{ Server struct{ HTTPListenAddress string; HTTPListenPort int }; AuthEnabled bool; ClickHouse struct{ Addr []string; Database, Username, Password, Cluster string; Replicated bool; ReplicaPath, ReplicaName string; Settings map[string]any }; LimitsConfig validation.Limits }`. YAML keys:
    - `server.http_listen_address`, `server.http_listen_port`
    - `auth_enabled`
    - `clickhouse.{addr,database,username,password,cluster,replicated,replica_path,replica_name,settings}`; `replicated` defaults to true and only matters when `cluster` is set
    - `limits_config` (Loki-compatible)
  - `func (c *Config) Topology() chstore.Topology` and `func (c *Config) CHSettings() clickhouse.Settings` (topology settings overlaid with `clickhouse.settings`)
  - `func Load(path string) (*Config, error)` applies Loki's limit defaults before unmarshalling
  - `lokihouse -config.file=path`

- [ ] **Step 1: Write the failing test**

`internal/config/config_test.go`:
```go
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
)

func TestLoad_DefaultsAndOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
auth_enabled: true
server: {http_listen_port: 3101}
clickhouse: {addr: ["ch:9000"], database: logs}
limits_config: {max_line_size: 1024}
`), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.True(t, c.AuthEnabled)
	require.Equal(t, 3101, c.Server.HTTPListenPort)
	require.Equal(t, []string{"ch:9000"}, c.ClickHouse.Addr)
	require.EqualValues(t, 1024, c.LimitsConfig.MaxLineSize)
	require.Equal(t, 7*24*time.Hour, time.Duration(c.LimitsConfig.RejectOldSamplesMaxAge)) // Loki default kept
	require.Equal(t, chstore.Topology{}, c.Topology())                                    // single node by default
}

func TestLoad_Cluster(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse:
  addr: ["ch1:9000", "ch3:9000"]
  cluster: lokihouse
  settings: {insert_quorum: 2, max_execution_time: 60}
`), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.Equal(t, chstore.Topology{Cluster: "lokihouse", Replicated: true}, c.Topology())
	s := c.CHSettings()
	require.EqualValues(t, 2, s["insert_quorum"]) // explicit setting wins over the topology default
	require.EqualValues(t, 60, s["max_execution_time"])
	require.EqualValues(t, 1, s["distributed_foreground_insert"])
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/config/...`
Expected: FAIL, `undefined: Load`

- [ ] **Step 3: Implement**

`internal/config/config.go`:
```go
package config

import (
	"flag"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/validation"
	"gopkg.in/yaml.v2"

	"github.com/bzed/lokihouse/internal/chstore"
)

type Config struct {
	Server struct {
		HTTPListenAddress string `yaml:"http_listen_address"`
		HTTPListenPort    int    `yaml:"http_listen_port"`
	} `yaml:"server"`
	AuthEnabled bool `yaml:"auth_enabled"`
	ClickHouse  struct {
		Addr        []string       `yaml:"addr"`
		Database    string         `yaml:"database"`
		Username    string         `yaml:"username"`
		Password    string         `yaml:"password"`
		Cluster     string         `yaml:"cluster"`      // remote_servers name; empty = single server
		Replicated  bool           `yaml:"replicated"`   // Replicated*MergeTree on the cluster (default true)
		ReplicaPath string         `yaml:"replica_path"` // optional; default: server's default_replica_path
		ReplicaName string         `yaml:"replica_name"` // optional; default: server's default_replica_name
		Settings    map[string]any `yaml:"settings"`     // extra ClickHouse session settings, override topology defaults
	} `yaml:"clickhouse"`
	LimitsConfig validation.Limits `yaml:"limits_config"`
}

func Load(path string) (*Config, error) {
	var defaults validation.Limits
	defaults.RegisterFlags(flag.NewFlagSet("defaults", flag.PanicOnError))
	validation.SetDefaultLimitsForYAMLUnmarshalling(defaults)

	c := &Config{AuthEnabled: true}
	c.Server.HTTPListenPort = 3100
	c.ClickHouse.Addr = []string{"localhost:9000"}
	c.ClickHouse.Database = "lokihouse"
	c.ClickHouse.Username = "default"
	c.ClickHouse.Replicated = true
	c.LimitsConfig = defaults

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.UnmarshalStrict(b, c); err != nil {
		return nil, err
	}
	return c, nil
}

// Topology returns the ClickHouse table layout selected by the config.
func (c *Config) Topology() chstore.Topology {
	ch := c.ClickHouse
	return chstore.Topology{
		Cluster:     ch.Cluster,
		Replicated:  ch.Cluster != "" && ch.Replicated,
		ReplicaPath: ch.ReplicaPath,
		ReplicaName: ch.ReplicaName,
	}
}

// CHSettings returns the session settings for every ClickHouse connection.
func (c *Config) CHSettings() clickhouse.Settings {
	s := c.Topology().Settings()
	for k, v := range c.ClickHouse.Settings {
		s[k] = v
	}
	return s
}
```
Use whichever YAML package Loki's `validation.Limits.UnmarshalYAML` signature expects. It takes `*yaml.Node`, which is `gopkg.in/yaml.v3`, so switch the import to `gopkg.in/yaml.v3` and use `yaml.NewDecoder` with `KnownFields(true)` if v2 fails to compile.

`cmd/lokihouse/main.go`:
```go
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/ingest"
)

func main() {
	path := flag.String("config.file", "lokihouse.yaml", "configuration file")
	flag.Parse()
	logger := log.NewLogfmtLogger(os.Stderr)
	if err := run(*path, logger); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}

func run(path string, logger log.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	ctx := context.Background()
	opts := func(db string) *clickhouse.Options {
		return &clickhouse.Options{
			Addr:     cfg.ClickHouse.Addr,
			Auth:     clickhouse.Auth{Database: db, Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password},
			Settings: cfg.CHSettings(),
		}
	}
	admin, err := clickhouse.Open(opts("default"))
	if err != nil {
		return err
	}
	if err := chstore.Migrate(ctx, admin, cfg.ClickHouse.Database, cfg.Topology()); err != nil {
		return err
	}
	_ = admin.Close()
	conn, err := clickhouse.Open(opts(cfg.ClickHouse.Database))
	if err != nil {
		return err
	}
	limits, err := validation.NewOverrides(cfg.LimitsConfig, nil)
	if err != nil {
		return err
	}
	q := &chstore.Querier{Conn: conn}
	a := &api.API{
		Engine:      logql.NewEngine(logql.EngineOpts{}, q, limits, logger),
		Q:           q,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: limits, Logger: logger},
		AuthEnabled: cfg.AuthEnabled,
	}
	addr := fmt.Sprintf("%s:%d", cfg.Server.HTTPListenAddress, cfg.Server.HTTPListenPort)
	level.Info(logger).Log("msg", "lokihouse listening", "addr", addr)
	return http.ListenAndServe(addr, a.Router())
}
```

`lokihouse.example.yaml`:
```yaml
auth_enabled: false
server:
  http_listen_port: 3100
clickhouse:
  addr: ["localhost:9000"]
  database: lokihouse
  username: default
  # ClickHouse cluster (remote_servers name). Tables become Replicated*MergeTree
  # <table>_local created ON CLUSTER, plus Distributed tables; list several nodes in addr.
  # cluster: lokihouse
  # replicated: true              # false = sharding without replication
  # replica_path: /clickhouse/tables/{shard}/{database}/{table}   # default: server default_replica_path
  # replica_name: '{replica}'
  # settings: {insert_quorum: 2}  # any ClickHouse session setting
limits_config: {}
```

- [ ] **Step 4: Run tests and a manual smoke test**

Run: `go test ./internal/config/... -v && make build`
(`TestLoad_Cluster` needs no cluster; it only checks config mapping.)
Expected: PASS, and `bin/lokihouse` exists.

Smoke test:
```bash
bin/lokihouse -config.file=lokihouse.example.yaml &
sleep 2
curl -s -XPOST localhost:3100/loki/api/v1/push -H 'Content-Type: application/json' \
  -d "{\"streams\":[{\"stream\":{\"app\":\"smoke\"},\"values\":[[\"$(date +%s%N)\",\"hello smoke\"]]}]}" -w '%{http_code}\n'
curl -s -G localhost:3100/loki/api/v1/query_range --data-urlencode 'query={app="smoke"}' | grep -o 'hello smoke'
kill %1
```
Expected: `204`, then `hello smoke`.

- [ ] **Step 5: Commit**

```bash
git add internal/config cmd lokihouse.example.yaml
git commit -m "feat: lokihouse binary with Loki-compatible limits config"
```

---

### Task 10: Differential oracle harness against real Loki

**Files:**
- Create: `test/oracle/loki.go`, `test/oracle/lokihouse.go`, `test/oracle/fixture.go`, `test/oracle/compare.go`, `test/oracle/oracle_test.go`, `test/oracle/loki-config.yaml`, `test/oracle/corpus/basic.logql`

All files carry the build tag `//go:build oracle`.

**Interfaces:**
- Consumes: built `bin/lokihouse`, Loki source at `LOKI_COMMIT`
- Produces:
  - `func StartLoki(t *testing.T) (baseURL string)` clones or reuses Loki at the pinned commit in `.cache/loki-src`, builds `.cache/loki-bin/loki` once (`go build ./cmd/loki`), and starts it on a free port with `loki-config.yaml` in a temp dir
  - `func StartLokihouse(t *testing.T) (baseURL string)` starts `go run ./cmd/lokihouse` with a fresh ClickHouse database
  - `func Fixture(base time.Time) []PushBatch` returns a deterministic, seeded data set (described below)
  - `func Compare(t *testing.T, name string, a, b []byte)` normalises and diffs two Loki JSON responses. It drops `data.stats` and `data.encodingFlags`. Within each stream it sorts entries that share a timestamp, because tie order inside one stream is undefined (spec §7.1). It then requires JSON equality.
  - Corpus format: one query per line. `#` starts a comment. A line may be prefixed with `instant:` or `range:` (default `range`).

Fixture content (seeded `rand.New(rand.NewSource(42))`, base = `time.Now().Truncate(time.Hour).Add(-2*time.Hour)`, 2 tenants, 12 streams over 3 apps × 2 envs × 2 pods, 2 000 entries per tenant over 90 minutes):
- logfmt lines `level=<info|warn|error|debug> method=<GET|POST> status=<200|404|500> duration=<n>ms bytes=<n> msg="..."`
- JSON lines `{"level":"..","status":..,"latency_ms":..,"user":{"id":..,"name":".."},"tags":["a","b"]}` on app `api`
- unstructured lines with IPs (`10.x.y.z`) and ANSI colour codes on app `legacy`
- structured metadata `trace_id` on half the entries
- three deliberate exact duplicates and two same-timestamp pairs in one stream

`test/oracle/loki-config.yaml`:
```yaml
auth_enabled: true
server:
  http_listen_port: 0       # overwritten by the harness
  grpc_listen_port: 0
common:
  path_prefix: ${DIR}
  replication_factor: 1
  ring:
    kvstore: {store: inmemory}
  storage:
    filesystem: {chunks_directory: ${DIR}/chunks, rules_directory: ${DIR}/rules}
schema_config:
  configs:
    - from: 2020-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index: {prefix: index_, period: 24h}
query_range:
  cache_results: false
  parallelise_shardable_queries: false
limits_config:
  split_queries_by_interval: 0
analytics:
  reporting_enabled: false
```
The harness substitutes `${DIR}` and the ports. Turning off sharding and splitting in the oracle makes it compute the same thing Loki's engine computes without fan-out, so differences point to lokihouse.

`test/oracle/corpus/basic.logql` (initial corpus; grows with every bug):
```
# selectors and line filters
{app="api"}
{app=~"api|web", env!="prod"}
{app="web"} |= "error"
{app="web"} != "GET" |= "status=500"
{app="web"} |= "POST" or "DELETE"
{app="web"} |~ "status=(4|5)\\d\\d"
{app="legacy"} |= ip("10.0.0.0/8")
{app="legacy"} | decolorize
# parsers and formatting
{app="api"} | json
{app="api"} | json | status >= 500
{app="api"} | json lvl="level", uid="user.id" | line_format "{{.lvl}} {{.uid}}"
{app="web"} | logfmt | duration > 100ms
{app="web"} | logfmt | label_format s="{{.status}}-{{.method}}" | keep s
{app="web"} | pattern "level=<lvl> method=<m> <_>" | lvl="error"
{app="web"} | regexp "status=(?P<st>\\d+)" | st="404"
{app="api"} | trace_id != ""
{app="web"} | logfmt | drop msg
# metric queries
range:count_over_time({app="web"}[5m])
range:sum by (env) (rate({app="web"} |= "error" [1m]))
range:sum by (app) (bytes_over_time({env="dev"}[10m]))
range:avg_over_time({app="web"} | logfmt | unwrap duration(duration) [5m]) by (pod)
range:quantile_over_time(0.95, {app="api"} | json | unwrap latency_ms [5m]) by (env)
range:topk(2, sum by (pod) (count_over_time({app="web"}[5m])))
range:sum(count_over_time({app="web"} |= "error"[5m])) / sum(count_over_time({app="web"}[5m]))
range:absent_over_time({app="nope"}[5m])
range:sum by (level) (count_over_time({app="web"} | logfmt [5m]))
range:rate({app="web"} | logfmt | unwrap bytes(bytes) [1m])
range:first_over_time({app="web"} | logfmt | unwrap bytes [5m]) by (pod)
range:max_over_time({app="api"} | json | unwrap latency_ms [5m]) by (app) > 100
range:label_replace(sum by (pod) (count_over_time({app="web"}[5m])), "p", "$1", "pod", "(.*)")
instant:sum by (app) (count_over_time({env="prod"}[1h]))
instant:{app="web"} |= "error"
```

- [ ] **Step 1: Write the oracle test**

`test/oracle/oracle_test.go`:
```go
//go:build oracle

package oracle

import (
	"bufio"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOracle(t *testing.T) {
	loki := StartLoki(t)
	house := StartLokihouse(t)
	base := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
	for _, b := range Fixture(base) {
		PushBoth(t, []string{loki, house}, b)
	}
	WaitQueryable(t, loki, base) // Loki ingesters: retry until the fixture count is visible

	start, end := base, base.Add(95*time.Minute)
	files, _ := filepath.Glob("corpus/*.logql")
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			kind, q := "range", line
			if k, rest, ok := strings.Cut(line, ":"); ok && (k == "range" || k == "instant") {
				kind, q = k, rest
			}
			for _, tenant := range []string{"tenant-a", "tenant-b"} {
				t.Run(tenant+"/"+kind+"/"+q, func(t *testing.T) {
					v := url.Values{"query": {q}, "limit": {"1000"}}
					path := "/loki/api/v1/query_range"
					if kind == "instant" {
						path = "/loki/api/v1/query"
						v.Set("time", strconv.FormatInt(end.UnixNano(), 10))
					} else {
						v.Set("start", strconv.FormatInt(start.UnixNano(), 10))
						v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
						v.Set("step", "60s")
					}
					a := Get(t, loki+path+"?"+v.Encode(), tenant)
					b := Get(t, house+path+"?"+v.Encode(), tenant)
					Compare(t, q, a, b)
				})
			}
		}
		fh.Close()
	}
	for _, p := range []string{"/loki/api/v1/labels", "/loki/api/v1/label/app/values", "/loki/api/v1/series?match[]=" + url.QueryEscape(`{app=~".+"}`)} {
		t.Run("meta"+p, func(t *testing.T) {
			sep := "?"
			if strings.Contains(p, "?") {
				sep = "&"
			}
			qs := sep + "start=" + strconv.FormatInt(start.UnixNano(), 10) + "&end=" + strconv.FormatInt(end.UnixNano(), 10)
			Compare(t, p, Get(t, loki+p+qs, "tenant-a"), Get(t, house+p+qs, "tenant-a"))
		})
	}
}
```

- [ ] **Step 2: Implement harness files**

Write `loki.go`, `lokihouse.go`, `fixture.go` and `compare.go` with the functions named in **Interfaces**. In addition:
- `PushBoth(t, urls []string, b PushBatch)` POSTs the same JSON body (`{"streams":[...]}`) with `X-Scope-OrgID: b.Tenant` and requires 204 from both.
- `Get(t, url, tenant) []byte` requires 200, except that a 400 must be 400 on both sides with an equal error message. In that case `Get` returns the body and `Compare` compares the error strings.
- `WaitQueryable` polls `count_over_time({app=~".+"}[2h])` on Loki until it equals the fixture size, with a 60 s timeout.
- `StartLoki` uses `exec.Command(bin, "-config.file", cfgPath, "-config.expand-env=true")` with env `DIR=<tmp>`, and passes `-server.http-listen-port=<free>` and `-server.grpc-listen-port=<free>`. It waits for `/ready` to answer 200 (Loki answers 503 while starting) for up to 120 s, and kills the process in `t.Cleanup`.
- `StartLokihouse` builds `bin/lokihouse` via `go build`. It writes a config with `auth_enabled: true`, a free port, a fresh database name, and `clickhouse.cluster`/`replicated` taken from `chtest.Topology()`. It starts the binary and waits for `/ready`. On cleanup it drops the database with `chstore.DropDatabase(ctx, conn, db, chtest.Topology())`, reusing `chtest.Options`. This makes `make oracle-cluster` (Task 12) run the same suite against the cluster.
- `Compare` on mismatch writes both bodies to `t.TempDir()` and fails with a unified diff (use `github.com/pmezard/go-difflib/difflib`, already in Loki's dependency tree).

- [ ] **Step 3: Run the oracle suite and triage**

Run: `make oracle`
Expected: the first build of Loki takes several minutes. Then every subtest either passes or shows a diff.

For every failing subtest, use **superpowers:systematic-debugging**. Each failure is one of:
1. A lokihouse bug. Fix it in the owning package and add a focused unit test there.
2. A harness normalisation gap (an undefined ordering only). Fix it in `Compare` and add a comment citing the Loki code that makes the order undefined.
3. A genuine accepted divergence. Write it in `docs/divergences.md` with the reason and skip the case with `t.Skip("divergence: <link>")`. This needs the project owner's approval, so ask before adding any.

Expected after triage: PASS for all corpus entries.

- [ ] **Step 4: Commit**

```bash
git add test/oracle docs/divergences.md
git commit -m "test: differential oracle suite against Loki at pinned commit"
```

---

### Task 11: Corpus expansion from Loki's own test queries

**Files:**
- Create: `test/oracle/corpus/loki_parser_tests.logql`, `tools/extract-corpus/main.go`
- Modify: whatever the new oracle failures require (expected: small fixes in `internal/chstore`, `internal/ingest/distcopy`, `internal/api`)

**Interfaces:**
- Consumes: Task 10 harness
- Produces: `go run ./tools/extract-corpus -loki .cache/loki-src > test/oracle/corpus/loki_parser_tests.logql`. The tool collects every string literal passed as `in:` / `query:` / `qs:` in `pkg/logql/syntax/parser_test.go`, `pkg/logql/engine_test.go` and `docs/sources/query/**/*.md` code blocks tagged `logql`. It keeps those that `syntax.ParseExpr` accepts. It rewrites stream selectors that match no fixture stream to `{app=~".+"}` so they exercise data. It deduplicates, sorts, and prefixes metric expressions with `range:`.

- [ ] **Step 1: Write the extractor test**

`tools/extract-corpus/main_test.go`:
```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalise(t *testing.T) {
	q, ok := normalise(`sum(rate({foo="bar"}[5m]))`)
	require.True(t, ok)
	require.Equal(t, `range:sum(rate({app=~".+"}[5m]))`, q)

	_, ok = normalise(`{foo="bar"} |= `) // unparsable
	require.False(t, ok)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./tools/extract-corpus/...`
Expected: FAIL, `undefined: normalise`

- [ ] **Step 3: Implement**

`tools/extract-corpus/main.go`:
```go
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

func main() {
	root := flag.String("loki", ".cache/loki-src", "Loki source tree")
	flag.Parse()
	set := map[string]struct{}{}
	for _, f := range []string{"pkg/logql/syntax/parser_test.go", "pkg/logql/engine_test.go"} {
		for _, s := range goStrings(filepath.Join(*root, f)) {
			if q, ok := normalise(s); ok {
				set[q] = struct{}{}
			}
		}
	}
	_ = filepath.Walk(filepath.Join(*root, "docs/sources/query"), func(p string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(p, ".md") {
			for _, s := range mdLogQL(p) {
				if q, ok := normalise(s); ok {
					set[q] = struct{}{}
				}
			}
		}
		return nil
	})
	out := make([]string, 0, len(set))
	for q := range set {
		out = append(out, q)
	}
	sort.Strings(out)
	fmt.Println("# generated by tools/extract-corpus — do not edit")
	for _, q := range out {
		fmt.Println(q)
	}
}

func goStrings(path string) []string {
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, path, nil, 0)
	if err != nil {
		return nil
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
			if s, err := strconv.Unquote(bl.Value); err == nil && strings.Contains(s, "{") {
				out = append(out, s)
			}
		}
		return true
	})
	return out
}

var fence = regexp.MustCompile("(?s)```logql\\n(.*?)```")

func mdLogQL(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, m := range fence.FindAllSubmatch(b, -1) {
		sc := bufio.NewScanner(strings.NewReader(string(m[1])))
		var cur []string
		for sc.Scan() {
			cur = append(cur, sc.Text())
		}
		out = append(out, strings.Join(cur, " "))
	}
	return out
}

// normalise parses q, rewrites every selector to {app=~".+"} and tags metric queries.
func normalise(q string) (string, bool) {
	e, err := syntax.ParseExpr(q)
	if err != nil {
		return "", false
	}
	all := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "app", ".+")}
	e.Walk(func(x syntax.Expr) bool {
		if m, ok := x.(*syntax.MatchersExpr); ok {
			m.Mts = all
		}
		return true
	})
	s := e.String()
	if _, isLog := e.(syntax.LogSelectorExpr); !isLog {
		s = "range:" + s
	}
	if strings.Contains(s, "\n") {
		return "", false
	}
	return s, true
}
```
`Expr.Walk(func(Expr) bool)` matches `syntax.WalkFn` at the pinned commit (verified).

- [ ] **Step 4: Generate the corpus and run the oracle**

Run:
```bash
go test ./tools/extract-corpus/... && \
go run ./tools/extract-corpus -loki .cache/loki-src > test/oracle/corpus/loki_parser_tests.logql && \
wc -l test/oracle/corpus/loki_parser_tests.logql && make oracle
```
Expected: several hundred queries. Triage failures exactly as in Task 10, Step 3. Each fix is its own commit, `fix(<pkg>): <what> (oracle: <query>)`. Done when `make oracle` passes.

- [ ] **Step 5: Commit**

```bash
git add tools/extract-corpus test/oracle/corpus
git commit -m "test: expand oracle corpus with Loki's own parser/engine/doc queries"
```

---

### Task 12: Cluster test environment and cluster runs of the whole suite

**Files:**
- Create: `test/cluster/cluster.xml`, `test/cluster/keeper.xml`, `test/cluster/up.sh`, `test/cluster/down.sh`, `internal/chstore/cluster_test.go`
- Modify: `Makefile` (targets `cluster-up`, `cluster-down`, `test-cluster`, `oracle-cluster`)

**Interfaces:**
- Consumes:
  - `chtest.New` and `chtest.Topology()` (Task 2)
  - `chstore.Writer` (Task 3)
  - the `seed` and `t0` helpers in `internal/chstore/querier_test.go` (Task 6, same package `chstore_test`)
- Produces:
  - `make cluster-up` starts a ClickHouse 26.9 cluster named `lokihouse` with 2 shards × 2 replicas (`ch1`–`ch4`) plus one Keeper, on the podman network `lhnet`. Native ports are `localhost:19001`–`19004`.
  - `make test-cluster` and `make oracle-cluster` run the **unchanged** unit and oracle suites against that cluster.

- [ ] **Step 1: Cluster configuration**

`test/cluster/cluster.xml`:
```xml
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <remote_servers>
    <lokihouse>
      <shard><internal_replication>true</internal_replication>
        <replica><host>ch1</host><port>9000</port></replica>
        <replica><host>ch2</host><port>9000</port></replica></shard>
      <shard><internal_replication>true</internal_replication>
        <replica><host>ch3</host><port>9000</port></replica>
        <replica><host>ch4</host><port>9000</port></replica></shard>
    </lokihouse>
  </remote_servers>
  <zookeeper><node><host>keeper</host><port>9181</port></node></zookeeper>
  <distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>
</clickhouse>
```

`test/cluster/keeper.xml`:
```xml
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <keeper_server><tcp_port>9181</tcp_port><server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <raft_configuration><server><id>1</id><hostname>keeper</hostname><port>9234</port></server></raft_configuration>
  </keeper_server>
</clickhouse>
```

`test/cluster/up.sh`:
```sh
#!/bin/sh
# Start a 2-shard x 2-replica ClickHouse cluster "lokihouse" with one Keeper (podman).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
img=docker.io/clickhouse/clickhouse-server:26.9
work=${LOKIHOUSE_CLUSTER_DIR:-$here/../../.cache/cluster}
mkdir -p "$work"
podman network exists lhnet || podman network create lhnet >/dev/null
podman container exists keeper || podman run -d --name keeper --network lhnet \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$here/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:Z" "$img" >/dev/null
for i in 1 2 3 4; do
  printf '<clickhouse><macros><shard>%s</shard><replica>ch%s</replica></macros></clickhouse>\n' \
    $(( (i + 1) / 2 )) "$i" > "$work/macros$i.xml"
  podman container exists "ch$i" || podman run -d --name "ch$i" --network lhnet -p "1900$i:9000" \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$here/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:Z" \
    -v "$work/macros$i.xml:/etc/clickhouse-server/config.d/macros.xml:Z" "$img" >/dev/null
done
for i in 1 2 3 4; do
  n=0
  until clickhouse-client --port "1900$i" -q "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do
    n=$((n + 1)); [ "$n" -lt 60 ] || { echo "ch$i not ready" >&2; exit 1; }; sleep 1
  done
done
echo "cluster lokihouse up: localhost:19001-19004"
```
If `clickhouse-client` is not installed on the host, run it inside the container instead: `podman exec ch$i clickhouse-client -q …`.

`test/cluster/down.sh`:
```sh
#!/bin/sh
podman rm -f ch1 ch2 ch3 ch4 keeper >/dev/null 2>&1 || true
podman network rm lhnet >/dev/null 2>&1 || true
```

`Makefile` additions:
```make
CLUSTER_ENV := LOKIHOUSE_TEST_CH_ADDR=localhost:19001 LOKIHOUSE_TEST_CH_CLUSTER=lokihouse
.PHONY: cluster-up cluster-down test-cluster oracle-cluster
cluster-up:
	sh test/cluster/up.sh
cluster-down:
	sh test/cluster/down.sh
test-cluster: cluster-up
	$(CLUSTER_ENV) go test -count=1 ./...
oracle-cluster: cluster-up
	$(CLUSTER_ENV) go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
```

Run: `make cluster-up`
Expected: `cluster lokihouse up: localhost:19001-19004`

- [ ] **Step 2: Write the cluster-only tests**

`internal/chstore/cluster_test.go`:
```go
package chstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func requireCluster(t *testing.T) {
	if chtest.Topology().Cluster == "" {
		t.Skip("single-node run; use make test-cluster")
	}
}

// A push is acknowledged only when every replica a later read may hit has the rows.
func TestCluster_ReadAfterPush(t *testing.T) {
	requireCluster(t)
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"load_balancing": "round_robin"}))
	for i := 0; i < 40; i++ {
		seed(t, w, "t1", logproto.Stream{
			Labels:  fmt.Sprintf(`{app="a%d"}`, i%7),
			Entries: []logproto.Entry{{Timestamp: t0.Add(time.Duration(i) * time.Second), Line: "x"}},
		})
		var logs, streams uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1'").Scan(&logs))
		require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(stream_id) FROM streams WHERE tenant = 't1'").Scan(&streams))
		require.Equal(t, uint64(i+1), logs, "push %d not visible on every replica", i)
		require.Equal(t, uint64(min(i+1, 7)), streams)
	}
}

// Rows of one stream, in logs and streams, share a shard; streams spread over shards.
func TestCluster_StreamColocation(t *testing.T) {
	requireCluster(t)
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	for i := 0; i < 30; i++ {
		seed(t, w, "t1", logproto.Stream{
			Labels:  fmt.Sprintf(`{app="a%d"}`, i),
			Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}, {Timestamp: t0.Add(time.Second), Line: "y"}},
		})
	}
	ctx := context.Background()
	var split uint64
	require.NoError(t, conn.QueryRow(ctx, `SELECT count() FROM (SELECT stream_id FROM (
		SELECT stream_id, shardNum() AS s FROM logs WHERE tenant = 't1'
		UNION DISTINCT SELECT stream_id, shardNum() AS s FROM streams WHERE tenant = 't1')
		GROUP BY stream_id HAVING uniqExact(s) > 1)`).Scan(&split))
	require.Zero(t, split, "a stream's rows live on more than one shard")
	var shards uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(shardNum()) FROM logs WHERE tenant = 't1'").Scan(&shards))
	require.Equal(t, uint64(2), shards, "30 streams should hash onto both shards")
}
```
`seed` and `t0` come from `querier_test.go` (Task 6). Go 1.21+ provides the builtin `min`.

- [ ] **Step 3: Run the whole suite on the cluster**

Run: `make test-cluster`
Expected: PASS for every package. This includes the two tests above, plus the Task 6 tests `TestSelectLogs_DedupesIdenticalEntries` and `TestSelectLogs_TimeBoundaries`, which now go through `Distributed` tables. A failure that only shows up here is a cluster bug. Check these first:
- SQL naming a `_local` table
- an `IN (SELECT … FROM <Distributed>)` subquery (ClickHouse rejects it under `distributed_product_mode = 'deny'`)
- a missing setting in `Topology.Settings()`

Use **superpowers:systematic-debugging**. Do not add `skip_unavailable_shards` or `FINAL` workarounds.

- [ ] **Step 4: Run the oracle on the cluster**

`StartLokihouse` (Task 10) already writes `clickhouse.cluster` from `chtest.Topology()`, so no harness change is needed.

Run: `make oracle-cluster`
Expected: the same PASS set as `make oracle`.

- [ ] **Step 5: Commit**

```bash
git add test/cluster internal/chstore/cluster_test.go Makefile
git commit -m "test: podman ClickHouse cluster and cluster runs of unit and oracle suites"
```

---

## Phase 1 exit criteria

- `make test` and `make oracle` are green against local ClickHouse 26.9 and Loki @ `fbc3f077`.
- `make test-cluster` and `make oracle-cluster` are green against the podman 2×2 replicated cluster, with the same PASS set as the single-node runs.
- `docs/divergences.md` is empty, or holds owner-approved entries only.
- `bin/lokihouse` is usable from Grafana as a Loki datasource for Explore, logs and metrics.

## Follow-up plans (written when each phase starts; see spec §8)

- Phase 2: Tier-A SQL push-down. Planner whitelist; windowed range aggregations via `-State`/`-Merge` windows plus `WITH FILL`; vector aggregations in SQL; regex parity gate; log-query `LIMIT` push-down; property tests Tier A == Tier B. Cluster rules:
  - stream sub-selects use `streams_local` (co-located) or `GLOBAL IN`
  - window functions over `Distributed` need `-State` per shard and `-Merge` on the initiator; verify per shape with `make test-cluster`
  - evaluate `enable_parallel_replicas` for single-shard replicated setups
- Phase 3: tail (websocket), `index/stats`, `index/volume`, `volume_range` (MV → AggregatingMergeTree), `detected_labels` / `detected_fields`, `patterns` (Drain on read), `format_query`, delete API (lightweight `DELETE FROM logs_local ON CLUSTER …` + read-time filter), retention TTL from Loki retention rules (TTL is per-replica and needs no cluster handling), `stream_stats_hourly` as an MV on `logs_local` into a `Replicated`AggregatingMergeTree `_local` table plus a `Distributed` table (prototyped on the 2×2 cluster), legacy `/api/prom/*` query routes.
- Phase 4: ruler (rules API, evaluation, recording-rule remote write).
- Phase 5: rate limits, multi-tenant (`a|b`) queries, query stats mapping, timeouts, perf benchmark vs Loki, packaging (podman compose, static binary). Cluster topics:
  - `Replicated` database-engine and ClickHouse Cloud (SharedMergeTree, where plain DDL suffices) topologies
  - behaviour when a replica or shard is down (the query error must map to Loki's 5xx)
  - throughput bench on the cluster
