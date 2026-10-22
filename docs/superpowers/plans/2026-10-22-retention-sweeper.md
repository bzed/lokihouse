# Retention Sweeper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A changed `retention_period` / `retention_stream` applies to all stored data, via a sweeper, with a schema migration for existing databases.

**Architecture:** Schema v2 drops `retention_days` (column, TTL, partition key part) from `logs`, `streams`, `stream_stats_hourly`; tables are partitioned by UTC day only. A new `internal/retention` sweeper (Loki's `compactor.retention_enabled`) drops expired day partitions and lightweight-deletes per-stream expired days. `lokihouse -schema.migrate` copies v1 tables into v2 tables and exchanges them.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9, Loki `pkg/compactor/retention`, `pkg/validation`.

**Spec:** `docs/superpowers/specs/2026-10-22-retention-sweeper-design.md`

## Global Constraints

- Follow `AGENTS.md`: Loki's config key names exactly (`compactor.retention_enabled`; its default is `true`, unlike Loki's `false`, by the owner's decision); every observable difference goes into `docs/divergences.md` in the same commit; user-visible changes get a `CHANGELOG.md` line under `## [Unreleased]`.
- No SQL built from user input by concatenation. Day partition IDs are rendered from a `time.Time` (`Format("20060102")`), never from strings read back from ClickHouse.
- Every I/O function takes `ctx` first; goroutines are owned and waited for on shutdown; packages starting goroutines run `goleak.VerifyTestMain`.
- Tests are deterministic: inject `now`, never sleep; poll with `require.Eventually`.
- Every statement that changes data on a cluster uses the storage table (`topo.Local(name)`) with `topo.OnCluster()`. Lightweight deletes use the existing `deleteCtx` (`lightweight_deletes_sync=2`); `DROP PARTITION` uses `replication_alter_partitions_sync=2`; `APPLY DELETED MASK` uses `mutations_sync=2`.
- `IN` lists of stream IDs are batched at `streamBatch = 5_000` per statement (stays under `max_query_size`, 256 KiB).
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` and `make soak` pass before the branch is done (query, ingest and SQL code change).

## Review Focus

- A normal start against a v1 database must exit 1 with `schema version 1: stop all lokihouse instances and run lokihouse -schema.migrate` and write nothing. Test in Task 2.
- A migration killed half-way must finish on the next `-schema.migrate` run with no lost and no duplicated rows. Test in Task 2.
- A migration on a database without enough free disk space must refuse before copying, not fill the disk. Test in Task 2.
- More streams than one `IN` batch (> 5 000) must still all be swept. Test in Task 3.
- UTC day boundary: with a retention of 1 day, at `now = D+1 00:00:01Z` the rows of day D (including 23:59:59.999999999Z) are gone and those of D+1 00:00:00Z are kept. Test in Task 3.

---

### Task 1: Schema v2 and writer without `retention_days`

**Files:**
- Modify: `internal/chstore/schema.sql`, `internal/chstore/schema.go`, `internal/chstore/writer.go`, `internal/ingest/handler.go`
- Create: `internal/chstore/testdata/schema_v1.sql` (verbatim copy of today's `schema.sql`, for Task 2's tests)
- Test: `internal/chstore/writer_test.go`, `internal/chstore/schema_test.go`, `internal/ingest/handler_test.go`

**Interfaces:**
- Produces: `func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream) error` (the `retentionDays` parameter is gone).
- Produces: schema template data methods `Table(name string) string` (storage table, `Local(name)` plus the render suffix) and `Dist(name string) string` (Distributed table, `name` plus the suffix), replacing `.Local "x"` and the literal Distributed names in `schema.sql`.
- Produces: `func renderSchema(db string, topo Topology, suffix string) ([]string, error)`; `RenderSchema(db, topo)` calls it with `""`.

- [ ] **Step 1: Copy `schema.sql` to `testdata/schema_v1.sql`** and write the failing tests:
  - `TestRenderSchema_NoRetention`: no rendered statement contains `retention_days` or `TTL`; the `logs` statement contains `PARTITION BY toDate(ts)`, `streams` `PARTITION BY day`, `stream_stats_hourly` `PARTITION BY toDate(hour)`.
  - `TestRenderSchema_Suffix`: `renderSchema(db, Topology{Cluster: "c"}, "_migrate")` names the storage table `logs_migrate_local` and the Distributed table `logs_migrate` (the suffix goes before `_local`); with `Topology{}` the table is `logs_migrate`.
  - In `writer_test.go`, replace the "two retention periods double the partitions" test with `TestWrite_TooManyDays`: a push spanning 61 UTC days returns `ErrTooManyPartitions` and stores nothing; 60 days succeed.
  - In `ingest/handler_test.go`, delete the tests that read `retention_days` (lines ~90–190 today); retention is tested in Task 3.
- [ ] **Step 2: Run** `go test ./internal/chstore/ ./internal/ingest/ -run 'RenderSchema|TooManyDays'`. Expected: FAIL.
- [ ] **Step 3: Implement.** Remove `retention_days`, `TTL` and `ttl_only_drop_parts` from the three tables and the view; delete `ttlExpr` and the `ttl` template func. `Write` keys partitions by UTC day only (`partitionKey` becomes `time.Time`); error text `push spans %d UTC days, at most %d are accepted per push; split it by time range`. Drop `Handler.retentionDays` and the `retention` import from `internal/ingest/handler.go` (distcopy still uses Loki's retention for metrics, unchanged). The suffix rule for `Table`: cluster `name+suffix+"_local"`, single node `name+suffix`.
- [ ] **Step 4: Run** `go test -race ./internal/chstore/ ./internal/ingest/`. Expected: PASS.
- [ ] **Step 5: Update** the "Pushes spanning many days" entry of `docs/divergences.md` to say 60 UTC days (no retention factor); `CHANGELOG.md` Changed line. **Commit** `feat(chstore): schema v2, day partitions without retention_days`.

### Task 2: Schema version check and `-schema.migrate`

**Files:**
- Create: `internal/chstore/migrate_v2.go`, `internal/chstore/migrate_v2_test.go`
- Modify: `internal/chstore/schema.go` (`Migrate` refuses v1), `internal/server/server.go` (new `MigrateSchema`), `cmd/lokihouse/main.go` (flag), `docs/man/lokihouse.1.md`
- Test: `internal/chstore/migrate_v2_test.go`, `cmd/lokihouse/main_test.go`, `internal/server/server_test.go`

**Interfaces:**
- Consumes: `renderSchema(db, topo, suffix)` from Task 1.
- Produces: `var ErrSchemaV1 = errors.New("schema version 1: stop all lokihouse instances and run lokihouse -schema.migrate")`; `Migrate` returns it wrapped when the storage `logs` table has a `retention_days` column (`system.columns`).
- Produces: `func MigrateV2(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error` — migrates every v1 table, then calls `Migrate`. A no-op on a v2 database.
- Produces: `func MigrateSchema(ctx context.Context, cfg *config.Config, logger log.Logger) error` in `internal/server`; `cmd/lokihouse` gains `-schema.migrate` ("migrate the ClickHouse schema to the current version and exit"), exit 0 on success, 1 on error.

- [ ] **Step 1: Write the failing tests** (ClickHouse-backed; a helper `newV1(t)` renders `testdata/schema_v1.sql` with the v1 template funcs into a fresh database and inserts, for two tenants, rows with `retention_days` 0 and 7 across 3 days, the matching `streams` rows, and lets the view fill `stream_stats_hourly`):
  - `TestMigrate_RefusesV1`: `Migrate` on `newV1` returns an error matching `errors.Is(err, ErrSchemaV1)`; row counts unchanged.
  - `TestMigrateV2_CopiesEverything`: after `MigrateV2`, no table has `retention_days`; `SELECT tenant, stream_id, ts, line, structured_metadata FROM logs ORDER BY ALL` equals the v1 content; `streams` and `sum(entries), sum(bytes)` of `stream_stats_hourly` per `(tenant, stream_id, hour)` equal the v1 values; a `Writer.Write` afterwards increments `stream_stats_hourly` (view recreated); `system.tables` has no `*_migrate*` table.
  - `TestMigrateV2_Idempotent`: a second `MigrateV2` changes nothing (same counts, no error).
  - `TestMigrateV2_ResumesAfterCrash`: on `newV1`, create `logs_migrate` (Task 1 suffix) and insert half the rows into it, then run `MigrateV2`; the result equals `CopiesEverything`'s (no duplicates: compare `count()` and `uniqExact` of the sorting key). Second variant: `logs` already exchanged (v2) while the old-data table `logs_migrate` still exists; `MigrateV2` drops it.
  - `TestMigrateV2_DiskSpace`: `checkFreeSpace(need, free uint64) error` returns an error containing `not enough free disk space` when `free < need`, nil otherwise.
  - `TestRun_SchemaMigrateFlag` (`cmd/lokihouse`): `run([]string{"-config.file", p, "-schema.migrate"}, …)` returns 0 against a v1 database, and a plain `run` against a v1 database returns 1 with stderr containing `lokihouse -schema.migrate`.
- [ ] **Step 2: Run** `go test ./internal/chstore/ -run 'Migrate'` and `go test ./cmd/lokihouse/`. Expected: FAIL.
- [ ] **Step 3: Implement `MigrateV2`** in `migrate_v2.go`. Per table in order `logs`, `streams`, `stream_stats_hourly`, decided by whether `topo.Local(name)` has `retention_days`:
  1. v2 and `<name>_migrate` storage table exists: drop it (`DROP TABLE … SYNC`, ON CLUSTER) and its Distributed. Continue.
  2. v1: drop `stream_stats_hourly_local_mv` / `stream_stats_hourly_mv` (`topo.Local("stream_stats_hourly")+"_mv"`) if it exists; drop a leftover `<name>_migrate`; create it from `renderSchema(db, topo, "_migrate")` (only that table's statements); `checkFreeSpace(bytes of the table from system.parts, min free_space of system.disks)` (on a cluster: `clusterAllReplicas`); `INSERT INTO <Dist or table>_migrate (<v2 columns>) SELECT <v2 columns> FROM <name>` with `parallel_distributed_insert_select=2` on a cluster; `EXCHANGE TABLES <storage> AND <storage>_migrate` (ON CLUSTER); drop `<name>_migrate` (now old data).
  Columns: logs `tenant, stream_id, ts, line, structured_metadata`; streams `tenant, day, stream_id, labels, labels_str`; stats `tenant, stream_id, hour, entries, bytes, min_ts, max_ts`. Log one line per table (`msg="migrated table"`, `table`, `rows`). Then call `Migrate` (recreates the view). In `Migrate`, before creating tables, return `fmt.Errorf("%w (database %s)", ErrSchemaV1, db)` for a v1 database.
- [ ] **Step 4: Wire the flag.** `server.MigrateSchema` opens the admin connection like `openClickHouse` and calls `chstore.MigrateV2`; `cmd/lokihouse` calls it instead of `server.Run` when `-schema.migrate` is set. Man page: add the option.
- [ ] **Step 5: Run** `go test -race ./internal/chstore/ ./internal/server/ ./cmd/lokihouse/` and `make test-cluster`. Expected: PASS on both.
- [ ] **Step 6: Commit** `feat: schema v1 to v2 migration (-schema.migrate)` with the `CHANGELOG.md` line (Changed, breaking: run `lokihouse -schema.migrate` once after upgrading, with all instances stopped; needs free disk space for a copy of the largest table).

### Task 3: Retention sweeper

**Files:**
- Create: `internal/chstore/retention.go`, `internal/chstore/retention_test.go`, `internal/retention/sweeper.go`, `internal/retention/sweeper_test.go`, `internal/retention/main_test.go` (goleak)
- Modify: `internal/chstore/deleterows.go` (`ApplyDeletedMask` takes the table), `internal/deletes/processor.go` (caller), `internal/config/config.go`, `internal/server/server.go`, `lokihouse.example.yaml`, `docs/reference/configuration.md` (the key row; `docs_test.go` enforces it)
- Test: the files above, `internal/config/config_test.go`

**Interfaces:**
- Consumes: `Topology`, `deleteCtx` (Task-independent, existing).
- Produces in `chstore`:
  - `func retentionTables() []string` returning `logs`, `streams`, `stream_stats_hourly` (a function, not a package variable: AGENTS.md forbids mutable package state).
  - `func DayPartitions(ctx context.Context, conn driver.Conn, db string, topo Topology, table string) ([]time.Time, error)` — distinct `min_date` of active parts of the storage table (all replicas on a cluster), UTC days.
  - `func DropDay(ctx context.Context, conn driver.Conn, db string, topo Topology, table string, day time.Time) error` — `ALTER TABLE … DROP PARTITION ID '<YYYYMMDD>'`.
  - `type StreamRef struct { Tenant string; ID uint64; Labels labels.Labels }`
  - `func StreamsUpTo(ctx context.Context, conn driver.Conn, day time.Time, fn func(StreamRef) error) error` — distinct `(tenant, stream_id)` from `streams` with `day <= day`, labels from `any(labels)`.
  - `func DaysWithRows(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, upTo time.Time) ([]time.Time, error)` — distinct `toDate(ts)` from `logs`.
  - `func DeleteDays(ctx context.Context, conn driver.Conn, db string, topo Topology, table, tenant string, ids []uint64, days []time.Time) error` — lightweight `DELETE` on the storage table; day column per table: `logs` `toDate(ts)`, `streams` `day`, `stream_stats_hourly` `toDate(hour)`; batches `ids` by `streamBatch`.
  - `func ApplyDeletedMask(ctx context.Context, conn driver.Conn, db string, topo Topology, table string) error` (was logs-only).
- Produces in `config`: `Compactor.RetentionEnabled bool \`yaml:"retention_enabled"\`` (default `true`).
- Produces in `retention`: `type Sweeper struct { Conn driver.Conn; DB string; Topo chstore.Topology; Limits *validation.Overrides; Interval time.Duration; Logger log.Logger; Now func() time.Time }`, `func (s *Sweeper) Sweep(ctx context.Context, now time.Time) error`, `func (s *Sweeper) Run(ctx context.Context)` (same loop shape as `deletes.Processor.Run`).

- [ ] **Step 1: Write the failing tests.** In `internal/retention/sweeper_test.go`, ClickHouse-backed (`chtest.New`), pushing through `chstore.Writer`, `now := time.Date(2026, 10, 22, 0, 0, 1, 0, time.UTC)`, limits built with `validation.NewOverrides`:
  - `TestSweep_GlobalRetentionDropsDays`: rows on 2026-10-10…10-21, `retention_period: 7d` → after `Sweep`, `logs`, `streams`, `stream_stats_hourly` hold only days 10-15…10-21 (`D + 7 <= 10-22` removed).
  - `TestSweep_ConfigChangeAppliesToExistingData` (the bug report): sweep with `retention_period: 30d` (nothing removed), then with a new `Sweeper` using `3d` → days ≤ 10-19 gone.
  - `TestSweep_StreamRuleShorter`: `retention_period: 30d`, `retention_stream: [{selector: '{app="noisy"}', priority: 1, period: 2d}]` → `noisy` keeps 10-20, 10-21; other streams keep everything.
  - `TestSweep_StreamRuleLonger`: `retention_period: 2d`, rule `{app="audit"}` `period: 30d` → `audit` keeps all days, others keep 10-20, 10-21; no whole-day partition of 10-10…10-19 is dropped while `audit` rows are in it.
  - `TestSweep_ForeverWithRule`: `retention_period: 0s`, rule `{app="tmp"}` `period: 1d` → only `tmp` loses days ≤ 10-21; nothing dropped wholesale.
  - `TestSweep_DayBoundary`: retention 1d, rows at `2026-10-21T23:59:59.999999999Z` and `2026-10-22T00:00:00Z` → first gone, second kept.
  - `TestSweep_ManyStreams`: 5 001 streams with rule `{app="tmp"}` `period: 1d`, one old row each → all gone.
  - `TestSweep_NothingToDoIssuesNoMutation`: after one sweep, `SELECT count() FROM system.mutations WHERE database = ?` (cluster: `clusterAllReplicas`) is unchanged by a second `Sweep`.
  - `TestRun_RetentionDisabledKeepsData` (`internal/server/server_test.go`): write rows dated 30 days ago, start `server.Run` with `retention_period: 7d`, `compactor.retention_enabled: false`, `compaction_interval: 10ms`; `require.Never(rows gone, 500*time.Millisecond, 20*time.Millisecond)`. Its twin with `retention_enabled: true` uses `require.Eventually(rows gone, 30*time.Second, 50*time.Millisecond)`.
  - `config_test.go`: `compactor.retention_enabled` defaults to `true` and parses `false`.
- [ ] **Step 2: Run** `go test ./internal/retention/ ./internal/config/`. Expected: FAIL (package/field missing).
- [ ] **Step 3: Implement the chstore primitives**, then `Sweep`:
  1. Tenants come from `StreamsUpTo`; per tenant `retention.NewTenantRetentionSnapshot(s.Limits, tenant)`; days per period `ceil(period / 24h)`, capped at 65 535.
  2. `maxDays` per the spec (0 when `retention_period` is 0, else max of it and all rule periods; rule periods are read from `s.Limits.StreamRetention(tenant)`).
  3. If `maxDays > 0`: for each table, `DropDay` every day from `DayPartitions` with `day + maxDays <= today`.
  4. For each stream from `StreamsUpTo(today - 1 day)` with effective `d` where `d > 0 && (maxDays == 0 || d < maxDays)`: group by `(tenant, d)`; per group and `streamBatch`, `DaysWithRows(…, today - d days)`; if any, `DeleteDays` on the three tables and remember the tables for `ApplyDeletedMask`.
  5. `ApplyDeletedMask` per touched table. Errors are collected (`errors.Join`), each step continues, so one failing tenant does not stop the others.
- [ ] **Step 4: Wire it.** `Load` default `RetentionEnabled = true`; `server.Run` starts `(&retention.Sweeper{…, Interval: cfg.Compactor.CompactionInterval}).Run(bg)` in the existing `loops` group when enabled; it stops with `stopLoops`. Log `msg="retention sweep"` with `dropped_days`, `deleted_stream_days` at info when non-zero. Add `compactor.retention_enabled` to `lokihouse.example.yaml` (commented, `# retention_enabled: true  # Loki's default is false`, and one line on what it does) and to the `compactor` table in `docs/reference/configuration.md`.
- [ ] **Step 5: Run** `go test -race ./internal/retention/ ./internal/chstore/ ./internal/deletes/ ./internal/config/ ./internal/server/` and `make test-cluster`. Expected: PASS.
- [ ] **Step 6: Commit** `feat(retention): apply retention_period and retention_stream to stored data` with the `CHANGELOG.md` lines (Added: sweeper and `compactor.retention_enabled`, default `true` where Loki's is `false`; Changed: a changed retention now applies to stored data). Add the divergence to `docs/divergences.md`: `compactor.retention_enabled` defaults to `true`; a Loki config with `retention_period` but no `retention_enabled` expires data on lokihouse that Loki keeps.

### Task 4: Documentation (retention, runbook, upgrade, schema promise)

**Files:**
- Modify: `docs/reference/configuration.md` (Retention section), `docs/operations.md`, `docs/divergences.md`, `docs/migrating.md`, `docs/install/index.md`, `docs/index.md`, `README.md`, `debian/README.Debian`, `CHANGELOG.md`

- [ ] **Step 1: Rewrite `### Retention`** in `configuration.md`: retention is on by default (`compactor.retention_enabled: true`, unlike Loki) and `false` turns it off; the sweeper applies the *current* config every `compactor.compaction_interval`, so changes reach existing data; semantics (UTC day D removed when `D + N <= today`, from the entry's timestamp); whole-day drops when no rule is shorter than the longest period, lightweight deletes plus `APPLY DELETED MASK` for stream rules; nothing expires while no lokihouse runs.
- [ ] **Step 2: Add `## Retention` to `operations.md`** with (a) why retention is not stored per row (spec item 1: key columns cannot be mutated, so a stored value could never follow the config), (b) the runbook: free space at once with `ALTER TABLE <db>.logs_local ON CLUSTER <c> DROP PARTITION ID '20260801'` (same for `streams_local`, `stream_stats_hourly_local`; no `_local`/`ON CLUSTER` on a single server), and how to check: `SELECT partition, sum(rows) FROM system.parts WHERE database='lokihouse' AND table='logs_local' AND active GROUP BY partition ORDER BY partition`, (c) the sweeper runs on every instance and is idempotent.
- [ ] **Step 3: Upgrade note** in `install/index.md` (new `## Upgrading to 0.9`): stop every instance, `lokihouse -config.file /etc/lokihouse/lokihouse.yaml -config.expand-env -schema.migrate`, needs free disk space for a copy of the largest table, then start. Mirror it in `debian/README.Debian`.
- [ ] **Step 4: Remove the "schema may change without a migration" statements** from `docs/index.md:8`, `README.md:20`, `docs/install/index.md:306-307`, replacing them with: schema changes ship with a migration (`-schema.migrate`).
- [ ] **Step 5: Divergences and migrating.** Replace the `Retention` divergence: retention is applied by a sweeper on day partitions, whole UTC days, at most 65 535 days, removed rows leave disk at the end of the sweep; the old "changing it does not affect stored data" sentence goes. Update the `### Retention` section of `migrating.md` the same way, and warn there that a Loki config with `retention_period` set but without `retention_enabled: true` expires data on lokihouse (set `retention_enabled: false` to keep Loki's behaviour).
- [ ] **Step 6: Verify** `make docs` (mkdocs strict) and `go test ./internal/config/` (docs key coverage). Expected: PASS. **Commit** `docs: retention sweeper, runbook, schema migration`.

### Task 5: Full verification

- [ ] **Step 1:** `make check` — PASS, coverage ≥ 85 %.
- [ ] **Step 2:** `make test-cluster` — PASS.
- [ ] **Step 3:** `make oracle-all` and `make oracle-cluster` — PASS (the oracle's Loki runs with `retention_enabled: true`; lokihouse must not expire oracle data: the fixtures are recent).
- [ ] **Step 4:** `make soak` — heap and goroutine profiles flat (sweeper loop added).
- [ ] **Step 5:** Manual upgrade rehearsal: start a 0.8.0 binary on a fresh database, push data, stop it, run the new binary without the flag (exit 1, message names `-schema.migrate`), run `-schema.migrate` (exit 0), start normally, query the old data back.
