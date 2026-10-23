# Label-Filter Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Label filters reach ClickHouse indexes: pre-parser filters such as `| trace_id="x"` through new structured-metadata indexes (exact enough for Tier A), and string-equality filters after `| json` / `| logfmt` through the existing `line_text` index. Existing databases are migrated offline by `lokihouse -schema.migrate`.

**Architecture:** Schema version 3 adds to `logs` two text indexes over `mapKeys`/`mapValues(structured_metadata)` and the `line_ascii_escape` flag (true when a line hides an ASCII letter or digit behind a `\u` escape). New databases get them from `schema.sql`; `MigrateV3` adds them to version 2 databases with `ALTER TABLE` (no data copy). `MetadataFilterSQL` maps Loki's `ExtractTestableLabelMatchers` (bloom extractor) to `structured_metadata` conditions. `ParsedFilterSQL` turns post-parser equality filters into `line LIKE` conditions over the value's verbatim runs, OR the flag. Both are necessary conditions for Tier B; Tier A accepts pre-parser equality filters as exact and falls back when a resolved stream carries the label.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9 text and minmax indexes, Loki `pkg/storage/bloom/v1` (`ExtractTestableLabelMatchers`), `pkg/logql/syntax`, `pkg/logql/log`.

**Spec:** `docs/superpowers/specs/2026-10-23-clickhouse-query-review.md` (items 7–9)

**Order:** last of the four plans; uses `chtest.LogsRowsRead` (query-path fixes) and adds its fragments to the filter that `rowQuery` renders (windowed reads; before that plan lands, to `openRows`' base query).

## Global Constraints

- Follow `AGENTS.md`: reuse Loki code (the bloom extractor) rather than re-implementing filter analysis; every observable difference in `docs/divergences.md` in the same commit; user-visible changes in `CHANGELOG.md`; `pkg/storage/bloom/v1` is in Loki's module graph at the pinned commit (no new dependency).
- Tier B fragments are necessary conditions (Go re-applies the pipeline). Tier A fragments are exact or the leaf is ineligible / falls back.
- A label filter on name `k` is never pushed for a query whose resolved streams include a stream label `k` (the filter may then match the stream label).
- Only `=` with a non-empty value is pushed as a value condition; label names starting with `__` are never pushed.
- Index DDL: `INDEX sm_keys mapKeys(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1` and `INDEX sm_vals mapValues(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1`.
- Escape flag DDL (in SQL, so the literal doubles the backslashes): `line_ascii_escape Bool MATERIALIZED match(line, '\\\\u00(3[0-9]|4[1-9a-fA-F]|5[0-9aA]|6[1-9a-fA-F]|7[0-9aA])')` and `INDEX line_ascii_escape_idx line_ascii_escape TYPE minmax GRANULARITY 1`. The regex matches exactly the `\u` escapes of `0–9 A–Z a–z`; Go's `\u003c` (`<`) and `\u0026` (`&`) and non-ASCII escapes do not set it.
- Migration is offline (all lokihouse instances stopped) and copies no data. Existing rows are not backfilled: they contain no `\u` data (owner), ClickHouse computes the materialized flag on read for old parts, so it is correct for them, and the new indexes cover parts written or merged after the migration.
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`, `make soak` pass.

## Review Focus

- A version 2 database after `-schema.migrate` has exactly the columns and indexes of a freshly created one, on a single server and on the replicated cluster, including the column on the Distributed `logs` (otherwise cluster queries naming it fail). Test in Task 2.
- A normal start against a version 2 database exits with the `-schema.migrate` message and changes nothing. Test in Task 2.
- A stream label and a metadata key of the same name (`{app="b", trace_id="t1"}` stream, metadata `trace_id` absent): `| trace_id="t1"` returns that stream's lines. Tests in Tasks 3 and 4.
- `| label_format trace_id="t1" | trace_id="t1"` must not push anything (label_format changes labels before the filter). Test in Task 3.
- `{"level":"\u0065rror"}` (valid JSON, letters as escapes) and values with escapes inside them (`"alpha\nbravo"`, Go's `"a\u003cbcde"`) are still found by `| json | <label>="…"`. Test in Task 5.

---

### Task 1: Version 3 schema for new databases

**Files:**
- Create: `internal/chstore/testdata/schema_v2.sql` — verbatim copy of `internal/chstore/schema.sql` **before** this task edits it (fixture for Task 2)
- Modify: `internal/chstore/schema.sql` (column and indexes in `CREATE TABLE logs`), `internal/chstore/schema_render_test.go` (if it pins the statement list)
- Test: `internal/chstore/schema_test.go`

**Interfaces:**
- Produces: the `logs` column `line_ascii_escape` and indexes `sm_keys`, `sm_vals`, `line_ascii_escape_idx` (DDL in Global Constraints). The Distributed `logs` is created `AS` the storage table, so it gets the column. Inserts already name their columns, so the materialized column needs no writer change.

- [ ] **Step 1: Copy the fixture, then write the failing tests**

`cp internal/chstore/schema.sql internal/chstore/testdata/schema_v2.sql` first.

`TestMigrate_CreatesMetadataIndexes`: `chtest.New`; `SELECT name FROM system.data_skipping_indices WHERE database = ? AND table = ? ORDER BY name` (storage table `chtest.Topology().Local("logs")`) returns `line_ascii_escape_idx, line_text, sm_keys, sm_vals`; `SELECT line_ascii_escape FROM logs` works (the Distributed table on a cluster).

`TestLineASCIIEscapeFlag`: insert lines (Go raw string literals) `{"l":"\u0065rror"}`, `{"l":"\u004A"}`, `{"l":"a\u003cb"}`, `{"l":"\u00e9"}`, `{"l":"plain"}`; `line_ascii_escape` is `true, true, false, false, false`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'TestMigrate_CreatesMetadataIndexes|TestLineASCIIEscapeFlag' -v`
Expected: FAIL (indexes missing; unknown column `line_ascii_escape`).

- [ ] **Step 3: Add the column and indexes to `CREATE TABLE logs` in `schema.sql`**

- [ ] **Step 4: Run them on a single server and the cluster**

Run: `go test ./internal/chstore -run 'TestMigrate|TestRenderSchema' -v && make test-cluster`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore
git commit -m "feat: schema v3 indexes structured metadata and flags escaped ASCII"
```

---

### Task 2: Offline migration from version 2 (`-schema.migrate`)

**Files:**
- Create: `internal/chstore/migrate_v3.go`, `internal/chstore/migrate_v3_test.go`, `internal/chstore/chtest/v2.go`
- Modify: `internal/chstore/migrate_v2.go` (`MigrateV2` stops calling `Migrate`; `hasRetentionDays` becomes `hasColumn`), `internal/chstore/schema.go` (`Migrate` refuses version 2), `internal/chstore/migrate_v2_test.go` (call `MigrateSchema`), `internal/server/server.go` (`MigrateSchema` calls `chstore.MigrateSchema`), `docs/man/lokihouse.1.md`, `docs/install/index.md`, `debian/README.Debian` (wherever `-schema.migrate` is described: it now also upgrades version 2, without a copy)

**Interfaces:**
- Produces:
  - `var ErrSchemaV2 = errors.New("schema version 2: stop all lokihouse instances and run lokihouse -schema.migrate")` — returned (wrapped, with the database name) by `Migrate` when the storage `logs` exists without `line_ascii_escape`.
  - `func hasColumn(ctx context.Context, conn driver.Conn, db, table, column string) (bool, error)` — replaces `hasRetentionDays` (callers pass `topo.Local(name)`, `"retention_days"`).
  - `func MigrateV3(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error` — no-op unless the storage `logs` exists without the column; then one `ALTER TABLE <db>.<local logs> <OnCluster> ADD COLUMN IF NOT EXISTS …, ADD INDEX IF NOT EXISTS sm_keys …, ADD INDEX IF NOT EXISTS sm_vals …, ADD INDEX IF NOT EXISTS line_ascii_escape_idx …` with setting `alter_sync = 2`, and on a cluster `ALTER TABLE <db>.logs <OnCluster> ADD COLUMN IF NOT EXISTS line_ascii_escape <same definition>` on the Distributed table. No `MATERIALIZE`. Logs `"msg", "migrated table", "table", "logs", "to", 3`.
  - `func MigrateSchema(ctx context.Context, conn driver.Conn, db string, topo Topology, logger log.Logger) error` — `MigrateV2` (version 1 tables are copied with the current DDL, so they come out as version 3), then `MigrateV3`, then `Migrate`. `server.MigrateSchema` calls it instead of `MigrateV2`.
  - `chtest.NewV2(t testing.TB) (driver.Conn, string)` — like `NewV1`, from `testdata/schema_v2.sql`, seeded through `chstore.Writer` with two streams of tenant `t1`, three rows each.

- [ ] **Step 1: Write the failing tests**

`TestMigrate_RefusesV2`: `chtest.NewV2`; `chstore.Migrate` returns an error with `errors.Is(err, chstore.ErrSchemaV2)`; `system.data_skipping_indices` of `logs` still holds only `line_text`.

`TestMigrateSchema_V2ToCurrent`: `chtest.NewV2`; `chstore.MigrateSchema` returns nil, then `chstore.Migrate` returns nil; the six seeded rows are still returned by `{app=~".+"}`, all with `line_ascii_escape = false`; `system.columns` (`name, type, default_kind, default_expression`) and `system.data_skipping_indices` (`name, type_full, expr`) of `logs` (storage and, on a cluster, Distributed) equal those of a fresh `chtest.New` database; a row written after the migration with metadata `trace_id=x` is found through `structured_metadata['trace_id'] = 'x'`.

`TestMigrateSchema_Idempotent`: running `MigrateSchema` twice on a `NewV2` database returns nil both times.

The existing `TestMigrateV2_*` tests call `MigrateSchema` and keep passing (version 1 → 3 in one run).

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'TestMigrate' -v`
Expected: FAIL to compile (`ErrSchemaV2`, `MigrateSchema`, `chtest.NewV2` undefined).

- [ ] **Step 3: Implement `hasColumn`, `MigrateV3`, `MigrateSchema`, the `Migrate` check, `NewV2`, and switch `server.MigrateSchema`**

- [ ] **Step 4: Run them on a single server and the cluster**

Run: `go test ./internal/chstore ./internal/server -v && make test-cluster`
Expected: PASS.

- [ ] **Step 5: Docs, changelog, commit**

`CHANGELOG.md`:
- `### Added`: "Schema version 3: text indexes on structured-metadata keys and values (`sm_keys`, `sm_vals`) and the `line_ascii_escape` column."
- `### Changed`: "**Breaking:** after upgrading, run `lokihouse -schema.migrate` once with every instance stopped. From version 2 it only alters `logs` (no copy, no extra disk space); a normal start against a version 2 database exits 1 and names the command. Existing data is not re-indexed."

```bash
git add internal/chstore internal/server docs debian/README.Debian CHANGELOG.md
git commit -m "feat: migrate schema version 2 to 3 offline with -schema.migrate"
```

---

### Task 3: Pre-parser label filters → `structured_metadata` (Tier B)

**Files:**
- Create: `internal/chstore/labelfilter.go`
- Modify: `internal/chstore/querier.go` (add the fragment to the read filter after streams are resolved)
- Test: `internal/chstore/labelfilter_test.go`

**Interfaces:**
- Produces: `func MetadataFilterSQL(expr syntax.LogSelectorExpr, streams []Stream) Fragment`. Mapping of `bloomv1.ExtractTestableLabelMatchers(expr)` (all ANDed):

| matcher | fragment |
|---|---|
| `KeyValueMatcher{k, v}`, `v != ""` | `structured_metadata[?] = ?` |
| `KeyMatcher{k}` | `mapContainsKey(structured_metadata, ?)` |
| `AndLabelMatcher` | AND of both (an empty side drops out) |
| `OrLabelMatcher` | `(a OR b)`, or empty if either side is empty |
| `UnsupportedLabelMatcher`, a stream with label `k`, a `k` starting with `__` | empty |

- [ ] **Step 1: Write the failing tests**

`TestMetadataFilterSQL` (table on SQL/args, `streams = [{app="a"}]` unless stated):
`{app="a"} | trace_id="x"` → `structured_metadata[?] = ?` / `trace_id, x`;
`| trace_id=~"x|y"` → `(structured_metadata[?] = ? OR structured_metadata[?] = ?)`;
`| trace_id=~".+"` → `mapContainsKey(structured_metadata, ?)`;
`| trace_id="x" or level="y"` → OR of both;
`| json | trace_id="x"` → empty; `| label_format trace_id="x" | trace_id="x"` → empty; `| trace_id=""` → empty; streams `[{app="a", trace_id="x"}]` → empty.

`TestSelectLogs_MetadataFilter`: streams `{app="a"}` (metadata `trace_id` = `t1`, `t2`, absent, and `""`) and `{app="b", trace_id="t1"}` (no metadata); `{app=~"a|b"} | trace_id="t1"` returns the `t1` line of `a` and every line of `b`; `| trace_id=~"t1|t2"` returns both `a` lines plus `b`.

`TestMetadataFilter_UsesIndex`: 50 000 rows in one stream with unique `trace_id` metadata, `OPTIMIZE … FINAL`; `{app="a"} | trace_id="<one of them>"` returns one line and `chtest.LogsRowsRead` < 25 000.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'MetadataFilter' -v`
Expected: FAIL to compile (`MetadataFilterSQL` undefined).

- [ ] **Step 3: Implement `MetadataFilterSQL` and add it to the read filter**

- [ ] **Step 4: Run them to verify they pass**

Run: `go test ./internal/chstore -v`
Expected: PASS.

- [ ] **Step 5: Changelog and commit**

`### Changed`: "Label filters on structured metadata before any parser (`{…} | trace_id=\"…\"`) use the new metadata indexes."

```bash
git add internal/chstore CHANGELOG.md
git commit -m "feat: push pre-parser label filters to structured-metadata indexes"
```

---

### Task 4: Pre-parser equality filters in Tier A

**Files:**
- Modify: `internal/chstore/sqlbuild.go` (`ExactLineFilterSQL` → `ExactFilterSQL`), `internal/pushdown/tiera.go` (`Eligible`, `Exec`), callers in tests
- Test: `internal/chstore/sqlbuild_test.go`, `internal/pushdown/tiera_test.go`, `test/oracle/corpus/pushdown.logql`

**Interfaces:**
- Produces: `func ExactFilterSQL(expr syntax.LogSelectorExpr) (f Fragment, labelNames []string, ok bool)` — today's exact line-filter SQL, now also accepting `*syntax.LabelFilterExpr` stages whose filterer is a `*log.StringLabelFilter` with `MatchEqual` and a non-empty value, or a `*log.BinaryLabelFilter` (`And`/`Or`) of such, rendered as `structured_metadata[?] = ?`; `labelNames` lists their names. Any other stage still gives `ok = false`.
- `TierA.Exec` returns `ErrFallback` when a resolved stream has a label in `labelNames` (the stream label would satisfy the filter).

- [ ] **Step 1: Write the failing tests**

`TestExactFilterSQL`: `{a="b"} |= "x" | level="warn"` → `line LIKE ? AND structured_metadata[?] = ?`, names `[level]`, ok; `| level="warn" or level="info"` → `(structured_metadata[?] = ? OR structured_metadata[?] = ?)`; `| level=~"w.*"`, `| level!="x"`, `| level=""`, `| json | level="x"` → `ok == false`.

`TestTierA_MetadataFilter`: `count_over_time({app="a"} | level="warn" [1m])` and `rate({app="a"} | level="warn" or level="info" [1m])` equal Tier B and grow `TierALeaves`; with an extra stream `{app="a", level="warn"}` they still equal Tier B and do not grow `TierALeaves` (fallback).

`pushdown.logql`: add `range:count_over_time({app="web"} | level="error" [5m])`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore ./internal/pushdown -run 'ExactFilterSQL|TestTierA_MetadataFilter' -v`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `ExactFilterSQL` (rename and extend) and the fallback in `Exec`**

- [ ] **Step 4: Run tests and the oracle**

Run: `go test ./internal/... && make oracle-all`
Expected: PASS.

- [ ] **Step 5: Changelog and commit**

`### Changed`: "`count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` with structured-metadata equality filters (`| level=\"warn\"`) run in ClickHouse."

```bash
git add internal/chstore internal/pushdown test/oracle CHANGELOG.md
git commit -m "feat: run metric queries with metadata label filters in Tier A"
```

---

### Task 5: Post-parser equality filters after `| json` / `| logfmt`

**Files:**
- Modify: `internal/chstore/labelfilter.go`, `internal/chstore/querier.go` (add to the read filter)
- Test: `internal/chstore/labelfilter_test.go`

**Interfaces:**
- Produces: `func ParsedFilterSQL(expr syntax.LogSelectorExpr, streams []Stream) Fragment`.
  - Walks `PipelineExpr.MultiStages` in order. Allowed before a pushed filter: `*syntax.LineFilterExpr`, `*syntax.LineParserExpr` with `Op == syntax.OpParserTypeJSON` and `Param == ""`, `*syntax.LogfmtParserExpr`. Label filters before the first parser are skipped (Task 3 owns them). Any other stage ends the walk.
  - After a parser, a `*syntax.LabelFilterExpr` with `*log.StringLabelFilter` `MatchEqual`, non-empty ASCII value, name not starting with `__`, and no resolved stream labelled with the name maps to `(<runs> OR mapContainsKey(structured_metadata, ?) OR line_ascii_escape)`, where `<runs>` is the AND of `line LIKE ?` (`"%" + escapedLike(run) + "%"`) over the value's maximal runs of verbatim characters with length ≥ 4; no such run → empty. `BinaryLabelFilter` combines like Task 3.
  - Verbatim characters (written unchanged by JSON and logfmt encoders): `A–Z a–z 0–9`, space, and `` _ . , : ; = + * # @ ! ? ( ) [ ] { } | ~ ^ $ % - ``.

- [ ] **Step 1: Write the failing tests**

`TestParsedFilterSQL` (table): `{app="a"} | json | level="error"` → `(line LIKE ? OR mapContainsKey(structured_metadata, ?) OR line_ascii_escape)` / `%error%, level`; `| logfmt | msg="hello world"` → one run `%hello world%`; `| json | msg="alpha<newline>bravo"` (Go string with a real newline) → `line LIKE ? AND line LIKE ?` inside the OR (`%alpha%`, `%bravo%`); `| json | code="E1"` (no run ≥ 4) → empty; `| json | level="fehlerö"` → empty; `| json | line_format "x" | level="error"` → empty; `| regexp "(?P<level>\\w+)" | level="error"` → empty; `| json | status >= 500` → empty; streams with a `level` label → empty.

`TestSelectLogs_ParsedFilter` (lines in `{app="a"}` as Go raw string literals unless stated; query `{app=~"a|b"} | json | <filter>`):
- `{"level":"error"}` and `{"level":"info"}` with `level="error"` → only the first.
- `{"msg":"alpha\nbravo"}` with `msg="alpha<newline>bravo"` → returned.
- `{"msg":"a\u003cbcde"}` with `msg="a<bcde"` → returned.
- `{"level":"\u0065rror"}` with `level="error"` → returned (via `line_ascii_escape`).
- metadata `level=error` on line `{"level":"info"}`, filter `level="error"` → returned.
- stream `{app="b", level="error"}` line `{"level":"info"}`, filter `level="error"` → returned.
- logfmt: `level=error msg="hello world"` with `{app="a"} | logfmt | msg="hello world"` → returned.

`TestParsedFilter_UsesIndex`: 50 000 JSON lines, one with `"code":"E4711xyz"`, and 1 000 lines containing Go's `\u003c`; `{app="a"} | json | code="E4711xyz"` returns it and `chtest.LogsRowsRead` < 25 000.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'ParsedFilter' -v`
Expected: FAIL to compile (`ParsedFilterSQL` undefined).

- [ ] **Step 3: Implement `ParsedFilterSQL` and add it to the read filter**

- [ ] **Step 4: Run tests and the full gate**

Run: `go test ./internal/chstore -v && make check && make test-cluster && make oracle-all && make oracle-cluster && make soak`
Expected: all pass.

- [ ] **Step 5: Changelog and commit**

`CHANGELOG.md` `### Changed`: "Label equality filters after `| json` and `| logfmt` use the ClickHouse text index."

```bash
git add internal/chstore CHANGELOG.md
git commit -m "feat: pre-filter json/logfmt label equality filters with the text index"
```
