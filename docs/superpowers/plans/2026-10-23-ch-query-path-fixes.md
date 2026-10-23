# ClickHouse Query-Path Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream-ID sets of any size work, pushdown tests prove granule skipping, `(?i)` line filters and regex stream matchers are pushed to SQL.

**Architecture:** Stream IDs travel as a clickhouse-go external table referenced with `GLOBAL IN`, instead of a client-side interpolated literal list. A `chtest` helper sums `read_rows` from `system.query_log` for one test database, so tests assert skipping. `regexNecessary` emits `line ILIKE` fragments for safe case-folded literals; `StreamMatcherSQL` emits `match()` for `RegexExact` label regexes.

**Tech Stack:** Go, clickhouse-go v2.49 (`ext` package), ClickHouse 26.9, Loki `pkg/logql/syntax`.

**Spec:** `docs/superpowers/specs/2026-10-23-clickhouse-query-review.md` (items 1–4)

**Order:** first of four plans; the others (`…-ch-time-windowed-reads.md`, `…-ch-tiera-aggregation.md`, `…-ch-label-filter-pushdown.md`) build on it.

## Global Constraints

- Follow `AGENTS.md`: every error wrapped with context, `ctx` first on I/O functions, no SQL built from user input by concatenation (values are bound or sent as external data), user-visible changes get a `CHANGELOG.md` line under `## [Unreleased]`, observable differences from Loki go into `docs/divergences.md`.
- Pushed line/label conditions are *necessary* conditions: Go re-applies the full pipeline. They may let extra rows through, never drop a row Loki returns.
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` pass before the branch is done (query and SQL code change).
- ClickHouse-backed tests need `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`.

## Review Focus

- A selector matching 20 000 streams returns their rows (log, sample, stats and retention paths) on a single server and on the cluster. Test in Task 2.
- Stream-ID filtering by external table must still prune by primary key: one of 50 streams reads far fewer rows than all 50. Test in Task 2.
- `|~ "(?i)disk"` (contains `k`/`s`) must not be pushed: a line containing `DIS` + U+212A (KELVIN SIGN) must still be returned. Test in Task 3.
- `{app=~"a|b"}` must not match `app="ab"` (anchoring), and `app!~"a|b"` must match streams without an `app` label (empty-string semantics), exactly as Go-only matching. Test in Task 4.
- An empty stream-ID set must not send a query with an empty external table that ClickHouse rejects. Test in Task 2.

---

### Task 1: `chtest.LogsRowsRead` and a skipping-proof text-index test

**Files:**
- Modify: `internal/chstore/chtest/chtest.go`
- Modify: `internal/chstore/textindex_test.go`

**Interfaces:**
- Produces: `func LogsRowsRead(t testing.TB, conn driver.Conn, db string, since time.Time) uint64` — sum of `read_rows` of finished initial `SELECT` queries in database `db` whose text contains `FROM logs` and that started at or after `since`, after `SYSTEM FLUSH LOGS query_log`. Every later plan uses it.

- [ ] **Step 1: Rewrite `TestLineFilterSQL_UsesTextIndex` to assert skipping**

Seed 50 000 rows into one stream (`INSERT … SELECT … FROM numbers(50000)` with `line = concat('row ', toString(number), if(number = 31337, ' needle here', ''))`), then for each of the four existing queries run `SELECT count() FROM logs WHERE tenant = 't1' AND <fragment>` and assert:

```go
since := time.Now()
// run the count query with f.SQL, f.Args
require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(50000/2), q)
```

Keep `{a="b"} |= "needle" or "haystack"` and the regex cases. Use `OPTIMIZE TABLE … FINAL` after the insert so the rows are one part.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/chstore -run TestLineFilterSQL_UsesTextIndex -v`
Expected: FAIL to compile, `undefined: chtest.LogsRowsRead`.

- [ ] **Step 3: Implement `LogsRowsRead` in `internal/chstore/chtest/chtest.go`**

Query `system.query_log` with `type = 'QueryFinish' AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= ? AND position(query, 'FROM logs') > 0`, summing `read_rows`. On a cluster (`Topology().Cluster != ""`) read `clusterAllReplicas(<cluster>, system, query_log)` after `SYSTEM FLUSH LOGS ON CLUSTER`; the initiator row carries the total.

- [ ] **Step 4: Run it to verify it passes**

Run: `go test ./internal/chstore -run TestLineFilterSQL_UsesTextIndex -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/chtest/chtest.go internal/chstore/textindex_test.go
git commit -m "test: prove text-index pushdown skips granules"
```

---

### Task 2: Stream-ID sets as external tables

**Files:**
- Create: `internal/chstore/streamset.go`
- Modify: `internal/chstore/querier.go` (`openRows`), `internal/chstore/buckets.go` (`BucketCounts`), `internal/chstore/stats.go` (`HourlyStats`), `internal/chstore/deleterows.go` (`ScanRows`), `internal/chstore/retention.go` (`DaysWithRows`)
- Test: `internal/chstore/streamset_test.go`

**Interfaces:**
- Produces:
  - `const streamIDsIn = "stream_id GLOBAL IN (SELECT id FROM lh_stream_ids)"` — the SQL condition every caller uses in place of `stream_id IN ?`.
  - `func withStreamIDs(ctx context.Context, ids []uint64) (context.Context, error)` — returns `ctx` carrying the external table `lh_stream_ids (id UInt64)` via `clickhouse.Context(ctx, clickhouse.WithExternalTable(t))`. Callers return early on `len(ids) == 0` before calling it (all five already do, or must).
- `DeleteRows`, `DeleteRange`, `DeleteDays` keep their batched literal lists (mutations do not take external data).

- [ ] **Step 1: Write the failing tests**

In `streamset_test.go`:
- `TestManyStreams_AllPathsWork`: write 20 000 streams `{app="many", n="<i>"}` with one entry each at `t0` (one `Writer.Write` call), then assert: `Querier.SelectLogs` for `{app="many"}` (FORWARD, limit 30 000, iterated directly, not through the engine's entry limits) yields 20 000 distinct `StreamHash()` values; the instant query `sum(count_over_time({app="many"}[1m]))` via `run` returns 20 000 (a single series stays under `max_query_series`); `Querier.IndexStats` reports `Streams == 20000`; `ScanRows` visits 20 000 rows; `DaysWithRows` returns `[t0's day]`.
- `TestStreamSet_PrunesByPrimaryKey`: 50 streams × 20 000 rows (insert via `INSERT … SELECT` from `numbers`, `stream_id` from `chstore.StreamIdentity`), query one stream through the querier, assert `chtest.LogsRowsRead` < 100 000.
- `TestStreamSet_EmptyIsNoQuery`: `HourlyStats` and `BucketCounts` with `ids = nil` return `nil, nil`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'TestManyStreams|TestStreamSet' -v`
Expected: `TestManyStreams_AllPathsWork` fails with `Max query size exceeded` (code 62); `TestStreamSet_EmptyIsNoQuery` fails on the nil-ids case.

- [ ] **Step 3: Implement `withStreamIDs` and switch the five queries to `streamIDsIn`**

Build the table with `ext.NewTable("lh_stream_ids", ext.Column("id", column.Type("UInt64")))` and `Append` each ID. `GLOBAL IN` is required on a cluster (the initiator ships the set to the shards) and behaves as `IN` on a single server. Keep IDs sorted (they already are via `sortedIDs`).

- [ ] **Step 4: Run them to verify they pass, single server and cluster**

Run: `go test ./internal/chstore -run 'TestManyStreams|TestStreamSet' -v && make test-cluster`
Expected: PASS; `TestStreamSet_PrunesByPrimaryKey` shows the set is used for key analysis.

- [ ] **Step 5: Changelog and commit**

`CHANGELOG.md` under `## [Unreleased]` → `### Fixed`: "Queries whose selector matches more than about 13 000 streams no longer fail with `Max query size exceeded`."

```bash
git add internal/chstore CHANGELOG.md
git commit -m "fix: send stream ID sets as external data, not as SQL literals"
```

---

### Task 3: Push `(?i)` literals as `line ILIKE`

**Files:**
- Modify: `internal/chstore/sqlbuild.go` (`regexNecessary`, new helper)
- Test: `internal/chstore/sqlbuild_test.go`, `internal/chstore/textindex_test.go`, `internal/chstore/querier_test.go`

**Interfaces:**
- Produces: `func foldLiteralFragment(r []rune) Fragment` — for a case-folded literal: `Fragment{}` if any rune is ≥ 0x80 or is `k`, `K`, `s`, `S`; otherwise the AND of `line ILIKE ?` (`"%" + escapedLike(run) + "%"`) over the maximal `[A-Za-z0-9]` runs of length ≥ 4, `Fragment{}` if there is none.
- `regexNecessary` no longer returns early on `hasFoldCase`: for each literal node it uses `foldLiteralFragment` when the node's `Flags&FoldCase != 0`, `litFragment` otherwise. `hasFoldCase` is deleted if unused. `exactLineFilterSQL` is unchanged (`RegexExact` still rejects `(?i)`, so Tier A stays ineligible).

- [ ] **Step 1: Write the failing tests**

`TestLeadingLineFilterSQL_FoldCase` (table test on `LeadingLineFilterSQL(...).SQL/Args`):

| query | SQL | args |
|---|---|---|
| `{a="b"} \|~ "(?i)error"` | `line ILIKE ?` | `%error%` |
| `{a="b"} \|~ "(?i)connection refused"` | `line ILIKE ?` | `%connection%` (`refused` holds an `s`, so its run is dropped) |
| `{a="b"} \|~ "(?i)disk"` | `` (empty) | — |
| `{a="b"} \|~ "(?i)fehlerö"` | `` | — |
| `{a="b"} \|~ "(?i)err"` | `` | — |
| `{a="b"} \|~ "(?i)(error\|fatal)"` | `(line ILIKE ? OR line ILIKE ?)` | `%error%`, `%fatal%` |

Add `{a="b"} |~ "(?i)needle"` to the loop in `TestLineFilterSQL_UsesTextIndex` (rows-read assertion).

`TestSelectLogs_FoldCaseNotLost` in `querier_test.go`: lines `"ERROR one"`, `"Error two"`, `"DIS\u212A full"` (Go interpreted string: `DIS` + KELVIN SIGN U+212A), `"nothing"`; `{app="a"} |~ "(?i)error"` returns the first two, `{app="a"} |~ "(?i)disk"` returns the Kelvin-sign line.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'FoldCase|UsesTextIndex' -v`
Expected: FAIL, fold-case rows produce empty SQL.

- [ ] **Step 3: Implement `foldLiteralFragment` and the per-node choice in `regexNecessary`**

- [ ] **Step 4: Run them to verify they pass**

Run: `go test ./internal/chstore -v`
Expected: PASS.

- [ ] **Step 5: Changelog and commit**

`### Changed`: "Case-insensitive line filters (`|~ \"(?i)…\"`, Grafana Logs Drilldown's search) use the ClickHouse text index."

```bash
git add internal/chstore CHANGELOG.md
git commit -m "feat: push case-insensitive literal line filters to the text index"
```

---

### Task 4: Regex stream matchers in SQL

**Files:**
- Modify: `internal/chstore/sqlbuild.go` (`StreamMatcherSQL`)
- Test: `internal/chstore/sqlbuild_test.go`, `internal/chstore/querier_test.go`

**Interfaces:**
- `StreamMatcherSQL(ms) (Fragment, []*labels.Matcher)` keeps its signature. For `MatchRegexp`/`MatchNotRegexp` with `RegexExact(m.Value)` it adds `match(labels[?], ?)` / `NOT match(labels[?], ?)` with the pattern `"^(?:" + m.Value + ")$"`, **and** still returns the matcher in the Go-side list.

- [ ] **Step 1: Write the failing tests**

Extend `TestStreamMatcherSQL`: `{a=~"x|y", b!~"z|w", c=~"q.*"}` → SQL `match(labels[?], ?) AND NOT match(labels[?], ?)`, args `a, ^(?:x|y)$, b, ^(?:z|w)$`; all three matchers in the Go-side list (`c` is not pushed: `RegexExact` does not allow `.`).

`TestResolveStreams_RegexSameAsGo` in `querier_test.go`: streams `{app="a"}`, `{app="b"}`, `{app="ab"}`, `{other="x"}`; for `{app=~"a|b"}`, `{other=~".*", app!~"a|b"}` (missing labels match `""`) and `{app=~"a|ab", other!="x"}`: the set of returned stream labels equals the set from filtering all streams in Go with the same matchers.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'StreamMatcherSQL|RegexSameAsGo' -v`
Expected: `TestStreamMatcherSQL` FAILS (no SQL for regexes); the resolve test passes already (it pins behaviour).

- [ ] **Step 3: Implement the regex branch in `StreamMatcherSQL`**

- [ ] **Step 4: Run them to verify they pass**

Run: `go test ./internal/chstore -v`
Expected: PASS.

- [ ] **Step 5: Full verification and commit**

Run: `make check && make test-cluster && make oracle-all && make oracle-cluster`
Expected: all pass, coverage ≥ 85 %.

```bash
git add internal/chstore
git commit -m "perf: evaluate exact regex stream matchers in ClickHouse"
```
