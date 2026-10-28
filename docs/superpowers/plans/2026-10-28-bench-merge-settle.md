# Benchmark Merge Settling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `make bench` / `make bench-s3` wait until each lokihouse store's ClickHouse has finished merging the freshly ingested parts before timing. They also record each store's active `logs` part count in the results, so S3 timings measure steady state and not the leftovers of ingest.

**Architecture:** After ingest and the Loki readiness waits, and before `saturateCaches`, the harness polls each lokihouse store's database. It waits until `system.merges` has no merge for that database and the active part count of `logs` stays unchanged for three consecutive polls, bounded by a timeout. Each store's part count, and whether it settled, goes into the results header. The harness needs each store's ClickHouse address and database, so the oracle's `startLokihouse` returns the database name too.

**Tech Stack:** Go test harness (`test/oracle`, build tags `oracle bench`), clickhouse-go v2, ClickHouse 26.9 system tables.

**Spec:** No separate spec file. The analysis (2026-10-27/28 session) measured the same 200k-line data set. On uncached S3, one part took 15 GETs / 0.7 MB and 9.7 ms for the `rate regex` logs query. 22 unmerged parts took 35 GETs / 3.7 MB, 12.3 ms, with p90 at 26 ms. On S3, every part × column read is its own GET. The benchmark pushes 40 batches of 5 000 lines and starts timing right after the last one, so ClickHouse is still merging during the timed rounds. The S3 Loki, by contrast, is fully flushed. Production needs no change: `internal/chstore/writer.go` already sends every push as an `async_insert` (`wait_for_async_insert=1`), which ClickHouse recommends for many small inserts.

## Global Constraints

- Independent of the Loki bump (`2026-10-28-loki-bump-39c256e5.md`), re-checked 2026-10-28: it touches no Loki API. It can land before or after. If it lands after, the benchmark's Loki binaries are built at the new pin, which is what the next recorded results should use anyway.
- No production code changes. This is test harness and docs only, so no `CHANGELOG.md` entry (AGENTS.md: tests and CI-internal changes need none).
- ClickHouse identifiers and values in system-table queries are bound parameters (`database = ?`), never concatenated.
- The poll interval is 1 s. A store counts as settled when it shows no merge and the same part count on 3 consecutive polls. Settling is bounded by `LOKIHOUSE_BENCH_SETTLE` (a `time.ParseDuration` value, default `2m`; `0` disables waiting but still reports part counts).
- A store that does not settle within the timeout does not fail the run. Its part count is reported with `(not settled)`.
- Every database connection the harness opens is closed via `t.Cleanup`.
- `test/oracle/bench_test.go` and `bench_s3_test.go` were being edited by another session (commit `29a349e` and later). Rebase onto the latest `main` before starting, and keep the diff in `bench_test.go` small.
- `go vet -tags oracle,bench ./test/oracle/` and `make check` pass.

## Review Focus

1. **Continuous merges never reach a stable count** (for example, a slow S3 merge still running at the deadline). Expected: the run continues after `LOKIHOUSE_BENCH_SETTLE` and reports `(not settled)`; the test never hangs or fails. Task 1's short run with `LOKIHOUSE_BENCH_SETTLE=1s` checks this path.
2. **`LOKIHOUSE_BENCH_SETTLE=0`.** Expected: no waiting, part counts still reported. Task 1 Step 4 runs it.
3. **The remote scenario** (`make bench-s3`, only `lokihouse S3 + cache`, ClickHouse from `up-remote.sh`). Expected: the store's address comes from `LOKIHOUSE_BENCH_CH_S3_CACHE` and settling works the same. Task 1 checks it with `BENCH_STORES=lokihouse-cache` when `.secrets/s3.env` exists. Otherwise it records the step as not run.
4. **Shared ClickHouse with other oracle databases** (the local-disk store uses `chAddr()`, which holds other `lh_oracle_*` databases). Expected: counts and merges are filtered to the store's own database only. The query in Task 1 Step 2 filters by `database = ?`.
5. **Loki stores** have no ClickHouse. Expected: they are skipped, with no connection attempted. Task 1 Step 2 skips stores whose `db` is empty.

---

### Task 1: Settle merges and report part counts in `TestBench`

**Files:**
- Modify: `test/oracle/lokihouse.go`: `startLokihouse` returns `(url, db string)`; add `StartLokihouseDBOn`
- Create: `test/oracle/bench_parts_test.go` (build tags `oracle && bench`)
- Modify: `test/oracle/bench_test.go`: `benchStore` fields, store construction, call site, results header
- Modify: `docs/benchmark.md`: Method and Caveats

**Interfaces:**
- Consumes: `chtest.Options(db string) *clickhouse.Options` (as `startLokihouse` already uses it), `chAddr() []string`, `chstore.Topology{}`.
- Produces:
  - `func StartLokihouseDBOn(t *testing.T, addr []string, topo chstore.Topology, extra string) (url, db string)` in `lokihouse.go`. `StartLokihouse`, `StartLokihouseLimits` and `StartLokihouseOn` keep their signatures and discard `db`.
  - `benchStore` gains `ch []string` and `db string`, both empty for Loki stores.
  - `func settleParts(t *testing.T, s benchStore, timeout time.Duration) (parts uint64, settled bool)` in `bench_parts_test.go`.
  - `func partsLine(stores []benchStore, parts []uint64, settled []bool) string` in `bench_parts_test.go`. It returns one line such as `ClickHouse active logs parts at timing start: lokihouse 3, lokihouse S3 + cache 4 (not settled)`, or `""` when no store has a database.

- [ ] **Step 1: Return the database from `startLokihouse`**

Change `startLokihouse` to return `(string, string)`: the URL and the `lh_oracle_…` name it already generates. Add `StartLokihouseDBOn`. Point the three existing wrappers at it, discarding `db`.
Run: `go vet -tags oracle ./test/oracle/ && go vet -tags oracle,bench ./test/oracle/`
Expected: no errors.

- [ ] **Step 2: Implement `settleParts` and `partsLine` in `bench_parts_test.go`**

For a store with `db == ""`, `settleParts` returns `(0, true)` without connecting. Otherwise it opens `chtest.Options("default")` with `Addr = s.ch`, closed via `t.Cleanup`. Every 1 s it runs:
```sql
SELECT (SELECT count() FROM system.parts WHERE database = ? AND table = 'logs' AND active),
       (SELECT count() FROM system.merges WHERE database = ?)
```
It returns `(parts, true)` after 3 consecutive polls with `merges == 0` and the same `parts`. At the deadline it returns `(last parts, false)`. With `timeout == 0` it polls once and returns `(parts, false)`. A query error is `t.Fatalf("parts of %s: %v", s.db, err)`.

- [ ] **Step 3: Wire it into `TestBench`**
  - Build the lokihouse stores with `StartLokihouseDBOn`, filling `ch` and `db`: `chAddr()` for `lokihouse`, `[]string{chS3}` for `lokihouse S3`, `[]string{chS3Cache}` for `lokihouse S3 + cache`.
  - Read `settle := envDuration(t, "LOKIHOUSE_BENCH_SETTLE", 2*time.Minute)`. `envDuration` rejects `0` today; accept `"0"` as zero for this variable only (an explicit check before calling it).
  - Between the Loki readiness loop and `saturateCaches`, call `settleParts` for every store. Write `partsLine(...)` as the second line of the results file, after the existing summary line, when it is not empty.

- [ ] **Step 4: Short runs** (only when no other benchmark is running on the host; check `podman ps` for `lhb-*`)

Run: `LOKIHOUSE_BENCH_LINES=20000 LOKIHOUSE_BENCH_ROUNDS=2 make bench`
Expected: PASS. `.cache/bench/results.md` line 2 names `lokihouse`, `lokihouse S3` and `lokihouse S3 + cache` with part counts and no `(not settled)`.
Run: `LOKIHOUSE_BENCH_SETTLE=0 LOKIHOUSE_BENCH_LINES=20000 LOKIHOUSE_BENCH_ROUNDS=2 make bench`
Expected: PASS. Counts are reported, typically higher than in the previous run, each marked `(not settled)`.
Run: `LOKIHOUSE_BENCH_SETTLE=1s LOKIHOUSE_BENCH_LINES=20000 LOKIHOUSE_BENCH_ROUNDS=2 make bench`
Expected: PASS within a few seconds of the ingest finishing, whatever the settle state.
If `.secrets/s3.env` exists: `BENCH_STORES=lokihouse-cache LOKIHOUSE_BENCH_LINES=20000 LOKIHOUSE_BENCH_ROUNDS=2 make bench-s3`
Expected: PASS with one part count for `lokihouse S3 + cache`.

- [ ] **Step 5: Document it in `docs/benchmark.md`**
  - **Method**, after the bullet about ingest and readiness waits: before timing, the harness waits up to `LOKIHOUSE_BENCH_SETTLE` (default `2m`, `0` to skip) until each ClickHouse store has no merge running and a stable part count. The S3 Loki is fully flushed, so settled parts are its counterpart. The results record each store's active `logs` part count.
  - **Caveats**: on S3 without cache, every part × column read is a GET. On this data set, 1 part against 22 unmerged parts was 15 against 35 GETs and 9.7 against 12.3 ms (p90 26 ms) for the `rate regex` logs query. Unsettled runs measure ingest leftovers.

- [ ] **Step 6: Gates and commit**

Run: `go vet -tags oracle,bench ./test/oracle/ && make check`
Expected: PASS.

```bash
git add test/oracle/lokihouse.go test/oracle/bench_parts_test.go test/oracle/bench_test.go docs/benchmark.md
git commit -m "test: let the benchmark wait for ClickHouse merges and report part counts"
```
