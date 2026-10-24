# Tier B: dedup in ClickHouse, parallel sample extraction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the Tier B (Loki-engine) metric query path, e.g. the bench's
`quantile_over_time(0.95, {env="prod"} | logfmt | unwrap duration(duration) [5m]) by (app)`
(155 ms vs Loki's 69 ms), by letting ClickHouse remove duplicates for metric reads,
removing per-row allocations from the Go dedup, and extracting samples on
`GOMAXPROCS` goroutines.

**Architecture:** Metric reads (`SelectSamples`) add `FINAL` to the row SELECT;
log reads (`SelectLogs`, keyset chunks) stay without `FINAL` and keep a Go dedup
that no longer allocates per row. `SelectSamples` becomes a pipeline: one
producer goroutine scans rows into batches, N workers (each with its own Loki
`SampleExtractor`, streams pinned to one worker) extract samples, and the
iterator hands them to Loki's engine in the original row order.

**Tech Stack:** Go 1.27, clickhouse-go v2, Loki `logql/log` at the pinned commit,
ClickHouse 26.9.

**Spec:** this plan's Background section (findings of the 2026-10-24 profiling
session; no separate spec document).

## Background (measured 2026-10-24, ClickHouse 26.9, bench data)

- Profile of the bench query (135 ms CPU per query, one goroutine): `rowReader.next`
  56 ms (Map column scan ~20, string scan ~9, dedup map/`labels.FromMap`/key
  concat ~27), Loki logfmt+unwrap ~35, Loki `quantileOverTime` sort ~34, labels
  string + hash ~7, plus GC. ClickHouse itself ~20 ms.
- `logs` is `ReplacingMergeTree ORDER BY (tenant, stream_id, ts, entry_hash)`,
  `entry_hash = cityHash64(line, mapSort(structured_metadata))`. Background
  merges already collapse rows by this key, so `FINAL` only makes the merged
  result independent of merge state. Duplicates share `ts`, hence the day
  partition: `do_not_merge_across_partitions_select_final = 1` is safe (Tier A,
  `buckets.go:54`, relies on the same). On a cluster `Distributed` shards by
  `cityHash64(tenant, stream_id)`, so duplicates are shard-local.
- ClickHouse cost of `FINAL` (bench window, ms p50):

  | state | full read plain/FINAL | 1000-row keyset chunk plain/FINAL |
  |---|---|---|
  | merged | 17 / 16 | 3 / 4 |
  | 41 unmerged parts, no dups | 10 / 14 | 5 / 10 |
  | 84 parts, every line 3× | 54 (300k rows to Go) / 83 | 6 / **75** |

  `FINAL` defeats top-N sorting for keyset chunks on unmerged data, so log reads
  must not use it. `LIMIT 1 BY` was worse than `FINAL` for keyset chunks even on
  merged data (17 vs 4 ms) and is not used.
- End-to-end bench query p50: merged 176 → 153 ms with `FINAL`; 3× duplicated
  and unmerged 326 → 208 ms. Results byte-identical.
- 26.9 defaults `use_skip_indexes_if_final = 1` and
  `use_skip_indexes_if_final_exact_mode = 1`, so text-index pushdown keeps
  working under `FINAL` (Task 2 pins this with a test).
- Loki's extractors are not safe for concurrent use: `LogfmtParser` holds one
  shared `*logfmt.Decoder`, and `ForStream` caches in an unguarded map. Every
  call of `syntax.SampleExpr.Extractor()` builds fresh stages, so one extractor
  per worker is safe. `stats.Context.AddPostFilterLines` is atomic.
- What stays serial after this plan: the producer (`rows.Next`/`Scan`, ~30 ms
  on the bench) and Loki's range-vector evaluation (`quantileOverTime`, ~35 ms).
  They overlap with each other and with extraction. Splitting the engine work
  (time-split leaves) is out of scope.

## Global Constraints

- `make check` passes (gofmt/goimports, vet, golangci-lint, govulncheck, `-race` tests, coverage ≥ 85 %).
- Query/SQL changes also pass `make test-cluster`, `make oracle-all`, `make oracle-cluster`; long-lived path changes pass `make soak`.
- No new third-party dependency (`xxhash`, `goleak`, `testify` are already used).
- Every goroutine has an owner that stops it; `internal/chstore` gets `goleak.VerifyTestMain`.
- No panics on the query path; errors wrapped with context; contexts passed on.
- Every observable difference from Loki goes in `docs/divergences.md` in the same commit; user-visible changes get a `CHANGELOG.md` line under `## [Unreleased]`.
- Conventional commit prefixes only (`feat:`, `fix:`, `test:`, `docs:`, `build:`, `ci:`, `refactor:`), one logical change each.
- Log reads (`SelectLogs`, `keysetReader`) never use `FINAL`.

## Review Focus

1. Queries over one or a few streams: workers are capped at the stream count, and a single-stream metric query must not get slower than today (pinned in Task 3 by a 1-stream equality test and in Task 4 by the bench `log filter`/`rate regex` rows).
2. Iterator closed early, or the query context cancelled mid-read (query timeout, max-series errors): every pipeline goroutine stops and the ClickHouse query is cancelled; `Next` never hangs (Task 3 tests, goleak in TestMain).
3. A ClickHouse error mid-stream surfaces through `Err()` after the rows already delivered, not as a hang or a silently short result (Task 3, cancelled-parent-context test).
4. Pending delete requests with several workers: each worker's extractor gets `utildeletion.SetupExtractor` (Task 3 runs the delete test with `Workers: 4`).
5. Rows that differ only in structured metadata are not duplicates: `FINAL` and the Go dedup must both keep them (Task 1 and Task 2 tests).

---

### Task 1: Go dedup without per-row allocation

`rowReader.next` (`internal/chstore/rowiter.go:32-53`) allocates a fresh
`groupSeen` map for nearly every row, converts metadata with `labels.FromMap`,
and builds a `line+"\xff"+md.String()` key. Rows arrive ordered by
`ts, stream_id, entry_hash`, so duplicates are adjacent within a
`(ts, stream_id, entry_hash)` group. Keep a reused slice of the group's rows and
compare each new row of the same group by `line` and `maps.Equal` on the raw
metadata. A hash collision (A, B, A in one group) is still caught. Metadata
stays a `map[string]string` in `row`; conversion to `labels.Labels` moves to the
consumers.

**Files:**
- Modify: `internal/chstore/rowiter.go` (`row.md` type, `rowReader` group state, `next`)
- Modify: `internal/chstore/querier.go` (`entryIter.Next`: `labels.FromMap(r.md)` before `ProcessString`)
- Modify: `internal/chstore/samples.go` (`sampleIter.Next`: same conversion)
- Modify: any other `row.md` reader the compiler reports (`keyset.go` copies rows only)
- Test: `internal/chstore/rowiter_internal_test.go`

**Interfaces:**
- Produces: `row.md map[string]string` (raw ClickHouse map, nil or empty when none). Task 3's workers convert it with `labels.FromMap`.
- `rowReader` fields `groupTS time.Time`, `groupSID uint64`, `groupHash uint64`, `group []row` (reused, `group[:0]` on a new group) replace `groupSeen`.

- [ ] **Step 1: Write the guard tests** in `rowiter_internal_test.go`, same shape as `TestRowReader_DedupesIdenticalRows` (literal `UNION ALL` rows, `entry_hash` given literally):
  - `TestRowReader_DedupesHashCollisionGroup`: rows `(1, ts1, 'x', {}, 7)`, `(1, ts1, 'y', {}, 7)`, `(1, ts1, 'x', {}, 7)` → `next()` yields `x`, `y`, then false.
  - `TestRowReader_KeepsMetadataOnlyDifference`: rows `(1, ts1, 'x', {'a':'1'}, 7)`, `(1, ts1, 'x', {'a':'2'}, 7)` → both yielded.
  - `TestRowReader_NewGroupResetsSeen`: rows `(1, ts1, 'x', {}, 7)`, `(1, ts2, 'x', {}, 7)` → both yielded.

  `UNION ALL` order is not guaranteed in ClickHouse: give each branch an ordinal column `n` and select the five scanned columns from the union `ORDER BY n`, so the A, B, A order is fixed.

- [ ] **Step 2: Run them against the current code**

  Run: `go test -race -run 'TestRowReader_' ./internal/chstore/`
  Expected: PASS (they pin today's behaviour; this task is a refactor).

- [ ] **Step 3: Rewrite the dedup in `rowReader.next`** as described above; scan metadata into `r.curRow.md` directly (no `labels.FromMap` in `rowReader`). `lastRow` keeps its keyset role.

- [ ] **Step 4: Run the package tests**

  Run: `go test -race ./internal/chstore/ ./internal/tail/ ./internal/api/`
  Expected: PASS, including `TestRowReader_*`, `TestSelectLogs_DedupesIdenticalEntries`, `TestSelectSamples_SameResultForAnyWindowSize`, `TestSelectLogs_SameResultForAnyWindowSize`.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/
git commit -m "refactor: remove duplicates without per-row allocations"
```

---

### Task 2: Metric reads remove duplicates with FINAL

**Files:**
- Modify: `internal/chstore/windows.go` (`rowQuery`, `rowQuery.sql`)
- Modify: `internal/chstore/querier.go` (`SelectSamples` sets `final: true`; `openRows` unchanged for logs)
- Test: `internal/chstore/windows_internal_test.go`, `internal/chstore/textindex_test.go`, `internal/chstore/querier_test.go`
- Modify: `docs/divergences.md`, `CHANGELOG.md`

**Interfaces:**
- Produces: `rowQuery.final bool`. When true, `sql(w)` renders `FROM logs FINAL WHERE …` and ends with `ORDER BY ts ASC, stream_id ASC, entry_hash SETTINGS do_not_merge_across_partitions_select_final = 1`. `keysetReader` never sees `final: true` (it inserts a predicate before `ORDER BY` and appends `LIMIT ?`); say so in the field comment, together with the reason (FINAL defeats top-N on unmerged parts, 75 vs 6 ms measured).

- [ ] **Step 1: Write the failing tests**
  - `windows_internal_test.go` `TestRowQuery_FinalRendering`: `rowQuery{tenant: "t", final: true}.sql(window{1, 2})` contains `"FROM logs FINAL WHERE"` and has suffix `" SETTINGS do_not_merge_across_partitions_select_final = 1"`; with `final: false` contains neither `FINAL` nor `SETTINGS`.
  - `querier_test.go` `TestSelectSamples_KeepsMetadataOnlyDifference`: seed, in two separate `seed` calls, `{app="a"}` entries at `t0` with line `"x"` and structured metadata `trace_id=1`, then `trace_id=2`; plus the first one again in a third call. `count_over_time({app="a"}[1m])` at `t0+1m` = 2.
  - `textindex_test.go` `TestSelectSamples_FinalKeepsTextIndex`: write 50 000 lines to `{a="b"}` through `chstore.Writer` (no `OPTIMIZE`), exactly one containing `needle`; run `count_over_time({a="b"} |= "needle" [1h])` through `engine(t, &chstore.Querier{Conn: conn})` and assert the count is 1 and `chtest.LogsRowsRead(t, conn, db, since) < 25000`.

- [ ] **Step 2: Run them**

  Run: `go test -race -run 'TestRowQuery_FinalRendering|TestSelectSamples_KeepsMetadataOnlyDifference|TestSelectSamples_FinalKeepsTextIndex' ./internal/chstore/`
  Expected: `TestRowQuery_FinalRendering` FAILS (no `final` field / no `FINAL`); the other two pass and pin the behaviour across the change.

- [ ] **Step 3: Implement** `rowQuery.final` and set it in `SelectSamples` (copy `r.rq`, set `final`, use it in the `open` closure). Leave the Go dedup on: under `FINAL` it finds no duplicate groups and costs one key comparison per row.

- [ ] **Step 4: Run tests**

  Run: `go test -race ./internal/chstore/ ./internal/pushdown/ ./internal/api/`
  Expected: PASS.

- [ ] **Step 5: Document**
  - `docs/divergences.md`, query section, new bullet:
    `- **Duplicate entries in metric queries:** Loki drops an entry as a duplicate when stream, timestamp, line and structured metadata are equal. Metric queries that run in lokihouse's engine let ClickHouse remove duplicates (`FINAL`), which compares a 64-bit hash of line and structured metadata instead (`entry_hash`, the table's merge key). Two different entries are merged only if they share stream, nanosecond timestamp and hash. Background merges already apply the same rule to stored data.`
  - `CHANGELOG.md` under `## [Unreleased]` → `### Changed`:
    `- Metric queries that run in lokihouse's own engine (e.g. `| unwrap`, `quantile_over_time`) let ClickHouse remove duplicate entries (`FINAL`) instead of lokihouse; much faster when retried pushes left duplicates in unmerged parts.`

- [ ] **Step 6: Cluster run**

  Run: `make test-cluster`
  Expected: PASS (`FINAL` on the `Distributed` `logs` table; Tier A already does this).

- [ ] **Step 7: Commit**

```bash
git add internal/chstore/ docs/divergences.md CHANGELOG.md
git commit -m "feat: metric queries remove duplicates in ClickHouse with FINAL"
```

---

### Task 3: Extract samples on GOMAXPROCS workers

Replace `sampleIter` with an ordered pipeline in a new file:

```
producer goroutine:  windowReader.next() → batch.rows (≤ sampleBatchRows),
                     batch.worker[i] = worker of rows[i].streamID
                     (assigned round-robin on first sight, map owned by producer)
                     → send batch to every worker's channel, then to ordered channel
worker w (N of them): for i where batch.worker[i]==w:
                       se := own ForStream cache[streamID] (from own SampleExtractor)
                       ProcessString(ts, line, labels.FromMap(md)) → batch.out[i]
                       {ok, value, lbls = s.Labels.String(), hash = xxhash(lbls, line)}
                     add post-filter count once per batch; batch.done.Done()
consumer (Next):     take batch from ordered channel, batch.done.Wait(),
                     yield out[i] with ok in index order
```

A stream always goes to the same worker, and each worker handles its batches
in channel order, so each per-stream extractor sees exactly the line sequence
it sees today and the output order is the row order. Results are identical to
the sequential iterator for every worker count.

**Files:**
- Create: `internal/chstore/samplepipe.go` (pipeline: batch type, producer, workers, iterator)
- Delete: `internal/chstore/samples.go` (the old sequential `sampleIter`; `samplepipe.go` replaces it)
- Modify: `internal/chstore/querier.go` (`Querier.Workers`, `SelectSamples` builds one extractor per worker, cancelable read context)
- Modify: `internal/chstore/export_test.go` (`OpenedSampleWindows`)
- Create: `internal/chstore/main_test.go` (goleak)
- Test: `internal/chstore/samplepipe_test.go`, `internal/chstore/querier_test.go`
- Modify: `docs/operations.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `row.md map[string]string` (Task 1); `rowQuery.final` (Task 2).
- Produces:
  - `Querier.Workers int`: sample-extraction goroutines per metric query; 0 means `runtime.GOMAXPROCS(0)` read at query time. The effective count is `min(that, number of resolved streams)`, at least 1. No config key (Loki has none); `GOMAXPROCS` (cgroup-aware since Go 1.25) is the operator's knob.
  - `const sampleBatchRows = 1024`, `const sampleBatchesInFlight = 4` (channel capacities; bounds memory to about `sampleBatchesInFlight × sampleBatchRows` rows).
  - `func newSampleIter(ctx context.Context, cancel context.CancelFunc, rr rowSource, byID map[uint64]Stream, extractors []log.SampleExtractor, st *stats.Context) *sampleIter`: goroutines start on the first `Next`, not here.
  - `*sampleIter` implements `iter.SampleIterator` (`Next`, `At`, `Labels`, `StreamHash`, `Err`, `Close`); `StreamHash` is the row's `stream_id` as today; `Hash` equals today's `xxhash.Sum64String(lbls + line)` (compute with an `xxhash.Digest`, no concatenation).
  - `sampleIter.rr rowSource` stays a field, so `OpenedSampleWindows` keeps working; it may be read only after `Close` returned.
  - `Close()` cancels the read context, waits for producer and workers, closes `rr` (producer-owned; closed by the producer on exit), is idempotent, returns nil. `Err()` returns the producer's read error, but not the `context.Canceled` that the iterator's own `Close` caused.

- [ ] **Step 1: Write the failing tests**
  - `main_test.go` (package `chstore_test`): `goleak.VerifyTestMain(m, leakIgnores()...)` with the bce-sdk-go ignore copied from `internal/api/main_test.go`, comment included.
  - `samplepipe_test.go` `TestSelectSamples_SameResultForAnyWorkerCount`: seed 12 streams `{app="a0".."a11", env=dev|prod}` × 300 logfmt lines `level=… duration=…ms` over 3 h, plus three re-pushes of one entry (duplicates). For each query in
    `count_over_time({env=~".+"}[5m])`,
    `sum by (level) (rate({env=~".+"} | logfmt [5m]))`,
    `quantile_over_time(0.95, {env="prod"} | logfmt | unwrap duration(duration) [5m]) by (app)`,
    `count_over_time({app="a1"} | logfmt [5m])` (one stream):
    the result with `Workers: 1` equals the results with `Workers: 3` and `Workers: 64`, each also with `WindowRows: 7` (`require.Equal` on `res.Data`). Also compare `PostFilterLines` from `stats.FromContext` between `Workers: 1` and `Workers: 3` for the logfmt query.
  - `samplepipe_test.go` `TestSelectSamples_CloseMidStream`: 20 000 rows in one stream set; `SelectSamples` with `Workers: 4`; one `Next`, then `Close` returns within 10 s (`select` with `time.After`), `Next` afterwards is false and `Err()` is nil. A second iterator closed without any `Next` also returns nil. goleak in `TestMain` proves the goroutines stopped.
  - `samplepipe_test.go` `TestSelectSamples_CancelledContext`: start iterating with `Workers: 4`, cancel the parent context after the first `Next`; draining `Next` ends (no hang) and `Err()` wraps `context.Canceled` (`errors.Is`).
  - `querier_test.go`: make `TestQuerier_HidesPendingDeletes` run its sample assertions for `Workers` in `{0, 4}`.

- [ ] **Step 2: Run them**

  Run: `go test -race -run 'TestSelectSamples_|TestQuerier_HidesPendingDeletes' ./internal/chstore/`
  Expected: FAIL to compile (`Querier.Workers` undefined).

- [ ] **Step 3: Implement** `samplepipe.go` and the `SelectSamples` changes:
  - In `SelectSamples`, build the extractors with `expr.Extractor()` + `utildeletion.SetupExtractor(p, …)` once per worker, before returning, so an extractor error is returned synchronously as today; keep the `extractor == nil → NoopSampleIterator` case.
  - Read context: `context.WithCancel(sctx)` as in `SelectLogs`; the cursor opens with it.
  - Producer and workers select on `ctx.Done()` for every channel send, so a `Close` while the consumer is gone cannot block them.
  - Pass the `stats.Context` into workers; add post-filter lines once per batch per worker.

- [ ] **Step 4: Run the tests with the race detector**

  Run: `go test -race -count=3 ./internal/chstore/`
  Expected: PASS, no goleak report. If goleak reports clickhouse-go's idle-connection goroutine, a test opened a `clickhouse.Open` connection without closing it: close it, do not add an ignore.

- [ ] **Step 5: Run the dependent packages**

  Run: `go test -race ./internal/pushdown/ ./internal/api/ ./internal/ruler/ ./internal/patterns/ ./internal/multitenant/`
  Expected: PASS.

- [ ] **Step 6: Document**
  - `docs/operations.md`: a short paragraph under the query/performance section: metric queries that lokihouse evaluates itself (parsers, `unwrap`) parse lines on up to `GOMAXPROCS` goroutines per query (Go follows the cgroup CPU quota). Set the `GOMAXPROCS` environment variable to limit that.
  - `CHANGELOG.md` `### Changed`: `- Metric queries that run in lokihouse's own engine parse and extract samples on up to GOMAXPROCS cores per query.`

- [ ] **Step 7: Commit**

```bash
git add internal/chstore/ docs/operations.md CHANGELOG.md
git commit -m "feat: extract metric-query samples on GOMAXPROCS workers"
```

---

### Task 4: Verify the whole change

No code; record the numbers in the final report. Do not edit `docs/benchmark.md` unless asked (it records release runs).

- [ ] **Step 1:** `make check`. Expected: PASS, coverage ≥ 85 %.
- [ ] **Step 2:** `make test-cluster`, `make oracle-all`, `make oracle-cluster`. Expected: PASS.
- [ ] **Step 3:** `make soak`. Expected: PASS; heap and goroutine profiles flat (no growth from the pipeline).
- [ ] **Step 4:** `make bench`. Expected: all comparisons pass. `unwrap quantile` lokihouse p50 well below 155 ms (target < 100 ms: the serial producer, about 30 ms, and Loki's engine, about 35 ms, now overlap); `log filter`, `rate regex`, `count_over_time`, `topk`, `bytes_over_time` no slower than the 0.11.0 row in `docs/benchmark.md`.
- [ ] **Step 5:** CPU-profile the bench query (`/debug/pprof/profile` while looping it) and confirm the remaining serial path: `rowReader.next` (Map/string scan) on the producer and `quantileOverTime` on the engine goroutine. Report both with ms per query as input for the next steps (scanning metadata as key/value arrays; time-split leaves).
