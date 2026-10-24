# Review: implementation `2026-10-24-tierb-dedup-final-parallel` (HEAD `7511cb8`)

Reviewer: Mistral Vibe (second pair of eyes)
Date: 2026-10-24
Branch: `tierb-dedup-final-parallel` — implementation commits `c4ab98c` (Go dedup
without per-row allocations), `37638ec` (FINAL on metric reads), `cdaf6f6`
(parallel sample extraction), `7031670` (test helper), `7511cb8` (short-result
error fix), on top of the plan `cb9a2e3`.
Skills applied: `clickhouse-best-practices` (31 rules, cited per rule) and
`clickhouse-architecture-advisor` (workload classification, decision rules,
provenance labels).

The plan this implements was reviewed in the previous REVIEW.md, preserved in
git history at `4e76923`.

## 1. Scope and method

The branch changes 18 files: `internal/chstore/{rowiter,querier,windows}.go`
modified, `samples.go` deleted, `samplepipe.go` + `main_test.go` +
`samplepipe_internal_test.go` added, five test files extended,
`docs/divergences.md`, `docs/operations.md`, `CHANGELOG.md` updated, plus the
plan document. No schema, ingest, ruler or SQL-builder changes beyond the row
SELECT rendering.

Method: read every changed file end to end; ran every AGENTS.md gate that
applies to query/SQL changes (section 2 — all executed in this review, against
the running 3-node test cluster, `clickhouse-server 26.9.14.10`); verified the
ClickHouse-facing claims live on those nodes (`system.settings`, syntax probes);
spot-checked the pinned Loki concurrency claims at `bc1962860a16`; applied both
skills to the query/SQL surface; cross-checked the implementation against the
plan step by step.

**Verdict: merge-ready. Every gate passes; the implementation matches the
reviewed plan, and every recommendation of the plan review (R2-R5, F1/F2/F7) is
addressed in code or docs.** The findings below are one low hardening item and
info notes; none changes the design.

## 2. Verification results (executed in this review)

| Gate | Command | Result |
|---|---|---|
| build + vet | `go build ./... && go vet ./internal/chstore/` | PASS |
| format | `golangci-lint fmt --diff` | clean |
| lint | `golangci-lint run ./...` | 0 issues |
| unit, race, cluster | `go test -race -count=1 ./internal/chstore/...` | PASS (275 s) |
| unit, all packages, cluster | `go test -count=1 ./...` (= `make test-cluster`) | PASS, all 17 packages |
| oracle, pushdown on, single node | `go test -tags oracle ./test/oracle/...` | PASS (41.8 s) |
| oracle, off | `LOKIHOUSE_ORACLE_PUSHDOWN=off …` | PASS (47.5 s) |
| oracle, downstream | `LOKIHOUSE_ORACLE_PUSHDOWN=downstream …` | PASS (107.0 s) |
| oracle, cluster | `LOKIHOUSE_TEST_CH_ADDR=localhost:19001 LOKIHOUSE_TEST_CH_CLUSTER=lokihouse go test -tags oracle ./test/oracle/...` | PASS (53.6 s) |
| soak | `LOKIHOUSE_SOAK_DURATION=4m go test -tags oracle,soak -run TestSoak ./test/oracle/` | PASS (242.9 s; post-GC heap and goroutine count flat while `count_over_time`/`rate … | logfmt` run through the new pipeline under push/query/tail load) |
| coverage | `make cover` (race, all packages, cluster) | PASS — total **88.0 %** (minimum 85 %); `samplepipe.go` 100 % on every function, `rowiter.go:next` 88.9 %, `windows.go:sql` 100 %, `querier.go:SelectSamples` 78.0 % |
| vuln | `govulncheck ./internal/chstore/...` | no called vulnerabilities; `go.mod`/`go.sum` untouched by the branch |

Live ClickHouse checks on the cluster nodes (26.9.14.10):

| Check | Result |
|---|---|
| `do_not_merge_across_partitions_select_final` default | `0` — the explicit `SETTINGS` clause in `rowQuery.sql` is required, and it is there |
| `use_skip_indexes_if_final` / `use_skip_indexes_if_final_exact_mode` defaults | `1` / `1` — the implementation correctly relies on the defaults and sets neither (plan-review R4) |
| `… ORDER BY … SETTINGS … LIMIT` (what keysetReader would render under `final`) | syntax error at `LIMIT` — a `final` keyset query fails loudly, as the field comment claims; `FROM logs FINAL` parses |
| `OPTIMIZE` in non-test code | none — no `OPTIMIZE TABLE … FINAL` anywhere (`insert-optimize-avoid-final`) |

Not run here: `make bench` (Task 4 Step 4 — 60 m Loki comparison; see F4), the
full-length 10 m soak (a 4 m run passed), and the mkdocs build. The oracle's
`oracle-all` modes were run against ch1 (`localhost:19001`) instead of a
dedicated single server at `localhost:9000`, which is not published in this
environment; the modes are address-independent.

## 3. Workload summary (architecture-advisor)

- **Workload:** observability — a Loki-compatible log store on ClickHouse.
- **Latency target:** close the Tier B `unwrap quantile` gap (lokihouse 155 ms
  p50 vs Loki 69 ms p50 at 0.11.0); the branch claims < 100 ms, unconfirmed
  without `make bench` (F4).
- **Data shape:** unchanged — `logs` is `ReplacingMergeTree ORDER BY (tenant,
  stream_id, ts, entry_hash)`, `PARTITION BY toDate(ts)`, `entry_hash =
  cityHash64(line, mapSort(structured_metadata))`, text skip indexes; on a
  cluster `Distributed` shards by `cityHash64(tenant, stream_id)`, so duplicates
  are shard-local.
- **Primary query patterns:** unchanged; this branch changes only how Tier B
  metric reads fetch rows (`FINAL`) and how samples are extracted (worker
  pipeline).
- **Operational constraints:** Loki API/behaviour compatibility (oracle —
  passes, including the fixture's three deliberate exact duplicates), AGENTS.md
  gates (all pass), no new third-party dependency, no new Loki config key.

## 4. Key decisions reviewed

1. **`FINAL` on metric reads only** — compliant with
   `insert-optimize-avoid-final`, which explicitly endorses `FINAL` in SELECT
   for deduplicated results in `ReplacingMergeTree` and prohibits only
   `OPTIMIZE TABLE … FINAL` (none exists in the codebase), and with the
   advisor's `decision-late-arriving-upserts` (category: official —
   replacement semantics belong to `ReplacingMergeTree`; read-side `FINAL`, no
   heavy mutations). Duplicates come from retried pushes; the engine already
   merges on the same key in the background, so `FINAL` only makes the query
   result independent of merge state.
2. **Log/keyset reads stay without `FINAL`** — correct on two verified grounds:
   the plan's measurement (FINAL defeats top-N sorting on unmerged parts, 75 ms
   vs 6 ms for a 1000-row chunk) and a live syntax probe (`SETTINGS` before the
   keyset reader's appended `LIMIT ?` is a syntax error, so drift fails loudly).
   The invariant is pinned by `TestSelectLogs_NeverReadsFinal` (query_log
   assertion: keyset queries carry `LIMIT` and never `FINAL`; the metric query's
   SELECT carries `FINAL`) — plan-review R2/F1, addressed.
3. **`do_not_merge_across_partitions_select_final = 1`, explicit** — required
   (default `0`, verified live) and safe: duplicates share `ts`, hence the day
   partition. Identical to the Tier A precedent (`buckets.go:54-60`), now with
   the same comment. `schema-partition-low-cardinality` /
   `schema-partition-lifecycle` remain compliant (day partitions, retention
   drops whole partitions; the plan touches neither).
4. **Skip indexes under `FINAL`** — the implementation relies on the 26.9
   defaults (`use_skip_indexes_if_final = 1`, `use_skip_indexes_if_final_exact_mode
   = 1`, both verified live on 26.9.14.10 and previously on 26.9.25.15) and sets
   neither — plan-review R4, addressed. `TestSelectSamples_FinalKeepsTextIndex`
   pins that the text index still prunes under `FINAL` (50 000 rows, one needle,
   `read_rows < 25 000`, count = 1). See F1 for the one default the test does
   not pin.
5. **Parallel extraction, one `SampleExtractor` per worker** — the right
   granularity, re-verified at the pinned Loki commit: `LogfmtParser` holds one
   shared `*logfmt.Decoder` (`pkg/logql/log/parser.go:379-391`),
   `Pipeline.streamPipelines` is an unguarded map (`pipeline.go:223-272`), and
   every `syntax.SampleExpr.Extractor()` call builds fresh stages — so one
   extractor per worker is safe and one shared extractor would not be.
   `stats.Context.AddPostFilterLines` is atomic (`stats/context.go:542`); the
   worker batches it per batch. Streams are pinned to workers round-robin and
   each worker drains its channel FIFO, so every per-stream extractor sees the
   same line sequence as the sequential iterator, and the consumer yields in row
   order through the `ordered` channel. `TestSelectSamples_SameResultForAnyWorkerCount`
   proves result equality for `Workers` ∈ {1, 3, 64} across four queries
   (including a single-stream one) and two window sizes, plus equal
   `PostFilterLines`.
6. **Pipeline deadlock rules** — plan-review R3/F2, addressed in code:
   per-worker routing (a batch goes only to workers owning rows in it, not a
   broadcast), `done.Add(n)` with `n` = the batch's receivers *before* any send
   (documented at the `Add`), every worker calls `Done` for every batch it
   receives — also after cancellation, where it skips the work and records the
   context error so a short result is never silent (fix commit `7511cb8`, with
   the 300-iteration `TestSampleIter_CancelledContextNeverShortWithoutError`).
   The producer stashes the read error and closes all channels on exit, so
   `Err()` surfaces it after the rows already delivered.
7. **Allocation-free Go dedup** — the group-slice + `maps.Equal` rewrite is
   semantically equivalent to the old map-key dedup: the guard tests pin the
   A,B,A hash-collision group, the metadata-only difference and the group reset,
   and `lastRow` keeps its keyset-continuation role (set before the dedup
   check). The `row.md` type change (`labels.Labels` → `map[string]string`)
   moves `labels.FromMap` to the two consumers (`entryIter`, pipeline workers);
   all readers were found by grep and the compiler. Kept on the sample path
   under `FINAL` as belt-and-braces, exactly as the plan requires (plan-review
   F3: do not "simplify" it away — the log path still needs it).
8. **`Workers = max(1, min(GOMAXPROCS, streams))`, no config key** — the only
   construction site (`internal/server/server.go:216`) gets the `GOMAXPROCS`
   default; capping at the stream count neutralises the single-stream
   regression (plan Review Focus #1); consistent with AGENTS.md Loki
   compatibility (no invented Loki key). The per-query goroutine multiplier is
   documented in `docs/operations.md` under `## Limits` → `### Query`
   (plan-review R5/F7, addressed).

## 5. Rules checked (clickhouse-best-practices)

| Rule | Status | Note |
|---|---|---|
| `insert-optimize-avoid-final` | Compliant | `FINAL` only in metric-read SELECTs (plus the pre-existing Tier A aggregate); no `OPTIMIZE TABLE … FINAL` anywhere; cost bounded by `do_not_merge_across_partitions_select_final = 1` (verified necessary: default 0). |
| `query-index-skipping-indices` | Compliant | Skip indexes keep working under `FINAL` via the 26.9 defaults (verified live); pinned by `TestSelectSamples_FinalKeepsTextIndex`. See F1 for `use_skip_indexes_if_final_exact_mode`. |
| `schema-pk-filter-on-orderby` | Compliant | The `FINAL` rendering keeps `WHERE tenant = ? AND stream_id IN … AND ts range` (ORDER BY prefix + `ts` in the key) and the `ORDER BY`, which the Go dedup's adjacency assumption and the pipeline's row order rely on. Pre-existing shape, unchanged. |
| `schema-pk-cardinality-order`, `schema-pk-prioritize-filters` | Compliant (no change) | Schema untouched. |
| `schema-partition-low-cardinality`, `schema-partition-lifecycle` | Compliant | Day partitions bounded by retention; `FINAL` leans on `ts` being in the sorting key, so duplicates never span partitions. |
| `schema-types-*` (5 rules) | Not applicable | No column type changes. |
| `insert-mutation-avoid-update` | Compliant | `ReplacingMergeTree` is already the engine for replacement semantics; the rule's own correct example is `SELECT … FINAL`, exactly what Task 2 adds. |
| `insert-mutation-avoid-delete` | Compliant | Deletes unchanged (lightweight `DELETE` + `APPLY DELETED MASK` + `DROP PARTITION`); `TestQuerier_HidesPendingDeletes` now also runs with `Workers: 4` (plan Review Focus #4). |
| `insert-batch-size`, `insert-async-small-batches`, `insert-format-native` | Not applicable | Ingest path unchanged. |
| `query-join-*` (5 rules) | Not applicable | The branch's queries join nothing. |
| `query-mv-incremental` | Compliant (no change) | `stream_stats_hourly_mv` untouched; it feeds the window planning the metric reads use. |
| `schema-json-when-to-use` | Not applicable | `structured_metadata` stays a typed `Map` column. |

## 6. Findings

Severity: **medium** = real gap, fix before merge; **low** = harden or
document; **info** = note for the record.

- **F1 (low):** `TestSelectSamples_FinalKeepsTextIndex` writes its 50 000 rows
  in a single insert — one part. It therefore pins `use_skip_indexes_if_final
  = 1` (without it, `FINAL` would scan all 50 000 granules and the
  `read_rows < 25 000` assertion fails), but it does **not** pin
  `use_skip_indexes_if_final_exact_mode = 1`, which is the setting that keeps
  `FINAL` + skip index *correct* when newer parts overlap the ranges an older
  part's index returned. Both defaults were verified live (26.9.14.10 here,
  26.9.25.15 in the plan review), so this is reliance on a server default, not
  a defect — but a server upgrade flipping that default to 0 would turn
  multi-part metric queries silently approximate and no test would notice.
  Optional hardening: add a second, small insert overlapping the first (or one
  duplicate re-push) to the test so the exact-mode path is exercised.
- **F2 (info):** the `sampleBatchesInFlight` comment ("bounds the rows in
  memory to about `sampleBatchesInFlight × sampleBatchRows`") understates the
  worst case by less than 2×: a batch also occupies a slot in every worker
  channel it is routed to, so the true bound is roughly (4 in `ordered` + 1
  blocked in dispatch + 1 under construction + 1 held by the consumer) × 1024
  ≈ 7 000 rows, not 4 000. Still small and strictly bounded; the comment's
  "about" covers the order of magnitude. No action required.
- **F3 (info):** every recommendation of the plan review is addressed —
  R2/F1 (keyset-never-`FINAL` guard test: `TestSelectLogs_NeverReadsFinal`),
  R3/F2 (`done`-count rule and producer-error path written into the design and
  implemented, per-worker routing instead of broadcast, plus the `7511cb8`
  short-result fix), R4 (no explicit `use_skip_indexes_if_final*` settings;
  only `do_not_merge_across_partitions_select_final` is set), R5/F7
  (`docs/operations.md` paragraph under `### Query`, stating the per-query
  goroutine multiplier and the `GOMAXPROCS` knob).
- **F4 (info):** `make bench` was not run (60 m, compares against Loki release
  rows). The branch's performance claim — `unwrap quantile` p50 from 155 ms to
  below 100 ms — is therefore unconfirmed; the plan's Background numbers remain
  the author's measurements. `docs/benchmark.md` is correctly untouched (the
  plan forbids editing it outside release runs). All correctness gates pass.
- **F5 (info):** in the unit tests, which layer removed a duplicate is
  timing-dependent: the seeds are tiny parts that background merges may
  collapse before the query runs, so `TestSelectSamples_KeepsMetadataOnlyDifference`
  and the re-pushes in `TestSelectSamples_SameResultForAnyWorkerCount` prove
  correct results, while `TestSelectLogs_NeverReadsFinal` proves via
  `query_log` that the metric SELECT carries `FINAL`. That is the right split —
  the Go dedup is retained as belt-and-braces (plan Task 2 Step 3) — and the
  oracle fixture's three deliberate exact duplicates validate the end-to-end
  dedup against Loki on both topologies.
- **F6 (info):** the documented divergence (metric queries merge two different
  entries that share stream, nanosecond timestamp and `cityHash64`, vs Loki's
  exact comparison) is in `docs/divergences.md` as planned. The oracle covers
  exact duplicates but not hash collisions; the Go dedup's collision case is
  unit-tested (`TestRowReader_DedupesHashCollisionGroup`). Acceptable: a
  collision requires `cityHash64` equality on line + `mapSort(md)`, the same
  assumption the engine's merges already make.
- **F7 (info):** AGENTS.md process compliance verified: conventional commit
  prefixes, one logical change per commit, `CHANGELOG.md` (two `Unreleased` /
  `Changed` bullets) and `docs/divergences.md` in the same commits as the
  behaviour changes, `goleak.VerifyTestMain` added to `internal/chstore` (the
  first goroutine-owning package) with the bce-sdk-go ignore copied verbatim
  from `internal/api/main_test.go` including its comment, no new dependency
  (`go.mod`/`go.sum` untouched; `xxhash`, `goleak`, `testify` already present),
  deterministic tests (`require.Equal` across worker counts, no sleeps), and no
  `t.Parallel` added.

## 7. Recommendations

1. **Merge** — all gates pass; the findings are one optional test hardening
   (F1) and info notes.
2. **F1:** optionally extend `TestSelectSamples_FinalKeepsTextIndex` with a
   second overlapping insert so `use_skip_indexes_if_final_exact_mode`'s
   default is pinned too.
3. **Run `make bench`** (plan Task 4 Step 4) before announcing the performance
   win; the < 100 ms p50 target is otherwise unconfirmed (F4).
4. **Run the full 10 m `make soak`** before release; the 4 m run passed with
   flat heap and goroutine profiles.

## 8. Bottom line

The implementation is faithful to the reviewed plan and passes every gate that
applies to a query/SQL change: `make check` (fmt, vet, golangci-lint 0 issues,
govulncheck, race tests, coverage 88.0 % ≥ 85 %), `make test-cluster` (all 17
packages), the oracle in all three pushdown modes on a single node and on the
cluster — including the fixture's deliberate exact duplicates, which validate
the `FINAL` dedup against Loki end to end — and a 4-minute soak with metric
queries streaming through the new worker pipeline. The ClickHouse-facing
decisions (`FINAL` on metric reads only, explicit
`do_not_merge_across_partitions_select_final = 1`, skip indexes under `FINAL`
via verified server defaults, no `OPTIMIZE FINAL`) are compliant with
`insert-optimize-avoid-final`, `query-index-skipping-indices` and the
architecture advisor's `decision-late-arriving-upserts`, and the concurrency
design is correct at the pinned Loki commit (one extractor per worker, stream
pinning, ordered output, atomic stats, documented `done`-count rule, tested
close/cancel paths under goleak and `-race`). What remains open is measurement,
not correctness: `make bench` (F4) and the full-length soak.
