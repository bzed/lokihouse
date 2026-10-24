# Review: plan `2026-10-24-tierb-dedup-final-parallel` (commit `cb9a2e3`)

Reviewer: Mistral Vibe (second pair of eyes)
Date: 2026-10-24
Branch: `tierb-dedup-final-parallel` (plan commit `cb9a2e3` on top of 0.11.0)
Skills applied: `clickhouse-best-practices` (31 rules, cited per rule) and
`clickhouse-architecture-advisor` (workload classification, decision rules,
provenance labels).

The previous review in this file (2026-10-23, branch `feat/ch-query-review`)
is preserved in git history at `76fab91`.

## 1. Scope and method

The commit under review adds one file,
`docs/superpowers/plans/2026-10-24-tierb-dedup-final-parallel.md` (278 lines): a
three-task implementation plan to cut the Tier B (Loki-engine) metric query path
—

- **Task 1:** rewrite the Go dedup in `rowReader.next` without per-row
  allocations (`internal/chstore/rowiter.go`),
- **Task 2:** remove duplicates in ClickHouse with `FINAL` on metric reads
  (`SelectSamples`), never on log reads,
- **Task 3:** extract samples on `GOMAXPROCS` workers behind an ordered
  producer/worker pipeline (`internal/chstore/samplepipe.go`),
- **Task 4:** verification (gates + benchmark, no code).

Method: read the plan end to end; read the code it touches (`schema.sql`,
`rowiter.go`, `windows.go`, `querier.go`, `samples.go`, `keyset.go`,
`buckets.go`, `deleterows.go`, `topology.go`, `export_test.go`,
`docs/benchmark.md`, `docs/divergences.md`, `CHANGELOG.md`, `go.mod`); read the
pinned Loki source (`.cache/loki-src` at `bc1962860a16`) for every concurrency
claim; checked every ClickHouse setting claim against a live
`clickhouse-server 26.9.25.15` (`system.settings`); and validated the plan's
central ClickHouse claims empirically with a scratch table (dropped afterwards,
section 3). Bench/profile numbers from the plan's Background section are the
author's measurements and are marked as such (F4).

**Verdict: the plan is sound and implementable as written. No blocking
findings.** Every ClickHouse-facing decision checks out against the schema, the
live 26.9 server, the pinned Loki source and both skills. The findings below
are low/info hardening and documentation.

## 2. Workload summary (architecture-advisor)

- **Workload:** observability — a Loki-compatible log store on ClickHouse.
- **Latency target:** close the `unwrap quantile` gap: lokihouse 155 ms p50 vs
  Loki 69 ms p50 at 0.11.0 (`docs/benchmark.md`); sub-100 ms target.
- **Data shape:** append-only log lines; `logs` is `ReplacingMergeTree ORDER BY
  (tenant, stream_id, ts, entry_hash)`, `PARTITION BY toDate(ts)`, with
  `entry_hash = cityHash64(line, mapSort(structured_metadata))`; text skip
  indexes on `line`, `sm_keys`, `sm_vals`, `line_ascii_escape`; an incremental
  MV (`stream_stats_hourly`) feeding window planning and Tier A.
- **Primary query patterns:** tenant + stream-set + time-range scans with line
  and metadata filters; Tier A pushes plain aggregates down to SQL; Tier B runs
  parsers/`unwrap` in Loki's engine in Go (this plan's target).
- **Operational constraints:** Loki API/behaviour compatibility (oracle),
  AGENTS.md gates (`make check`, `test-cluster`, `oracle-*`, `soak`, coverage),
  no new third-party dependency, no new Loki config key.

## 3. Empirical ClickHouse validation (26.9.25.15, scratch table)

Reproduced the plan's storage claims on the live server with a scratch copy of
the `logs` shape (day partition, same ORDER BY, text index), 100 000 distinct
rows pushed 3× as three separate parts (retried pushes), plus rows differing
only in structured metadata and one exact duplicate:

| Check | Result |
|---|---|
| raw rows / `FINAL` | 300 003 → 100 002 (dedup across unmerged parts works) |
| `FINAL` + `do_not_merge_across_partitions_select_final = 1` | 100 002 — identical (duplicates share `ts`, hence the day partition) |
| rows differing only in `structured_metadata` under `FINAL` | both kept (2) — `entry_hash` covers `mapSort(md)` |
| exact duplicate of one of them, separate part | merged (2 not 3) |
| text index under `FINAL` (needle among 300 k rows) | `read_rows = 2` (granule skipping works under `FINAL`), result correct: plain 2, `FINAL` 1, `FINAL`+no-cross-partition 1 |
| `read_rows` under `FINAL` | counts pre-merge rows (2 raw, 1 result) → `totalLinesProcessed` semantics unchanged by `FINAL` |

Settings verified live: `use_skip_indexes_if_final = 1` and
`use_skip_indexes_if_final_exact_mode = 1` are 26.9 defaults (the latter
"scans the additional parts that overlap the ranges returned by the skip
index" — the correctness mechanism the plan relies on);
`do_not_merge_across_partitions_select_final = 0` by default, so the plan's
explicit `SETTINGS` clause is required and matches Tier A. The scratch database
was dropped afterwards.

## 4. Claim verification against the codebase and pinned Loki

| # | Plan claim | Verified against | Result |
|---|---|---|---|
| 1 | `logs`: `ReplacingMergeTree ORDER BY (tenant, stream_id, ts, entry_hash)`, `entry_hash = cityHash64(line, mapSort(structured_metadata))`, `PARTITION BY toDate(ts)` | `internal/chstore/schema.sql:3-17` | confirmed |
| 2 | `Distributed` shards by `cityHash64(tenant, stream_id)` → duplicates shard-local | `schema.sql:66-67` | confirmed |
| 3 | Tier A already relies on `FINAL` + `do_not_merge_across_partitions_select_final = 1` with the same safety argument | `internal/chstore/buckets.go:54` + comment | confirmed |
| 4 | `rowReader.next` allocates a fresh `groupSeen` map per group, `labels.FromMap`, `line+"\xff"+md.String()` key | `rowiter.go:32-53` | confirmed |
| 5 | Rows arrive ordered `ts, stream_id, entry_hash`; keyset continuation is a strict `>` on the full key | `windows.go:88-101`, `keyset.go:30-56` | confirmed |
| 6 | `SelectSamples` = one unchunked cursor per window; `SelectLogs` = keyset chunks (continuation inserted before `ORDER BY`, `LIMIT ?` appended) | `querier.go:196-201`, `keyset.go:41-56` | confirmed — `keysetReader` never sees `final` |
| 7 | Bench: `unwrap quantile` 155 ms vs Loki 69 ms at 0.11.0 | `docs/benchmark.md` result table | confirmed |
| 8 | `LogfmtParser` holds one shared `*logfmt.Decoder` | Loki `pkg/logql/log/parser.go:379-391`, used at `403-405` | confirmed — not concurrency-safe |
| 9 | `ForStream` caches in unguarded maps | Loki `pipeline.go:266-274` (`streamPipelines`), `metrics_extraction.go:91-104`, `356-368` (`streamExtractors`) | confirmed |
| 10 | Every `syntax.SampleExpr.Extractor()` call builds fresh stages → one extractor per worker is safe | Loki `pkg/logql/syntax/extractor.go:12-83` (`stagesFor(r.Left)` per call) | confirmed |
| 11 | `stats.Context.AddPostFilterLines` is atomic | Loki `pkg/logqlmodel/stats/context.go:542` | confirmed |
| 12 | `utildeletion.SetupExtractor(p, …)` is pure per call → safe once per worker | Loki `pkg/util/deletion/deletion.go:23-34` | confirmed |
| 13 | 26.9 defaults `use_skip_indexes_if_final = 1`, `use_skip_indexes_if_final_exact_mode = 1` | live `system.settings` | confirmed |
| 14 | Referenced tests/helpers exist: `TestRowReader_DedupesIdenticalRows`, `engine(t, …)`, `chtest.LogsRowsRead`, `TestQuerier_HidesPendingDeletes`, `TestSelectSamples_SameResultForAnyWindowSize`, `OpenedSampleWindows`, goleak in `go.mod`, bce ignore in `internal/api/main_test.go` | `rowiter_internal_test.go:15`, `querier_test.go:35`, `chtest/chtest.go:81`, `querier_test.go:310`, `windows_test.go:39`, `export_test.go`, `go.mod:53` | confirmed |
| 15 | `row.md` readers are only `rowiter.go`, `keyset.go` (copies), `entryIter`, `sampleIter` | grep across `internal/` | confirmed — Task 1's file list is complete |
| 16 | `internal/chstore` starts no goroutines today → `goleak.VerifyTestMain` is required once the pipeline lands (AGENTS.md) | grep `go func` in `internal/chstore` | confirmed — plan adds `main_test.go` |
| 17 | `chstore.Querier` is constructed only at `internal/server/server.go:216` → `Workers` zero value (= `GOMAXPROCS`) applies everywhere; no config key needed | `server.go:216` | confirmed |
| 18 | No new third-party dependency (`xxhash`, `goleak`, `testify` already used) | `go.mod` | confirmed |
| 19 | `CHANGELOG.md` `## [Unreleased]` empty; `docs/divergences.md` has a Query section | both files | confirmed — the plan's doc steps land cleanly |
| 20 | Go 1.27, `GOMAXPROCS` cgroup-aware since Go 1.25 | `go.mod` (`go 1.27.1`) | confirmed |

## 5. Rules checked (clickhouse-best-practices)

| Rule | Status | Note |
|---|---|---|
| `insert-optimize-avoid-final` | Compliant | The rule prohibits `OPTIMIZE TABLE … FINAL` and explicitly states `FINAL` in SELECT "may be necessary for deduplicated results in ReplacingMergeTree and is generally fine to use". The plan uses `FINAL` only on metric reads, never `OPTIMIZE FINAL`, keeps log/keyset reads without it, and bounds the cost with `do_not_merge_across_partitions_select_final = 1` (verified necessary: default 0). |
| `query-index-skipping-indices` | Compliant | Skip indexes (`line_text`, `sm_keys`, `sm_vals`, `line_ascii_escape_idx`) keep working under `FINAL`: 26.9 defaults verified live, mechanism verified empirically (section 3), and Task 2 pins it with a rows-read assertion (`TestSelectSamples_FinalKeepsTextIndex`) plus a correctness assertion (count = 1). |
| `schema-pk-filter-on-orderby` | Compliant | Row queries filter the `tenant` prefix, the stream set and a `ts` range; `ORDER BY (tenant, stream_id, ts, entry_hash)` aligns; the `FINAL` rendering keeps the `ORDER BY` for deterministic order. Pre-existing, unchanged by the plan. |
| `schema-pk-cardinality-order` | Compliant (no change) | `tenant` (LowCardinality) first, then `stream_id`, `ts`, `entry_hash` — matches the query filters. Schema untouched by the plan. |
| `schema-pk-prioritize-filters` | Compliant (no change) | Same as above. |
| `schema-partition-low-cardinality` | Compliant | Day partitions, count bounded by retention (the sweeper drops whole UTC days); the plan's `FINAL` usage leans on the day partition and is safe because `ts` is in the sorting key. |
| `schema-partition-lifecycle` | Compliant | Partitioning is used for lifecycle (retention drops whole day partitions), not as a query crutch. |
| `schema-types-*` (5 rules) | Not applicable | The plan changes no column types. |
| `insert-mutation-avoid-update` | Compliant | `ReplacingMergeTree` is already the engine for replacement semantics (retried pushes, `delete_requests` version column); the rule's own "correct" example is `SELECT … FINAL`, exactly what Task 2 adds. |
| `insert-mutation-avoid-delete` | Compliant | Deletes already use lightweight `DELETE` + `APPLY DELETED MASK` + `DROP PARTITION` for retention (`deleterows.go`, `retention.go`); the plan does not change deletes. Review Focus #4 covers the delete-filter/worker interaction. |
| `insert-batch-size`, `insert-async-small-batches`, `insert-format-native` | Not applicable | Ingest path unchanged. |
| `query-join-*` (5 rules) | Not applicable | The plan's queries join nothing (the stream-ID set rides the `lh_stream_ids` external table). |
| `query-mv-incremental` | Compliant | `stream_stats_hourly_mv` is an incremental MV into an `AggregatingMergeTree` — the documented pattern; it also feeds the window planning this plan builds on. |
| `schema-json-when-to-use` | Not applicable | `structured_metadata` is a typed `Map` column, pre-existing. |

## 6. Architecture assessment (clickhouse-architecture-advisor)

**Key decisions reviewed:**

1. **Dedup at read with `FINAL`, metric queries only.** Correct per
   `decision-late-arriving-upserts` (replacement semantics belong to
   `ReplacingMergeTree`, category: official — the duplicates come from retried
   pushes; heavy write-side mutations are correctly avoided) and per
   `insert-optimize-avoid-final` (SELECT `FINAL` endorsed; `OPTIMIZE FINAL`
   avoided). Excluding log reads is the critical half of the decision: keyset
   pagination (`LIMIT ?` + strict `>` continuation) is incompatible with
   per-query merging, and the plan's own measurements show `FINAL` defeats
   top-N on unmerged parts (75 vs 6 ms). Keeping the Go dedup on the sample
   path under `FINAL` (one key comparison per row, no duplicates found) is a
   cheap belt-and-braces that also covers the window before background merges
   catch up.
2. **Allocation-free Go dedup for the log path.** Pure Go refactor; semantics
   preserved (the planned guard tests pin the A,B,A hash-collision case, the
   metadata-only difference and the group reset; I checked the new
   group-by-`(ts, stream_id, entry_hash)` slicing is equivalent to today's
   map-based exact dedup, including across chunk boundaries, where the strict
   `>` continuation already skips the rest of the last key group — the same
   rule the engine itself applies).
3. **Per-query parallel extraction bounded by `min(GOMAXPROCS, streams)`, ≥ 1.**
   Correctly neutralises the single-stream regression risk (Review Focus #1),
   keeps per-stream extractor state single-goroutine (streams pinned to one
   worker, batches processed in channel order → identical per-stream line
   sequence and identical output order), and bounds memory to
   `sampleBatchesInFlight × sampleBatchRows` rows. Verified against the pinned
   Loki source that one `SampleExtractor` per worker is the right granularity
   (shared `logfmt.Decoder`, unguarded `ForStream` caches).
4. **No new config key; `GOMAXPROCS` is the knob.** Consistent with AGENTS.md
   Loki compatibility (no invented Loki key); the only construction site
   (`server.go:216`) gets the `GOMAXPROCS` default for free.

**Recommendations:**

### R1. Implement the plan as written

**Category:** derived · **Confidence:** high · **Source:** sections 3-5 above;
`insert-optimize-avoid-final`, `decision-late-arriving-upserts`.
**Validation:** already done — schema, settings, Loki concurrency and the
FINAL mechanics all verified (sections 3-4); Task 4 re-measures end to end
with `make bench` against the 0.11.0 rows.

### R2. Add a guard test for the "keyset never sees `final`" invariant

**What:** one test asserting `keysetReader`'s rendered SQL never contains
`FINAL`/`SETTINGS` (or that `rowQuery{final: true}` is rejected on the keyset
path).
**Why:** the invariant is currently a field comment (F1). Today a violation
fails loudly (`LIMIT ?` appended after `SETTINGS` is a syntax error), but a
cheap test turns silent future drift into a red build.
**Category:** derived · **Confidence:** high · **Source:** `keyset.go:41-56`;
ClickHouse `SETTINGS`-last syntax (verified live).
**Validation:** `go test -run TestKeyset ./internal/chstore/`.

### R3. State the pipeline's `done`-count rule and prefer per-worker routing

**What:** in `samplepipe.go`, the `batch.done` WaitGroup count must equal the
number of workers that receive the batch; the sketch's "send batch to every
worker's channel" (broadcast) makes that N per batch and wakes every worker per
batch — routing each batch only to workers that own rows in it is equally
simple and strictly better. Also state explicitly how a producer read error
reaches `Err()` (stash it, close the ordered channel).
**Why:** a count/receiver mismatch hangs the consumer in `done.Wait()` — the
one deadlock the sketch leaves to the implementer; Review Focus #2/#3 tests
would catch it, but the rule should be in the design.
**Category:** field (heuristic) · **Confidence:** medium · **Source:** plan
Task 3 sketch; AGENTS.md "every goroutine has an owner that stops it".
**Validation:** `TestSelectSamples_CloseMidStream`,
`TestSelectSamples_CancelledContext`, `go test -race -count=3`, goleak.

### R4. Keep the `use_skip_indexes_if_final` pin, do not set the settings explicitly

**What:** rely on the 26.9 defaults (`use_skip_indexes_if_final = 1`,
`use_skip_indexes_if_final_exact_mode = 1`) rather than adding `SETTINGS` for
them, and keep `TestSelectSamples_FinalKeepsTextIndex`.
**Why:** the defaults are the correctness-preserving configuration (verified
live, including the exact-mode description); pinning via test detects a
server-upgrade default change, an explicit `SETTINGS` would hide it.
`do_not_merge_across_partitions_select_final = 1` must stay explicit (default
0, and required to bound `FINAL` cost).
**Category:** official · **Confidence:** high · **Source:**
`system.settings` on 26.9.25.15;
https://clickhouse.com/docs/operations/settings/settings
**Validation:** the Task 2 text-index test (rows read < 25 000, count = 1).

### R5. Document the per-query goroutine multiplier in `docs/operations.md`

**What:** the planned paragraph should state that a metric query spawns up to
`min(GOMAXPROCS, resolved streams)` extraction goroutines *per query*, so
concurrent queries multiply; `GOMAXPROCS` bounds the total.
**Why:** operators sizing a box need the multiplier; the plan documents the
knob but not the fan-out shape.
**Category:** derived · **Confidence:** high · **Source:** plan Task 3
interfaces; `make soak` goroutine profiles (Task 4 Step 3) as validation.

### R6. No change needed for query stats semantics

**What:** nothing — `read_rows` (and therefore `totalLinesProcessed`) counts
pre-merge rows under `FINAL` (verified live: 2 read, 1 result), so the existing
`docs/divergences.md` stats bullet stays accurate.
**Category:** official (observed server behaviour) · **Confidence:** high
· **Source:** `system.query_log` on 26.9.25.15; `internal/chstore/withstats.go`.

## 7. Findings

Severity: **medium** = real gap, fix before merge; **low** = harden or
document; **info** = note for the record.

- **F1 (low):** the "`keysetReader` never sees `final: true`" invariant is a
  comment, not a guard. A violation fails loudly today (`LIMIT ?` after
  `SETTINGS` is a syntax error — verified against ClickHouse's `SETTINGS`-last
  syntax), but add the R2 guard test so drift is a red build, not a runtime
  query failure.
- **F2 (low):** the Task 3 sketch leaves two rules to the implementer: the
  `batch.done` WaitGroup count must match the batch's receivers exactly
  (broadcast makes it N; a mismatch hangs `Next` in `done.Wait()`), and the
  producer's read error must be stashed and surfaced via `Err()` after the
  ordered channel closes. See R3.
- **F3 (info):** Tasks 1 and 2 are complementary, not redundant. Task 1 saves
  allocations on duplicates that still reach Go (log reads — no `FINAL` — and
  sample reads before merges catch up); Task 2 removes duplicates at the source
  for metric reads. After Task 2 lands, the sample path's Go dedup finds ~no
  duplicates — do not "simplify" Task 1 away on that observation; the log
  path still needs it.
- **F4 (info):** the Background numbers (profile breakdown, the `FINAL` cost
  table, 176→153 / 326→208 ms end-to-end) are the author's 2026-10-24
  measurements; I could not re-derive them here (no bench data). They are
  internally consistent with `docs/benchmark.md` (155 ms p50) and with the
  `FINAL` mechanics I verified empirically (section 3). Task 4 re-measures
  with `make bench` and explicit targets — the right way to confirm them.
- **F5 (info):** the planned divergence bullet is honest and complete: metric
  queries will merge two different entries that share stream, nanosecond
  timestamp and `cityHash64` (vs Loki's exact line+metadata comparison), and
  it already notes that background merges apply the same hash rule to stored
  data — i.e. `FINAL` makes the query path consistent with what merges will
  eventually do, rather than introducing a new kind of divergence. Collision
  probability is negligible and the engine itself merges on the same key.
- **F6 (info):** no config key for `Querier.Workers` — verified: the only
  construction site is `internal/server/server.go:216`, so the zero value
  (`GOMAXPROCS` at query time) applies everywhere and the plan's "Loki has
  none" reasoning holds. Per-query goroutine fan-out (queries ×
  `min(GOMAXPROCS, streams)`) is bounded in memory but not in goroutine count;
  R5 documents it, `make soak` (Task 4 Step 3) checks the profiles.
- **F7 (low):** `docs/operations.md` has no "query/performance section" — its
  sections are Limits (Ingestion/Query), Health checks, Deletes and disk
  space, Retention, Metrics, Profiling, Multi-tenant queries, When ClickHouse
  is unavailable. Anchor the planned `GOMAXPROCS` paragraph under `### Query`
  (Limits) or `## Profiling`.
- **F8 (info):** the Task 1 group comparison is O(k²) within a
  `(ts, stream_id, entry_hash)` group. Groups are duplicates (k ≈ 1-3), and a
  collision storm would require `cityHash64` collisions — the same assumption
  the storage engine already makes when merging. No action.
- **F9 (info):** AGENTS.md process compliance verified: gates enumerated
  correctly (`make check`, `test-cluster`, `oracle-all`/`oracle-cluster`,
  `soak`), goleak `TestMain` planned for the first goroutine-owning package,
  no new dependency, divergences + CHANGELOG in the same commits as the
  behaviour changes, conventional commit prefixes, deterministic tests
  (`require.Equal` across worker counts, no sleeps).

## 8. Recommendations (prioritized)

1. **Implement the plan as written** — no blocking findings; the
   ClickHouse-facing decisions are verified against the live 26.9 server, the
   schema and the pinned Loki source.
2. **F1/R2:** add the keyset-never-`FINAL` guard test (Task 2 or Task 3).
3. **F2/R3:** write the `done`-count rule and the producer-error path into the
   Task 3 sketch before implementing; prefer per-worker routing over broadcast.
4. **R4:** keep the skip-index pin as a test; do not add explicit
   `use_skip_indexes_if_final*` settings; keep
   `do_not_merge_across_partitions_select_final = 1` explicit.
5. **F7 + R5:** anchor the `operations.md` paragraph correctly and document the
   per-query goroutine multiplier.
6. **F4:** let Task 4's `make bench` run confirm the Background numbers against
   the 0.11.0 rows (guard list verified present in `docs/benchmark.md`).

## 9. Bottom line

The plan's three ClickHouse-facing decisions — `FINAL` on metric reads only,
`do_not_merge_across_partitions_select_final = 1`, and skip indexes under
`FINAL` — are all correct on 26.9, empirically validated, and compliant with
`insert-optimize-avoid-final` (which explicitly endorses SELECT `FINAL` for
`ReplacingMergeTree` and prohibits only `OPTIMIZE TABLE FINAL`), with
`query-index-skipping-indices` and with the architecture advisor's
`decision-late-arriving-upserts` (ReplacingMergeTree + read-side `FINAL`, no
heavy mutations). The concurrency design is sound at the pinned Loki commit
(one extractor per worker; stream pinning; ordered output; atomic stats), the
single-stream and cancellation/close paths are covered by named tests plus
goleak, and the AGENTS.md gate list is complete. The findings are one guard
test, two sketch-level rules for the pipeline, and documentation placement —
nothing that changes the design.

Not verified here: the author's profiling numbers (F4 — Task 4 re-measures),
cluster behaviour of `FINAL` on the `Distributed` table (Tier A precedent
`buckets.go:54` plus the `make test-cluster` gate cover it; duplicates are
shard-local by `cityHash64(tenant, stream_id)`), and the mkdocs build.
