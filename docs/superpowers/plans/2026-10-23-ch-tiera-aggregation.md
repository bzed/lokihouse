# Tier A Aggregation Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier A reads `line` only when bytes are asked for, and for `sum [by (…)] (<eligible range aggregation>)` groups by only the structured-metadata keys the grouping can see, instead of the whole metadata map.

**Architecture:** `BucketCounts` takes a `BucketOpts` (bytes on/off, metadata keys to keep). `pushdown.Map` makes `sum`/`sum by` over a range aggregation the downstream leaf, keeping the outer `sum` above it, the way Loki's shard mapper does; Tier A executes such a leaf as the inner range aggregation with narrowed metadata and returns un-summed series, which the outer `sum` re-aggregates exactly.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9, Loki `pkg/logql` (DownstreamEngine, `syntax`).

**Spec:** `docs/superpowers/specs/2026-10-23-clickhouse-query-review.md` (item 6)

**Order:** after `2026-10-23-ch-query-path-fixes.md` (`BucketCounts` already uses `streamIDsIn`). Independent of the windowed-reads plan.

## Global Constraints

- Follow `AGENTS.md` (errors, ctx, changelog, divergences, no package-level mutable state).
- Tier A results equal Tier B results (`runTierA` compares them) within the oracle's 1e-9 relative tolerance already documented on `Map`.
- Narrowing applies only to `sum` with `by (…)` or without grouping. `sum without (…)`, `count`, `avg`, `min`, `max`, `topk` and the rest keep today's leaves (full metadata).
- `BucketCounts` runs with the setting `do_not_merge_across_partitions_select_final = 1` (safe: duplicates share `ts`, hence the day partition).
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` pass.

## Review Focus

- `sum by (level)` where `level` is both a stream label and a structured-metadata key (clash spills to `level_extracted`) must equal Tier B. Test in Task 2.
- `sum by (level_extracted)` must fetch metadata key `level` too. Test in Task 2.
- `count by (level) (count_over_time(…))` must not be narrowed: merging series would change the count. Test in Task 2.
- A sum-by leaf that falls back to Tier B (pending delete, too many buckets) still gives the right answer: the outer `sum` re-aggregates an already summed vector. Test in Task 2.
- `bytes_over_time` must still report bytes after the bytes switch. Test in Task 1.

---

### Task 1: `BucketOpts`: bytes on demand, metadata subset, no cross-partition FINAL merge

**Files:**
- Modify: `internal/chstore/buckets.go`, `internal/pushdown/tiera.go` (call site)
- Test: `internal/chstore/stats_integration_test.go` (`TestBucketCounts`), `internal/pushdown/tiera_test.go`

**Interfaces:**
- Produces:
  ```go
  type BucketOpts struct {
      Bytes  bool     // sum(length(line)); false reports Bytes = 0 and does not read line
      AllMD  bool     // group by the whole structured_metadata map
      MDKeys []string // when !AllMD: group by only these metadata keys (empty = none)
  }
  func BucketCounts(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, origin, through, g int64, filter Fragment, opt BucketOpts) ([]BucketRow, error)
  ```
  The metadata expression is `structured_metadata` when `AllMD`, else `mapFilter((k, v) -> has(?, k), structured_metadata)` with `MDKeys` bound as an array. `BucketRow` is unchanged (`MD` holds the selected subset).

- [ ] **Step 1: Write the failing tests**

Extend `TestBucketCounts` with entries carrying metadata `{level, trace_id}`:
- `BucketOpts{Bytes: true, AllMD: true}` → today's rows (bytes > 0, both keys).
- `BucketOpts{AllMD: true}` → same counts, every `Bytes == 0`.
- `BucketOpts{MDKeys: []string{"level"}}` → `MD` maps hold only `level`; counts per (stream, level, bucket) sum to the totals.
- `BucketOpts{MDKeys: []string{}}` → `MD` empty, one row per (stream, bucket).

`TestTierA_Filters` gains `bytes_over_time({app="a"}[1m])` and `bytes_rate({app="a"}[1m])` cases (compare to Tier B).

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run TestBucketCounts -v`
Expected: FAIL to compile (`BucketOpts` undefined).

- [ ] **Step 3: Implement `BucketOpts` and the setting; `TierA.Exec` passes `Bytes` = op is `bytes_over_time` or `bytes_rate`, `AllMD: true`**

- [ ] **Step 4: Run them to verify they pass**

Run: `go test ./internal/chstore ./internal/pushdown -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore internal/pushdown
git commit -m "perf: Tier A reads log lines only for byte aggregations"
```

---

### Task 2: `sum [by]` leaves with narrowed metadata

**Files:**
- Modify: `internal/pushdown/mapper.go` (`Map`, `mapNode`), `internal/pushdown/engine.go` (`Leaf`, `execOne`, `anyEligible`), `internal/pushdown/tiera.go` (`Eligible`, `Exec`), `test/oracle/oracle_test.go` (leaf counting compiles against the new leaf type)
- Test: `internal/pushdown/mapper_test.go`, `internal/pushdown/tiera_test.go`, `test/oracle/corpus/pushdown.logql`

**Interfaces:**
- Consumes: `BucketOpts` (Task 1).
- Produces:
  - `func Map(expr syntax.SampleExpr) (mapped syntax.SampleExpr, leaves []syntax.SampleExpr, ok bool)` — a leaf is either `*syntax.RangeAggregationExpr` or a `*syntax.VectorAggregationExpr` with `Operation == syntax.OpTypeSum`, `Grouping == nil || !Grouping.Without`, whose `Left` is a `*syntax.RangeAggregationExpr`. Such a node maps to a copy of itself whose `Left` is `logql.DownstreamSampleExpr{SampleExpr: <original node>}`.
  - `type Leaf interface { Eligible(e syntax.SampleExpr) bool; Exec(ctx context.Context, e syntax.SampleExpr, p logql.Params) (logqlmodel.Result, error) }`
  - `func mdKeysFor(g *syntax.Grouping) []string` in `tiera.go` — `[]string{}` for no grouping or `by ()`; else every grouping name plus, for names ending in `_extracted`, the name without the suffix.
- `TierA.Exec` on a sum leaf runs the inner range aggregation with `BucketOpts{MDKeys: mdKeysFor(grouping)}` and returns its matrix **un-summed**; comment on `Exec` that the outer `sum` from `Map` completes the aggregation, and that a Tier B fallback returns the summed vector, which the outer `sum` leaves unchanged.

- [ ] **Step 1: Write the failing tests**

`mapper_test.go`, `TestMap_SumLeaves`:
- `sum by (level) (count_over_time({a="b"}[1m]))` → mapped is a `*syntax.VectorAggregationExpr` with the same operation and grouping whose `Left` is a `logql.DownstreamSampleExpr` wrapping the original node; one leaf, that original `*syntax.VectorAggregationExpr`; the input expression's `String()` is unchanged (no mutation).
- `sum without (x) (…)`, `count by (level) (…)`, `sum by (l) (sum by (l, m) (…))` → leaf is the range aggregation, as today.

`tiera_test.go`, `TestTierA_SumByNarrowsMetadata`: streams `{app="a"}` and `{app="a", level="s"}`; entries with metadata `level ∈ {info, warn}` and a unique `trace_id`. For each query below `runTierA` results are equal, and `TierALeaves` grows by the number of leaves:
`sum by (level) (count_over_time({app="a"}[1m]))`, `sum(rate({app="a"}[1m]))`, `sum by (level_extracted) (count_over_time({app="a"}[1m]))`, `sum by (app, level) (bytes_over_time({app="a"} |= "x" [1m]))`, `sum without (trace_id) (count_over_time({app="a"}[1m]))`, `count by (level) (count_over_time({app="a"}[1m]))`.
Also call `TierA.Exec` directly on the first query's leaf and assert no returned series has a `trace_id` label.

`TestTierA_SumLeafFallback`: the first query with a pending delete (reuse `fakeDeletes`) and with `MaxBuckets: 1` equals Tier B.

`test/oracle/corpus/pushdown.logql`: add `range:sum(count_over_time({app="web"}[5m]))` and `instant:sum by (pod) (rate({app=~".+"}[5m]))`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/pushdown -run 'TestMap_SumLeaves|TestTierA_Sum' -v`
Expected: FAIL (leaf is the range aggregation; `trace_id` present).

- [ ] **Step 3: Implement the mapper case, the `Leaf` signature change, `mdKeysFor`, and the sum-leaf branch in `Eligible`/`Exec`**

- [ ] **Step 4: Run tests and the full gate**

Run: `go test ./internal/pushdown -v && make check && make test-cluster && make oracle-all && make oracle-cluster`
Expected: all pass; oracle Tier A leaf growth still > 0.

- [ ] **Step 5: Changelog and commit**

`### Changed`: "`sum` and `sum by` over `count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` aggregate in ClickHouse by only the needed structured-metadata keys (large gains for logs with per-line metadata such as `trace_id`)."

```bash
git add internal/pushdown test/oracle CHANGELOG.md
git commit -m "perf: push sum-by grouping into Tier A metadata grouping"
```
