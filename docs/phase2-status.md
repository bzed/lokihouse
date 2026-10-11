# Phase 2 Status: SQL Push-Down

## Exit Criteria

| Criterion | Status | Notes |
|-----------|--------|-------|
| `make test` (single node) | ✅ PASS | All unit tests pass |
| `make test-cluster` (2×2 podman) | ✅ PASS | All tests pass on cluster |
| `make oracle-all` (single node) | ✅ PASS | All modes (off, downstream, on) pass |
| `make oracle-cluster` (2×2 podman) | ✅ PASS | All tests pass on cluster |
| Tier A property test | ✅ PASS | 3 seeds × 300 draws; Tier A ran on 100% of eligible leaves |

## Benchmark Results

**Test setup:** `BenchmarkLeaf` seeds 100,000 entries over 20 streams and 2h
through `chstore.Writer`, then runs each query end to end as a 2h range query
with a 1m step and a 5m range: mode `on` (Tier A leaf) against mode `off`
(Phase 1 engine, Tier B). Before timing, it checks that both modes return the
same data and that Tier A actually ran.

**Hardware:** AMD Ryzen 9 9900X 12-Core Processor, single-node ClickHouse 26.9

| Query | Tier A (ms/op) | Tier B (ms/op) | Speedup | Tier A (MB/op) | Tier B (MB/op) |
|-------|---------------:|---------------:|--------:|---------------:|---------------:|
| `count_over_time({app=~".+"}[5m])` | 8.6 | 47.4 | 5.5× | 1.7 | 66.3 |
| `rate({app=~".+"} \|= "error"[5m])` | 8.4 | 17.5 | 2.1× | 1.7 | 13.6 |
| `bytes_rate({app=~".+"}[5m])` | 8.0 | 46.7 | 5.8× | 1.7 | 66.3 |

**Key findings:**
- Tier A's cost is roughly flat (one `GROUP BY` and a series × steps result),
  while Tier B's cost grows with the number of matching lines. A selective
  line filter narrows the gap because Tier B's SQL pre-filter already drops
  most rows.
- Tier A allocates about 97% less memory for these queries.
- Root vector aggregations (`sum by (...) (rate(...))`) are not in the table:
  `Map` runs them on Tier B (see Known Issues).

An earlier version of this table came from a benchmark whose seed INSERTs
failed silently (it wrote the MATERIALIZED `entry_hash` column and a
non-existent `streams.labels_hash` column), so it measured empty queries.

## Decision Record

### Vector aggregations stay in Loki's evaluator over Tier A leaves

**Decision:** Push-down of vector aggregations (e.g., `sum by`, `avg by`) into SQL is NOT implemented in Phase 2.

**Rationale:**
- Leaf push-down (executing `count_over_time`, `rate`, etc. in ClickHouse) already eliminates the per-line transfer, which is the dominant cost
- Pushing `sum by` into SQL would only reduce the series × steps transfer, which is already small after leaf push-down
- The benchmark shows that the remaining cost is dominated by the leaf evaluation itself, not the vector aggregation

**When to revisit:** Only if the benchmark shows that series transfer dominates at realistic cardinality (e.g., >10,000 series × >1,000 steps).

### What stays Tier B

The following operations remain in Tier B (executed by Loki's evaluator over ClickHouse data):

- **Parsers:** `| logfmt`, `| json`, `| pattern`, `| regexp`
- **Unwrap:** `| unwrap <field>`
- **Line format:** `| line_format`
- **Case-insensitive regex:** `|~ "(?i)..."` (does not pass the RegexExact gate)
- **Grouping inside range aggregations:** `count_over_time(...) by (label)` (not eligible for Tier A)

These operations either cannot be expressed in SQL or would require complex query rewrites that do not provide sufficient benefit.

## Known Issues

### 1. Root vector aggregations and root arithmetic run on Tier B

`Map` returns `ok=false` for a root vector aggregation (`sum`, `avg`, `topk`, ...)
or a root arithmetic binop whose subtree contains a range aggregation: Loki's
downstream engine accumulates floats in a different order than its inline
evaluation (differences around 1e-16), and the oracle compares exactly. So the
most common dashboard shape, `sum by (x) (rate(...))`, does not use Tier A yet.
Lifting this needs either a tolerance in the oracle for these shapes or an
evaluation path that keeps Loki's accumulation order.

### Fixed during the Phase 2 review

- **"Tier A 1-count boundary difference"**: the difference came from Tier B, not
  Tier A. clickhouse-go formats a `time.Time` parameter at second precision, so
  Tier B's `ts >= ? AND ts < ?` truncated both query bounds to whole seconds.
  Tier B dropped rows in `[floor(end), end)` and log queries returned rows in
  `[floor(start), start)`. The keyset continuation key was truncated the same
  way, so a log read with more than one chunk of rows inside one second looped
  forever (forward) or skipped rows (backward). All of these are now bound as
  Unix nanoseconds through `fromUnixTimestamp64Nano`.
- Tier A looped forever on instant queries (step 0).
- Tier A emitted two series when different (stream, structured metadata)
  pairs produced the same label set; Loki merges them into one series.
- Above `MaxBuckets`, a Tier A leaf failed the query; it now falls back to
  Tier B (`ErrFallback`), as the plan specifies. `Exec` no longer writes the
  default into the shared `TierA` struct, which was a data race.
- `rate` and `bytes_rate` divide by `range.Seconds()` like Loki does, and the
  per-step window sum is a two-pointer slide instead of a rescan of every
  bucket in the window.

## Files

- `internal/pushdown/tiera.go` — Tier A implementation
- `internal/pushdown/bench_test.go` — Benchmarks
- `internal/pushdown/property_test.go` — Property test (Tier A == Tier B)
- `test/oracle/corpus/pushdown.logql` — Oracle corpus for push-down coverage
- `Makefile` — `oracle-all` target for testing all modes
