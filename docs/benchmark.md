# Benchmark

`make bench` (`test/oracle/bench_test.go`, build tags `oracle bench`) measures
lokihouse query latency against Loki on identical data and identical queries.
It is a correctness-gated benchmark: every query's response from both stores is
compared with the oracle's `Compare`, so a fast-but-wrong answer fails the run.

## Method

- Both stores ingest the same deterministic, seeded data set: 200 000 lines
  over 50 streams (10 apps × dev/prod), spread over the final hour, pushed to
  both in 5 000-line batches. Tweak with `LOKIHOUSE_BENCH_LINES`,
  `LOKIHOUSE_BENCH_STREAMS` and `LOKIHOUSE_BENCH_ROUNDS`.
- After ingesting, the harness waits until Loki counts every line (its readiness
  gate), then runs nine representative LogQL queries — plain and regex log
  filters, `count_over_time`, `rate`, `topk`, `unwrap` + `quantile_over_time`,
  `bytes_over_time`, and the `labels` and `series` metadata endpoints.
- Each query is timed over five rounds (first round included; the data is
  already warm). `p50`/`p95` are reported per store; responses are compared for
  equality before they are counted.
- Results are written to `$LOKIHOUSE_BENCH_OUT` (default
  `.cache/bench/results.md`) as a Markdown table.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 9900X (12 cores / 24 threads) |
| RAM | 60 GiB |
| Host | single node; both binaries on the same host |
| lokihouse | ClickHouse 26.9, single node in Podman |
| Loki | single binary, filesystem storage (no S3, no object store) |

## Caveats

- Loki serves recent data from the ingester's in-memory chunks, while lokihouse
  reads from ClickHouse. This benchmark measures a single node, not scale-out;
  the two are not deployed with equivalent replication or sharding.
- Latency is end-to-end (HTTP) and includes Loki's single-process querier plus
  lokihouse's ClickHouse push-down; neither is tuned for low latency.
- `topk` is compared as a multiset of values: when several series tie at the
  top-k boundary, which series wins the tie is undefined between the two
  engines, so the benchmark compares the top-k values, not the selected series.

## Result

### Pre-release measurement (unreleased, after 0.12.0)

Recorded 2026-10-24 on the environment above, with the unreleased query-path
changes after 0.12.0 (200 000 lines, 50 streams, 5 rounds per query). This
run is not a release measurement; it is re-run at every release.
lokihouse is faster than Loki on the six push-down queries (`logfmt filter`,
`count_over_time`, `rate regex`, `topk`, `unwrap quantile`, `bytes_over_time`)
and a few milliseconds behind on `log filter`, `labels` and `series`. The two
changes since 0.12.0 target the two slowest queries: single-stream log
queries read ClickHouse in the part's physical order instead of sorting by
timestamp (`log filter` 9 ms → 8 ms), and sample queries over up to 16 streams
read every stream in parallel, one goroutine per stream, in the part's physical
order, merging the streams in Go (`unwrap quantile` 72 ms → 54 ms, now 14 ms
faster than Loki). `unwrap quantile` is still the most expensive query in the
set: the wall is the engine's own aggregation, which runs Loki's code. The
data set is small and warm, so the label-filter pushdown and windowed reads
of 0.11.0 barely show here; they matter on larger data.

| query | Loki p50 | Loki p95 | lokihouse p50 | lokihouse p95 |
|---|---:|---:|---:|---:|
| log filter | 5ms | 6ms | 8ms | 8ms |
| logfmt filter | 49ms | 50ms | 27ms | 28ms |
| count_over_time | 69ms | 69ms | 14ms | 14ms |
| rate regex | 17ms | 18ms | 12ms | 15ms |
| topk | 33ms | 33ms | 10ms | 11ms |
| unwrap quantile | 68ms | 69ms | 54ms | 55ms |
| bytes_over_time | 40ms | 41ms | 14ms | 15ms |
| labels | 0s | 0s | 2ms | 2ms |
| series | 0s | 0s | 2ms | 2ms |

The previous run (0.12.0, same day, same machine) had lokihouse at 9ms for
`log filter`, 27ms for `logfmt filter`, 12ms for `count_over_time`, 13ms for
`rate regex`, 12ms for `topk`, 72ms for `unwrap quantile` and 14ms for
`bytes_over_time`.

Re-run with `make bench` to refresh; the table is also written to
`.cache/bench/results.md`.
