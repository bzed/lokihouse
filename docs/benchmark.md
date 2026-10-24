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

Recorded 2026-10-24 on the environment above, at lokihouse 0.11.0 (200 000
lines, 50 streams, 5 rounds per query). lokihouse is faster on `logfmt filter`
and the push-down metric queries (`count_over_time`, `topk`, `bytes_over_time`)
and level with Loki on `log filter` and `rate regex`. `unwrap quantile` is slower
than Loki — it is the most expensive query in the set and not yet pushed down.
The data set is small and warm, so the 0.11.0 label-filter pushdown and windowed
reads barely show here; they matter on larger data.

| query | Loki p50 | Loki p95 | lokihouse p50 | lokihouse p95 |
|---|---:|---:|---:|---:|
| log filter | 9ms | 9ms | 8ms | 9ms |
| logfmt filter | 79ms | 79ms | 26ms | 28ms |
| count_over_time | 70ms | 71ms | 12ms | 14ms |
| rate regex | 17ms | 18ms | 15ms | 17ms |
| topk | 33ms | 33ms | 11ms | 12ms |
| unwrap quantile | 69ms | 70ms | 155ms | 157ms |
| bytes_over_time | 33ms | 33ms | 15ms | 16ms |
| labels | 1ms | 1ms | 2ms | 2ms |
| series | 1ms | 1ms | 2ms | 2ms |

The previous run (2026-10-12, 0.5.0) had lokihouse at 9ms for `log filter`, 30ms
for `logfmt filter`, 17ms for `count_over_time`, 13ms for `rate regex`, 13ms for
`topk` and 159ms for `unwrap quantile`.

Re-run with `make bench` to refresh; the table is also written to
`.cache/bench/results.md`.
