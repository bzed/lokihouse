# Benchmark

`make bench` (`test/oracle/bench_test.go`, build tags `oracle bench`) measures
lokihouse query latency against Loki on identical data and identical queries,
in two lokihouse configurations: the default (optimal) one and the
single-connection fallback one. It is a correctness-gated benchmark: every
query's response from all three stores is compared with the oracle's `Compare`,
so a fast-but-wrong answer fails the run.

## Method

- All three stores ingest the same deterministic, seeded data set: 200 000
  lines over 50 streams (10 apps × dev/prod), spread over the final hour,
  pushed in 5 000-line batches. Tweak with `LOKIHOUSE_BENCH_LINES`,
  `LOKIHOUSE_BENCH_STREAMS` and `LOKIHOUSE_BENCH_ROUNDS`.
- The three stores are Loki, lokihouse with the default configuration (the
  optimal settings for this hardware), and lokihouse in the fallback case:
  `clickhouse.max_open_conns = 2`, the smallest pool that can ingest (a push
  with new streams holds two connections — the logs batch and the stream
  registration), which caps the sample fan-out's connection budget at one, so
  every sample query reads all streams in one query and every query runs on a
  single connection.
- After ingesting, the harness waits until Loki counts every line (its readiness
  gate), then runs nine representative LogQL queries — plain and regex log
  filters, `count_over_time`, `rate`, `topk`, `unwrap` + `quantile_over_time`,
  `bytes_over_time`, and the `labels` and `series` metadata endpoints.
- Each query is timed over five rounds (first round included; the data is
  already warm) against all three stores. The fastest and slowest round
  (min–max) are reported per store; responses are compared for equality before
  they are counted.
- Results are written to `$LOKIHOUSE_BENCH_OUT` (default
  `.cache/bench/results.md`) as a Markdown table.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 9900X (12 cores / 24 threads) |
| RAM | 60 GiB |
| Host | single node; all binaries on the same host |
| lokihouse | ClickHouse 26.9, single node in Podman |
| Loki | single binary, filesystem storage (no S3, no object store) |

ClickHouse runs queries in parallel up to its `max_concurrent_queries`
setting, which defaults to 100 for local installs; the right value depends on
the hardware (cores, memory, disk). On ClickHouse Cloud, running queries in
parallel is mainly limited by the service tier you pay for. lokihouse
multiplies that parallelism on the client side: a metric query on up to 16
streams holds up to 16 connections from its own pool
(`clickhouse.max_open_conns`, default 100). The benchmark runs with the
defaults throughout — the optimal settings for this machine.

## Caveats

- Loki serves recent data from the ingester's in-memory chunks, while lokihouse
  reads from ClickHouse. This benchmark measures a single node, not scale-out;
  the two are not deployed with equivalent replication or sharding.
- Latency is end-to-end (HTTP) and includes Loki's single-process querier plus
  lokihouse's ClickHouse push-down; neither is tuned for low latency.
- The three stores ingest the same 200 000 lines, so the ClickHouse server
  keeps merging parts in the background while the queries are timed; absolute
  numbers wobble between runs. The min–max spread makes the wobble visible; the
  per-query comparison between the stores is the signal.
- The single-connection column is the fallback floor, not a second tuning:
  with one connection per query the fan-out never engages, so sample queries
  read all streams in one query — the shape a saturated pool degrades to. For
  a single sequential user it costs nothing measurable here; the fan-out's
  value is concurrency, which this benchmark does not measure. In production
  these fallbacks are counted in `lokihouse_chstore_fanout_fallbacks_total`
  and logged (see [operations](operations.md#query)).
- `topk` is compared as a multiset of values: when several series tie at the
  top-k boundary, which series wins the tie is undefined between the two
  engines, so the benchmark compares the top-k values, not the selected series.

## Result

### Pre-release measurement (unreleased, after 0.12.0)

Recorded 2026-10-25 on the environment above, with the unreleased query-path
changes after 0.12.0 (200 000 lines, 50 streams, 5 rounds per query). This
run is not a release measurement; it is re-run at every release.
lokihouse is faster than Loki on the five push-down queries (`logfmt filter`,
`count_over_time`, `rate regex`, `topk`, `bytes_over_time`) and on par or a few
milliseconds behind on `log filter`, `unwrap quantile`, `labels` and `series`.
The single-connection fallback costs nothing measurable for a single
sequential user: the fan-out's value is concurrency, which this benchmark does
not exercise. The two query-path changes since 0.12.0 do not show on this
benchmark's query shapes: `log filter` matches five streams, but the
part-order read is single-stream, and `unwrap quantile` matches 25 streams,
above the 16-stream fan-out limit — both sit at their 0.12.0 values within
noise (`rate regex` matches 15 streams and does fan out, without a measurable
difference here). `unwrap quantile` is the most expensive query in the set:
the wall is the engine's own aggregation, which runs Loki's code. The data set
is small and warm, so the label-filter pushdown and windowed reads of 0.11.0
barely show here; they matter on larger data.

| query | Loki | lokihouse (optimal) | lokihouse (single connection) |
|---|---:|---:|---:|
| log filter | 8ms–11ms | 7ms–10ms | 6ms–8ms |
| logfmt filter | 84ms–87ms | 27ms–31ms | 25ms–28ms |
| count_over_time | 82ms–85ms | 12ms–19ms | 10ms–12ms |
| rate regex | 21ms–23ms | 12ms–18ms | 12ms–13ms |
| topk | 37ms–39ms | 10ms–12ms | 10ms–11ms |
| unwrap quantile | 78ms–81ms | 70ms–72ms | 66ms–70ms |
| bytes_over_time | 41ms–45ms | 14ms–20ms | 13ms–15ms |
| labels | 0s–1ms | 2ms–2ms | 1ms–2ms |
| series | 0s–1ms | 2ms–2ms | 1ms–2ms |

The previous release run (0.12.0, same machine) had lokihouse at 9ms for
`log filter`, 27ms for `logfmt filter`, 12ms for `count_over_time`, 13ms for
`rate regex`, 12ms for `topk`, 72ms for `unwrap quantile` and 14ms for
`bytes_over_time`.

Re-run with `make bench` to refresh; the table is also written to
`.cache/bench/results.md`.
