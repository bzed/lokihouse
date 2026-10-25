# Benchmark

`make bench` (`test/oracle/bench_test.go`, build tags `oracle bench`) measures
lokihouse query latency against Loki on identical data and identical queries.
It is a correctness-gated benchmark: every query's response from every store is
compared with Loki's using the oracle's `Compare`, so a fast-but-wrong answer
fails the run.

## Method

- Three stores ingest the same deterministic, seeded data set: 200 000 lines
  over 10 distinct streams (`app` 0–9, even apps `env=dev`, odd apps
  `env=prod`), spread over the final hour, pushed in 5 000-line batches.
  Tweak with `LOKIHOUSE_BENCH_LINES`, `LOKIHOUSE_BENCH_STREAMS` and
  `LOKIHOUSE_BENCH_ROUNDS`.
- **Loki** runs with its default query tuning: query sharding
  (`parallelise_shardable_queries`) and splitting on, the results cache off.
  **Loki (unsharded)** is the oracle's configuration, with sharding and
  splitting off, so the plain engine answers. It shows what Loki's parallelism
  buys. **lokihouse** runs with its defaults.
- After ingesting, the harness waits until both Lokis count every line, then
  runs nine representative LogQL queries: plain and regex log filters,
  `count_over_time`, `rate`, `topk`, `unwrap` + `quantile_over_time`,
  `bytes_over_time`, and the `labels` and `series` metadata endpoints.
- Each query gets two discarded warm-up rounds per store, then 20 timed rounds.
  Every round queries all three stores and rotates their order, so background
  work (ClickHouse merges, Loki flushes) does not always land on the same
  store. The median and p90 are reported at 0.1 ms resolution, with the
  ratio of lokihouse's median to Loki's (below 1 means lokihouse is faster).
- Results are written to `$LOKIHOUSE_BENCH_OUT` (default
  `.cache/bench/results.md`) as a Markdown table.

## Environment

| | |
| --- | --- |
| CPU | AMD Ryzen 9 9900X (12 cores / 24 threads) |
| RAM | 60 GiB |
| Host | single node; all servers on the same host |
| lokihouse | ClickHouse 26.9, single node on the same host |
| Loki | single binary, filesystem storage (no S3, no object store) |

## Caveats

- Loki serves recent data from the ingester's in-memory chunks, while lokihouse
  reads from ClickHouse. This benchmark measures a single node, not scale-out;
  the two are not deployed with equivalent replication or sharding.
- Latency is end-to-end (HTTP) and includes Loki's single-process querier plus
  lokihouse's ClickHouse push-down. Loki runs its default query tuning; neither
  is tuned further for low latency.
- `topk` is compared as a multiset of values: when several series tie at the
  top-k boundary, which series wins the tie is undefined between the two
  engines, so the benchmark compares the top-k values, not the selected series.

## Result

Recorded 2026-10-25 on the environment above, with the unreleased query-path
changes after 0.12.0. Two runs agreed within 0.5 ms per query; this is the
second.

| query | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| log filter | 6.7ms (8.1ms) | 6.9ms (8.2ms) | 8.7ms (9.4ms) | 1.30 |
| logfmt filter | 66.7ms (68.1ms) | 66.8ms (68.7ms) | 28.9ms (31.1ms) | 0.43 |
| count_over_time | 58.0ms (60.2ms) | 68.6ms (71.2ms) | 13.6ms (14.3ms) | 0.23 |
| rate regex | 14.6ms (15.6ms) | 17.9ms (18.6ms) | 13.7ms (14.5ms) | 0.94 |
| topk | 27.8ms (29.4ms) | 33.7ms (34.3ms) | 12.0ms (12.4ms) | 0.43 |
| unwrap quantile | 57.0ms (58.3ms) | 68.3ms (73.7ms) | 69.8ms (71.6ms) | 1.23 |
| bytes_over_time | 31.7ms (33.6ms) | 45.8ms (47.0ms) | 14.7ms (15.5ms) | 0.46 |
| labels | 0.4ms (0.7ms) | 0.4ms (0.5ms) | 1.6ms (2.0ms) | 3.77 |
| series | 1.5ms (2.0ms) | 0.5ms (0.8ms) | 1.8ms (2.2ms) | 1.16 |

lokihouse is 2–4× faster than Loki on the five queries the SQL push-down
answers (`logfmt filter`, `count_over_time`, `topk`, `bytes_over_time`, and
`rate regex` at about par). It is slower on four:

- **`unwrap quantile` (1.23×).** It runs in lokihouse's own copy of Loki's
  engine, and that copy is single-threaded per query. The engine's
  aggregation is about 38 ms of the 70 ms. Loki shards the query (each
  `by (app)` series lives in one shard) and evaluates the shards in
  parallel. Unsharded, Loki is level with lokihouse (68 ms against 70 ms).
- **`log filter` (1.30×, +2 ms).** Before it reads a single row, lokihouse
  makes two ClickHouse round trips in sequence: the tenant's delete requests
  (≈2 ms, every query) and the stream resolution on `streams` (≈2 ms). Then
  the 3 ms read follows. Loki caches delete requests for 5 minutes in its
  querier (`deletion.NewDeleteRequestsClient`) and resolves streams from the
  in-memory index. Every range query pays the delete-request round trip in
  lokihouse; it only shows here because this query is otherwise short.
- **`labels` (3.8×) and `series` (1.2×).** Each is one ClickHouse query of
  about 1 ms plus the round trip, against Loki's in-memory ingester index.
  The gap is about 1 ms in absolute terms.

The data set is small and warm, and Loki serves it from in-memory chunks. The
label-filter pushdown and windowed reads of 0.11.0 barely show here; they
matter on larger data.

Re-run with `make bench` to refresh; the table is also written to
`.cache/bench/results.md`.
