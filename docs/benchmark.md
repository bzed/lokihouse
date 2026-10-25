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
changes after 0.12.0, including the delete-requests cache. Two runs agreed
within 0.5 ms per query; this is the second.

| query | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| log filter | 4.7ms (6.1ms) | 3.5ms (4.0ms) | 7.0ms (7.6ms) | 1.51 |
| logfmt filter | 50.1ms (1360.1ms) | 37.5ms (38.5ms) | 29.7ms (31.4ms) | 0.59 |
| count_over_time | 69.2ms (72.7ms) | 71.9ms (73.9ms) | 12.4ms (12.8ms) | 0.18 |
| rate regex | 16.6ms (17.7ms) | 17.6ms (18.9ms) | 12.5ms (13.1ms) | 0.75 |
| topk | 32.7ms (33.8ms) | 33.9ms (35.7ms) | 10.7ms (11.4ms) | 0.33 |
| unwrap quantile | 61.8ms (64.7ms) | 68.6ms (70.7ms) | 68.5ms (69.5ms) | 1.11 |
| bytes_over_time | 32.0ms (38.4ms) | 42.4ms (43.1ms) | 13.7ms (14.0ms) | 0.43 |
| labels | 0.4ms (0.5ms) | 0.4ms (0.5ms) | 1.5ms (1.7ms) | 3.33 |
| series | 1.6ms (1.9ms) | 0.6ms (0.7ms) | 1.8ms (2.2ms) | 1.14 |

lokihouse is 2–4× faster than Loki on the five queries the SQL push-down
answers (`logfmt filter`, `count_over_time`, `topk`, `bytes_over_time`, and
`rate regex` at about par). It is slower on four:

- **`unwrap quantile` (1.23×).** It runs in lokihouse's own copy of Loki's
  engine, and that copy is single-threaded per query. The engine's
  aggregation is about 38 ms of the 70 ms. Loki shards the query (each
  `by (app)` series lives in one shard) and evaluates the shards in
  parallel. Unsharded, Loki is level with lokihouse (68 ms against 70 ms).
- **`log filter` (1.51×, +2.3 ms).** lokihouse answers it in 7.0 ms, down
  from 8.7 ms: the tenant's delete requests now come from Loki's
  delete-requests cache (`deletion.NewDeleteRequestsClient`, refreshed every
  5 minutes) and no longer cost a ClickHouse round trip per query. What
  remains is the stream resolution on `streams` (≈2 ms) before the 3 ms read,
  against Loki's in-memory index. The ratio is higher than before only because
  Loki itself measured 4.7 ms in these runs against 6.7 ms in the earlier
  ones (unsharded 3.5 ms against 6.9 ms); Loki's figure for this query varies
  more between sessions than lokihouse's.
- **`labels` (3.8×) and `series` (1.2×).** Each is one ClickHouse query of
  about 1 ms plus the round trip, against Loki's in-memory ingester index.
  The gap is about 1 ms in absolute terms.

The data set is small and warm, and Loki serves it from in-memory chunks. The
label-filter pushdown and windowed reads of 0.11.0 barely show here; they
matter on larger data.

Re-run with `make bench` to refresh; the table is also written to
`.cache/bench/results.md`.
