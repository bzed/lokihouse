# Benchmark

`make bench` (`test/oracle/bench_test.go`, build tags `oracle bench`) measures
lokihouse query latency against Loki on identical data and identical queries.
It is a correctness-gated benchmark: every query's response from every store is
compared with Loki's using the oracle's `Compare`, so a fast-but-wrong answer
fails the run.

## Method

- `make bench` starts the benchmark's own servers with podman
  (`test/bench/up.sh`; stop them with `make bench-down`): RustFS 1.0.3 as the
  S3 store, and three single-node ClickHouse 26.9 servers whose MergeTree
  tables live on local disk, on S3 (RustFS), and on S3 behind ClickHouse's
  local filesystem cache.
- Every store ingests the same deterministic, seeded data set: 200 000 lines
  over 10 distinct streams (`app` 0–9, even apps `env=dev`, odd apps
  `env=prod`), spread over the final hour, pushed in 5 000-line batches.
  Tweak with `LOKIHOUSE_BENCH_LINES`, `LOKIHOUSE_BENCH_STREAMS` and
  `LOKIHOUSE_BENCH_ROUNDS`.
- **Recent data, local disk** (the first table):
  - **Loki** is a single binary on the local filesystem with its default
    query tuning: query sharding (`parallelise_shardable_queries`) and
    splitting on, the results cache off. The data is recent, so its
    ingester answers from memory.
  - **Loki (unsharded)** is the oracle's configuration, with sharding and
    splitting off, so the plain engine answers. It shows what Loki's
    parallelism buys.
  - **lokihouse** runs with its defaults on ClickHouse with local disk.
- **Object storage** (the second table), the usual production layout:
  - **Loki S3** keeps chunks, TSDB index and delete requests in an S3
    bucket, with the same query tuning as **Loki**. A writer Loki ingests the
    data and shuts down with `flush_on_shutdown`, which flushes every chunk
    and uploads the index. A second Loki then starts on the same bucket with
    an empty ingester and answers the queries, so every chunk comes from S3,
    as for data older than the ingesters keep. Loki's embedded chunk cache is
    on (Loki enables it whenever no other chunk cache is configured, and it
    cannot be switched off), so after the warm-up rounds chunks are served
    from memory.
  - **lokihouse S3** reads from ClickHouse with its MergeTree tables on S3
    and no local cache: every read goes to RustFS.
  - **lokihouse S3 + cache** adds ClickHouse's filesystem cache in front of
    S3 (as ClickHouse Cloud does), the counterpart to Loki's chunk cache.
- After ingesting, the harness waits until every Loki counts every line,
  then runs nine representative LogQL queries: plain and regex log filters,
  `count_over_time`, `rate`, `topk`, `unwrap` + `quantile_over_time`,
  `bytes_over_time`, and the `labels` and `series` metadata endpoints.
- Each query gets two discarded warm-up rounds per store, then 20 timed rounds.
  Every round queries all stores and rotates their order, so background work
  (ClickHouse merges, Loki flushes) does not always land on the same store.
  The median and p90 are reported at 0.1 ms resolution, with the ratio of
  lokihouse's median to Loki's (below 1 means lokihouse is faster).
- Results are written to `$LOKIHOUSE_BENCH_OUT` (default
  `.cache/bench/results.md`) as Markdown tables. Without
  `LOKIHOUSE_BENCH_S3` (when the test is run directly, not through
  `make bench`) only the first table is produced.

### Why RustFS

MinIO's community edition stopped publishing images in October 2025 and its
repository was archived in February 2026; `docker.io/minio/minio` can no
longer be pulled. RustFS is an Apache-2.0, S3-compatible server that reached
1.0 in September 2026 and publishes versioned images on Docker Hub. The
benchmark needs only plain S3 (bucket creation, GET/PUT/LIST, path-style
addressing), which both Loki's Thanos client and ClickHouse's S3 disk use
against it unchanged.

## Caveats

- In the first table Loki serves recent data from the ingester's in-memory
  chunks, while lokihouse reads from ClickHouse. This benchmark measures a
  single node, not scale-out; the two are not deployed with equivalent
  replication or sharding.
- RustFS runs on the same host, so an S3 request costs a local HTTP round
  trip, not the 10–50 ms of a real S3 GET. Remote object storage widens the
  gap between cached and uncached reads for both systems; Loki's chunk cache
  and ClickHouse's filesystem cache are what hide it in production.
- The data set (≈15 MB uncompressed) fits both caches, so the cached columns measure
  warm reads.
- Latency is end-to-end (HTTP) and includes Loki's single-process querier plus
  lokihouse's ClickHouse push-down. Loki runs its default query tuning; neither
  is tuned further for low latency.
- `topk` is compared as a multiset of values: when several series tie at the
  top-k boundary, which series wins the tie is undefined between the two
  engines, so the benchmark compares the top-k values, not the selected series.

## Result

Recorded 2026-10-25 on the environment above, from `main` with the
delete-requests cache (#15). Two runs agreed within about 10 % per query,
except Loki's in the first table; this is the second.

Loki answering recent data from its ingester is the noisiest figure here: its
`logfmt filter` median was 25 ms in one run and 59 ms in the other (82 ms in an
earlier session), with p90 spikes above one second, and its
`count_over_time` moved between 68 ms and 102 ms across sessions. lokihouse's
figures and Loki's S3 figures stay within about 10 % between runs. Compare
ratios within one table, not across sessions.

### Recent data, local disk

| query | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| log filter | 6.9ms (8.1ms) | 3.9ms (4.6ms) | 7.6ms (8.5ms) | 1.10 |
| logfmt filter | 59.4ms (1028.1ms) | 38.1ms (39.4ms) | 29.2ms (31.8ms) | 0.49 |
| count_over_time | 98.1ms (101.7ms) | 76.0ms (78.5ms) | 12.3ms (13.1ms) | 0.12 |
| rate regex | 18.0ms (19.2ms) | 18.4ms (19.8ms) | 13.1ms (13.7ms) | 0.73 |
| topk | 33.4ms (36.3ms) | 36.3ms (38.2ms) | 10.8ms (11.9ms) | 0.32 |
| unwrap quantile | 65.8ms (68.7ms) | 73.0ms (77.4ms) | 68.7ms (70.7ms) | 1.04 |
| bytes_over_time | 33.9ms (35.2ms) | 36.6ms (38.4ms) | 13.6ms (14.5ms) | 0.40 |
| labels | 0.7ms (0.9ms) | 0.6ms (0.7ms) | 1.7ms (2.2ms) | 2.55 |
| series | 1.9ms (2.2ms) | 0.7ms (0.9ms) | 2.1ms (2.7ms) | 1.15 |

### Object storage (S3 on RustFS), Loki flushed

| query | Loki S3 | lokihouse S3 | lokihouse S3 + cache | lokihouse S3 ÷ Loki S3 | lokihouse S3 + cache ÷ Loki S3 |
|---|---:|---:|---:|---:|---:|
| log filter | 5.2ms (6.5ms) | 14.6ms (15.7ms) | 8.0ms (8.8ms) | 2.82 | 1.55 |
| logfmt filter | 23.1ms (24.6ms) | 48.0ms (53.9ms) | 30.8ms (33.7ms) | 2.07 | 1.33 |
| count_over_time | 66.5ms (68.0ms) | 20.7ms (22.3ms) | 12.7ms (13.0ms) | 0.31 | 0.19 |
| rate regex | 17.6ms (19.5ms) | 21.0ms (23.9ms) | 13.4ms (14.1ms) | 1.19 | 0.76 |
| topk | 32.4ms (33.5ms) | 18.7ms (20.2ms) | 11.2ms (11.9ms) | 0.58 | 0.35 |
| unwrap quantile | 63.5ms (65.4ms) | 82.8ms (85.2ms) | 70.1ms (72.5ms) | 1.30 | 1.10 |
| bytes_over_time | 31.7ms (32.9ms) | 22.8ms (25.6ms) | 14.0ms (15.5ms) | 0.72 | 0.44 |
| labels | 0.6ms (0.8ms) | 4.0ms (4.8ms) | 1.9ms (2.1ms) | 6.31 | 3.05 |
| series | 1.8ms (1.9ms) | 5.4ms (6.3ms) | 2.6ms (3.0ms) | 2.98 | 1.44 |

**Recent data.** lokihouse is 2–8× faster than Loki on the queries the SQL
push-down answers (`logfmt filter`, `count_over_time`, `topk`,
`bytes_over_time`), 1.4× on `rate regex`, and about level on `log filter`
and `unwrap quantile`.

- **`log filter` (1.10×, +0.7 ms).** The tenant's delete requests come from
  Loki's delete-requests cache (`deletion.NewDeleteRequestsClient`,
  refreshed every 5 minutes) and no longer cost a ClickHouse round trip per
  query. What remains is the stream resolution on `streams` (≈2 ms) before
  the 3 ms read, against Loki's in-memory index.
- **`unwrap quantile` (1.04×).** It runs in lokihouse's own copy of Loki's
  engine, single-threaded per query; Loki shards it and evaluates the shards
  in parallel.
- **`labels` (2.6×) and `series` (1.2×).** Each is one ClickHouse query of
  about 1 ms plus the round trip, against Loki's in-memory ingester index;
  about 1 ms in absolute terms.

**Object storage.** With both sides reading from S3 and both caches warm,
lokihouse (S3 + cache) runs within 2 ms of its local-disk numbers and keeps
its lead on the aggregations (`count_over_time` 0.19×, `topk` 0.35×,
`bytes_over_time` 0.44×, `rate regex` 0.76×). Loki, reading flushed chunks
through its chunk cache, is faster and steadier on the line-returning
queries than from its own ingester (`logfmt filter` 23 ms against 59 ms), so
lokihouse loses those (`logfmt filter` 1.33×, `log filter` 1.55×, +3 ms),
`unwrap quantile` (1.10×), and the metadata endpoints, whose Loki answers
come from the downloaded TSDB index in under 2 ms.

Without its filesystem cache, ClickHouse fetches parts from S3 on every
query and adds 2–17 ms; it still beats Loki on the three heaviest
aggregations. Loki has no comparable uncached mode: its embedded chunk cache
is always on unless another cache replaces it.

The data set is small and warm, and in the first table Loki serves it from
in-memory chunks. The
label-filter pushdown and windowed reads of 0.11.0 barely show here; they
matter on larger data.

Re-run with `make bench` to refresh; the tables are also written to
`.cache/bench/results.md`.
