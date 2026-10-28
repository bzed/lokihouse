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
- Before timing, the harness waits up to `LOKIHOUSE_BENCH_SETTLE` (default
  `2m`, `0` to skip) until each ClickHouse store has no merge running and a
  stable active part count for its `logs` table. The S3 Loki is fully
  flushed, so settled parts are its counterpart. The results record each
  store's active `logs` part count; a store that does not settle in time is
  marked `(not settled)`.
- Before timing, two full-span queries (a line scan and a parsed-field scan)
  read all data from every store, so that Loki's chunk cache and ClickHouse's
  filesystem cache hold everything the timed queries touch.
- Each query gets two discarded warm-up rounds per store, then 20 timed rounds.
  Every round queries all stores and rotates their order, so background work
  (ClickHouse merges, Loki flushes) does not always land on the same store.
  The median and p90 are reported at 0.1 ms resolution, with the ratio of
  lokihouse's median to Loki's (below 1 means lokihouse is faster).
- Results are written to `$LOKIHOUSE_BENCH_OUT` (default
  `.cache/bench/results.md`) as Markdown tables. Without
  `LOKIHOUSE_BENCH_S3` (when the test is run directly, not through
  `make bench`) only the first table is produced.

### Against a remote S3 store

`make bench-s3` runs only the object-storage scenario against a remote S3
bucket instead of RustFS. The store comes from `.secrets/s3.env` (gitignored;
`S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`; override the file with `BENCH_S3_ENV`). Loki and the
ClickHouse server (`test/bench/up-remote.sh`) each write under a fresh
prefix in the bucket, and `s3cmd` removes what is left when the run ends.
Only Loki and lokihouse with ClickHouse's filesystem cache run there:
uncached ClickHouse reads would measure the network latency to the store,
which Loki's embedded chunk cache hides, so the two are not comparable.
`BENCH_STORES` picks `loki` or `lokihouse-cache` (default both); `LOKIHOUSE_BENCH_LINES`, `_ROUNDS` and `_SPAN` size the
run. Answers are compared with Loki's only when Loki runs in the same
invocation.

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
- On S3 without a cache, every part × column read is its own GET, so unmerged
  parts cost requests. On this data set, 1 part against 22 unmerged parts was
  15 against 35 GETs and 9.7 against 12.3 ms (p90 26 ms) for the `rate regex`
  logs query. A run that is `(not settled)` measures ingest leftovers.
- The data set (≈15 MB uncompressed) fits both caches, so the cached columns measure
  warm reads.
- Latency is end-to-end (HTTP) and includes Loki's single-process querier plus
  lokihouse's ClickHouse push-down. Loki runs its default query tuning; neither
  is tuned further for low latency.
- `topk` is compared as a multiset of values: when several series tie at the
  top-k boundary, which series wins the tie is undefined between the two
  engines, so the benchmark compares the top-k values, not the selected series.

## Result

Recorded 2026-10-28 on the environment above, from `main`: one run of
`make bench`, 200 000 lines, 50 streams, 20 interleaved rounds, with 3 active
ClickHouse parts per store at timing start.

Loki answering recent data from its ingester is the noisiest figure here: its
`logfmt filter` median has ranged from 25 ms to 82 ms across sessions, with
p90 spikes above one second, and its `count_over_time` from 59 ms to 102 ms.
lokihouse's figures and Loki's S3 figures stay within about 10 % between runs.
Compare ratios within one table, not across sessions.

### Recent data, local disk

| query | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| log filter | 9.8ms (11.6ms) | 5.5ms (6.3ms) | 8.3ms (8.7ms) | 0.84 |
| logfmt filter | 40.5ms (44.0ms) | 40.3ms (41.3ms) | 30.2ms (31.2ms) | 0.74 |
| count_over_time | 59.3ms (62.6ms) | 77.1ms (79.8ms) | 12.8ms (14.1ms) | 0.22 |
| rate regex | 15.7ms (16.4ms) | 19.7ms (21.0ms) | 12.7ms (14.2ms) | 0.81 |
| topk | 29.1ms (30.8ms) | 38.0ms (38.9ms) | 11.1ms (12.1ms) | 0.38 |
| unwrap quantile | 55.0ms (56.7ms) | 74.2ms (75.8ms) | 59.7ms (62.3ms) | 1.09 |
| bytes_over_time | 30.0ms (32.7ms) | 38.1ms (39.7ms) | 14.6ms (16.0ms) | 0.49 |
| labels | 0.6ms (0.9ms) | 0.6ms (0.7ms) | 1.7ms (2.5ms) | 2.85 |
| series | 2.1ms (2.5ms) | 0.8ms (1.1ms) | 2.7ms (3.3ms) | 1.26 |

### Object storage (S3 on RustFS and a remote S3 store), Loki flushed

| query | Loki S3 | lokihouse S3 | lokihouse S3 + cache | S3 ÷ Loki | S3 + cache ÷ Loki | Loki, remote S3 | lokihouse + cache, remote S3 | ÷ Loki, remote |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| log filter | 5.3ms (6.4ms) | 15.3ms (17.6ms) | 8.3ms (9.0ms) | 2.89 | 1.56 | 4.9ms (5.9ms) | 7.8ms (9.1ms) | 1.59 |
| logfmt filter | 26.1ms (27.9ms) | 49.5ms (54.4ms) | 32.3ms (34.0ms) | 1.90 | 1.24 | 26.1ms (27.3ms) | 33.4ms (34.9ms) | 1.28 |
| count_over_time | 56.9ms (58.5ms) | 22.8ms (28.1ms) | 13.1ms (15.1ms) | 0.40 | 0.23 | 65.1ms (67.0ms) | 11.5ms (13.4ms) | 0.18 |
| rate regex | 15.1ms (16.9ms) | 21.9ms (34.4ms) | 13.2ms (14.2ms) | 1.45 | 0.88 | 17.0ms (18.2ms) | 12.9ms (14.4ms) | 0.76 |
| topk | 28.3ms (29.4ms) | 20.1ms (23.0ms) | 11.8ms (12.5ms) | 0.71 | 0.42 | 31.8ms (33.0ms) | 10.8ms (12.0ms) | 0.34 |
| unwrap quantile | 53.0ms (55.0ms) | 65.6ms (68.8ms) | 58.2ms (59.6ms) | 1.24 | 1.10 | 60.5ms (62.7ms) | 62.7ms (65.4ms) | 1.04 |
| bytes_over_time | 28.3ms (29.7ms) | 24.1ms (33.8ms) | 15.0ms (17.2ms) | 0.85 | 0.53 | 31.3ms (33.1ms) | 13.5ms (15.3ms) | 0.43 |
| labels | 0.6ms (0.9ms) | 4.0ms (5.3ms) | 1.8ms (2.7ms) | 6.13 | 2.75 | 0.6ms (0.9ms) | 1.7ms (2.0ms) | 3.06 |
| series | 1.9ms (3.5ms) | 6.0ms (7.5ms) | 3.1ms (3.5ms) | 3.17 | 1.61 | 1.8ms (2.1ms) | 2.5ms (3.1ms) | 1.37 |

The first five columns are RustFS on the same host (`make bench`); the last
three are one separate run (same day, freshly generated data) against a remote S3
bucket (`make bench-s3`, see [Against a remote S3 store](#against-a-remote-s3-store)).
The remote run measures only Loki and lokihouse with the filesystem cache,
which is why it has no uncached column. The cache hides the store's latency
for both systems, so the remote ratios land close to the RustFS ones
(`count_over_time` 0.18× against 0.23×, `topk` 0.34× against 0.42×,
`log filter` 1.59× against 1.56×); the remote figures say little about cold
reads. Compare ratios within a run, not absolute times across runs.

**Recent data.** lokihouse is 1.4–4.5× faster than Loki on the queries the
SQL push-down answers (`logfmt filter`, `count_over_time`, `topk`,
`bytes_over_time`), 1.2× on `rate regex`, 1.2× on `log filter`, and about
level on `unwrap quantile`.

- **`log filter` (0.84×).** The tenant's delete requests come from Loki's
  delete-requests cache (`deletion.NewDeleteRequestsClient`, refreshed every
  5 minutes) and no longer cost a ClickHouse round trip per query. lokihouse
  resolves the stream on `streams` (≈2 ms) before the 3 ms read; Loki's
  ingester answered in 9.8 ms in this run, but 5.5 ms unsharded, so the
  unsharded Loki (5.5 ms) is faster than lokihouse.
- **`unwrap quantile` (1.09×).** It runs in lokihouse's own copy of Loki's
  engine, single-threaded per query; Loki shards it and evaluates the shards
  in parallel.
- **`labels` (2.9×) and `series` (1.3×).** Each is one ClickHouse query of
  about 1 ms plus the round trip, against Loki's in-memory ingester index;
  about 1 ms in absolute terms.

**Object storage.** With both sides reading from S3 and both caches warm,
lokihouse (S3 + cache) runs within 2 ms of its local-disk numbers and keeps
its lead on the aggregations (`count_over_time` 0.23×, `topk` 0.42×,
`bytes_over_time` 0.53×, `rate regex` 0.88×). Loki, reading flushed chunks
through its chunk cache, is faster and steadier on the line-returning
queries than from its own ingester (`logfmt filter` 26 ms against 41 ms), so
lokihouse loses those (`logfmt filter` 1.24×, `log filter` 1.56×, +3 ms),
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

### With time-split engine queries

Recorded 2026-10-25 on the same host, after metric range queries that run in
lokihouse's own engine were split by `split_queries_by_interval` (default `1h`)
and evaluated in parallel. 200 000 lines, 50 streams, 20 interleaved rounds,
median (p90). Every store's answers were compared with Loki's.

| `unwrap quantile` over | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| 1 h (default) | 82.7ms (89.3ms) | 86.5ms (91.7ms) | 68.5ms (71.7ms) | 0.83 |
| 6 h (`LOKIHOUSE_BENCH_SPAN=6h`) | 45.1ms (71.7ms) | 123.2ms (124.8ms) | 34.6ms (35.8ms) | 0.77 |

A 1 h range is one split bucket, so lokihouse runs it as before (the 1.11 above
became 0.83 only because Loki was slower in this session); the gain from
splitting grows with the range, and the 6 h run is where the legs run in
parallel. The 6 h run keeps the data in Loki's ingester (`max_chunk_age: 24h`),
because flushed chunks drop out of its results after about 2 h 40 min.

Re-run with `make bench` to refresh (`LOKIHOUSE_BENCH_SPAN` sets the data and
query span); the tables are also written to `.cache/bench/results.md`.

