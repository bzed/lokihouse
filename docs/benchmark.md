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

Recorded 2026-10-28 on the environment above, from `main` with the speculative
stream read: one run of `make bench`, 200 000 lines, 50 streams, 20
interleaved rounds, with 3 active ClickHouse parts per store at timing start.
The remote S3 columns are from the earlier `make bench-s3` run (same day,
before growing keyset chunks and the speculative stream read), not repeated
here.

Loki answering recent data from its ingester is the noisiest figure here: its
`logfmt filter` median has ranged from 25 ms to 82 ms across sessions, with
p90 spikes above one second, and its `count_over_time` from 59 ms to 102 ms.
lokihouse's figures and Loki's S3 figures stay within about 10 % between runs.
Compare ratios within one table, not across sessions.

### Recent data, local disk

| query | Loki | Loki (unsharded) | lokihouse | lokihouse ÷ Loki |
|---|---:|---:|---:|---:|
| log filter | 5.5ms (6.5ms) | 5.3ms (5.9ms) | 5.5ms (6.1ms) | 1.00 |
| logfmt filter | 45.6ms (49.4ms) | 49.8ms (52.2ms) | 19.1ms (20.6ms) | 0.42 |
| count_over_time | 53.1ms (54.9ms) | 107.7ms (112.8ms) | 13.7ms (15.2ms) | 0.26 |
| rate regex | 13.7ms (14.9ms) | 18.8ms (20.5ms) | 13.5ms (14.8ms) | 0.99 |
| topk | 25.4ms (27.0ms) | 35.9ms (37.4ms) | 11.4ms (12.2ms) | 0.45 |
| unwrap quantile | 49.3ms (51.2ms) | 74.4ms (78.0ms) | 56.2ms (60.6ms) | 1.14 |
| bytes_over_time | 25.4ms (27.1ms) | 36.6ms (37.9ms) | 15.2ms (15.8ms) | 0.60 |
| labels | 0.9ms (1.1ms) | 0.8ms (1.0ms) | 2.4ms (2.7ms) | 2.67 |
| series | 2.1ms (3.0ms) | 0.8ms (0.9ms) | 2.9ms (3.2ms) | 1.38 |

### Object storage (S3 on RustFS and a remote S3 store), Loki flushed

| query | Loki S3 | lokihouse S3 | lokihouse S3 + cache | S3 ÷ Loki | S3 + cache ÷ Loki | Loki, remote S3 | lokihouse + cache, remote S3 | ÷ Loki, remote |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| log filter | 4.8ms (5.9ms) | 9.7ms (13.1ms) | 5.5ms (6.0ms) | 2.01 | 1.15 | 4.9ms (5.9ms) | 7.8ms (9.1ms) | 1.59 |
| logfmt filter | 28.5ms (29.6ms) | 31.0ms (32.6ms) | 19.7ms (21.1ms) | 1.09 | 0.69 | 26.1ms (27.3ms) | 33.4ms (34.9ms) | 1.28 |
| count_over_time | 50.7ms (52.8ms) | 23.3ms (32.3ms) | 14.1ms (15.8ms) | 0.46 | 0.28 | 65.1ms (67.0ms) | 11.5ms (13.4ms) | 0.18 |
| rate regex | 13.2ms (14.7ms) | 22.1ms (23.4ms) | 13.6ms (14.5ms) | 1.67 | 1.03 | 17.0ms (18.2ms) | 12.9ms (14.4ms) | 0.76 |
| topk | 24.4ms (25.3ms) | 19.1ms (21.5ms) | 12.1ms (13.2ms) | 0.78 | 0.50 | 31.8ms (33.0ms) | 10.8ms (12.0ms) | 0.34 |
| unwrap quantile | 47.5ms (50.3ms) | 61.7ms (67.1ms) | 55.5ms (56.6ms) | 1.30 | 1.17 | 60.5ms (62.7ms) | 62.7ms (65.4ms) | 1.04 |
| bytes_over_time | 24.1ms (25.2ms) | 25.3ms (35.8ms) | 15.0ms (16.1ms) | 1.05 | 0.62 | 31.3ms (33.1ms) | 13.5ms (15.3ms) | 0.43 |
| labels | 0.7ms (0.9ms) | 5.1ms (6.6ms) | 2.5ms (3.1ms) | 7.13 | 3.55 | 0.6ms (0.9ms) | 1.7ms (2.0ms) | 3.06 |
| series | 2.0ms (2.4ms) | 6.6ms (7.1ms) | 3.0ms (3.2ms) | 3.28 | 1.48 | 1.8ms (2.1ms) | 2.5ms (3.1ms) | 1.37 |

The first five columns are RustFS on the same host (`make bench`); the last
three are one separate, earlier run (same day, freshly generated data, before
the speculative stream read) against a remote S3 bucket (`make bench-s3`, see [Against a remote S3 store](#against-a-remote-s3-store)).
The remote run measures only Loki and lokihouse with the filesystem cache,
which is why it has no uncached column. The cache hides the store's latency
for both systems, so the remote ratios land close to the RustFS ones
(`count_over_time` 0.18× against 0.28×, `topk` 0.34× against 0.50×); the
remote `log filter` (1.59×) predates the speculative stream read and would
be lower now. The remote figures say little about cold reads. Compare ratios within a run, not absolute times across runs.

**Recent data.** lokihouse is 1.7–3.8× faster than Loki on the queries the
SQL push-down answers (`logfmt filter`, `count_over_time`, `topk`,
`bytes_over_time`), level on `rate regex` (0.99×) and `log filter` (1.00×),
and 1.14× slower on `unwrap quantile`.

- **`logfmt filter` (0.42×).** The filter runs in Go, so most rows of a chunk
  are dropped. Keyset chunks now grow 4× per round trip (1000, then 4000 rows),
  so the query needs 2 chunk reads instead of 4, which took it from 30 ms to
  about 20 ms.

- **`log filter` (1.00×).** The tenant's delete requests come from Loki's
  delete-requests cache (`deletion.NewDeleteRequestsClient`, refreshed every
  5 minutes) and no longer cost a ClickHouse round trip per query. A repeated
  selector starts its 3 ms `logs` read from the cached stream set while the
  ≈2 ms `streams` lookup runs, so the latency is the longer of the two
  instead of their sum (8.1 ms before, 5.5 ms now); the fresh lookup then
  confirms the stream set, or the read is redone (counter
  `lokihouse_query_stream_speculations_total`). The first query of a selector
  still pays both in sequence.
- **`unwrap quantile` (1.14×).** It runs in lokihouse's own copy of Loki's
  engine, single-threaded per query; Loki shards it and evaluates the shards
  in parallel.
- **`labels` (2.7×) and `series` (1.4×).** Each is one ClickHouse query of
  about 1 ms plus the round trip, against Loki's in-memory ingester index;
  about 1 ms in absolute terms.

**Object storage.** With both sides reading from S3 and both caches warm,
lokihouse (S3 + cache) runs within 1 ms of its local-disk numbers and keeps
its lead on the aggregations (`count_over_time` 0.28×, `topk` 0.50×,
`bytes_over_time` 0.62×) and on `logfmt filter` (0.69×, 20 ms against
29 ms), and is level on `rate regex` (1.03×). Loki, reading flushed chunks
through its chunk cache, is faster and steadier on the line-returning
queries than from its own ingester (`logfmt filter` 29 ms against 46 ms), so
lokihouse still loses `log filter` (1.15×, +0.7 ms), `unwrap quantile`
(1.17×), and the metadata endpoints, whose Loki answers come from the
downloaded TSDB index in under 2 ms.

Without its filesystem cache, ClickHouse fetches parts from S3 on every
query and adds 2–13 ms; it still beats Loki on the three heaviest
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

