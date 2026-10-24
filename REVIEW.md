# Review: branch `perf/part-order-stream-fanout` (HEAD `8e5572a`)

Reviewer: Claude Opus 5.5 (second pair of eyes)
Date: 2026-10-24

This file holds two passes. §0 is the re-review of the fix commits
`de5cc68..8e5572a` against the findings of the first pass. §1–§8 are the
first pass at `8099b8e`, kept unchanged so that the finding numbers stay
stable.

## 0. Re-review of the fixes (HEAD `8e5572a`)

Fix commits: `de5cc68` (F2), `a321011` (F3, F5, F7), `702ab64` (F1, F6),
`3e33e3d` (F4, F8), `8e5572a` (soak criterion). Method: read every
changed line, then probed the claims that can only be checked live. The
probes were throwaway tests in a separate worktree under `/tmp`, run on
the 2×2 test cluster, and were not committed. A copy is kept at
`/tmp/claude-1000/-home-bzed-workspace-vibe-lokihouse/f67d784b-eada-4098-8770-1687c6456c80/scratchpad/zz_review2_test.go`.

**Verdict: not merge-ready yet, but close.** F2, F3, F4, F6, F7 and F8 are
fixed. F1 is fixed for the case it was reported on (independent concurrent
queries), but the reservation is against a budget, not against real
connections, and the deadlock is still reproducible when one request holds
several cursors at once (R1). The F5 fix works for the sample fan-out but
has **no effect** on the single-stream log read it also claims to cover
(R2).

| Finding | Claimed fix | Status |
|---|---|---|
| F1 pool deadlock | all-or-nothing `TryAcquire` from a budget of `max_open_conns/2`, fallback to the global read | **Partial**: see R1 |
| F2 keyset not prunable | `ts > X OR (ts = X AND entry_hash > h)` under `single` (`<` backward) | **Fixed**. The SQL is correct for both directions (matches `orderBy`). The regression test pins `read_rows < 400 000` (496 618 → 308 202) and the predicate shape. Residual R4 |
| F3 window plan × streams | the fan-out reads one window, never consults `HourlyStats` | **Fixed**. `TestSelectSamples_FanoutSingleWindow`/`FanoutQueryShape` pin it; no sort to bound under read-in-order, so memory stays per-block |
| F4 docs | `divergences.md` duplicate rule by stream count; `operations.md` fan-out paragraph | **Fixed**, except one sentence in `operations.md` that R1 falsifies ("a busy pool degrades latency, never correctness") |
| F5 shard routing | `perStreamContext` sets `optimize_skip_unused_shards = 1` for per-stream sample reads and the single-stream log read | **Fixed for samples** (`TestSelectSamples_FanoutRoutesToOwningShard`). **Not effective for logs**: see R2 |
| F6 package state | `Querier.FanoutMax`, `Querier.FanoutConns`; `SetSampleFanoutMax` removed | **Fixed**. Minor comment bug, R3 |
| F7 sample hash | comment in `samplepipe.go` records the difference from Loki | **Fixed** |
| F8 benchmark page | unreleased run under a "Pre-release measurement" heading, prose corrected | **Fixed** |
| F9 buffering | documented in `operations.md` | **Fixed** (documented) |

New dependency check: `golang.org/x/sync` moves from indirect to direct at
v0.24.0, the version Loki's `go.mod` pins (line 135 at `bc1962860a16`), so
AGENTS.md's dependency rule holds. `cespare/xxhash` drops to indirect.

### R1 (high): the fan-out still deadlocks when a request holds several cursors

`FanoutConns` is a counter, not a set of connections. A fan-out that
reserved *n* budget units still takes its *n* connections from the shared
pool, and it blocks in the pool's `acquire` when other users hold them.
The commit argues that this is waiting, not deadlock, because "each running
query holds its connection only while it streams to a consumer that drains
it". That holds for a request with one cursor. It does not hold when the
engine consumes several sample iterators of **one request** in lockstep:
binary operations, and in principle the multi-tenant merge. A global leg
then holds its connection while its consumer waits on the fan-out leg,
which waits for the pool. Probe `TestReviewLockstepBinop`:

- pool `MaxOpenConns = 4`, `dial_timeout = 3 s`, `max_block_size = 500`;
- four 50 000-row streams with ~200-byte lines (`seedPoolStreams`);
- 10 concurrent range queries
  `sum(count_over_time({app=~"s00|s01"}[1ms])) + sum(count_over_time({app=~"s02|s03"}[1ms]))`
  at a 1 ms step through Loki's engine;
- fan-out with `FanoutConns: 2` (half the pool, as production wires it):
  **9/10 failed with `acquire conn timeout` in one of three runs** (0/10
  in the other two);
- the same load with the fan-out disabled (`FanoutMax: 1`, the 0.12.0
  shape): 0/10 in all three runs.

So the fix narrows F1 a lot, but a fan-out can still deadlock the pool,
and the `operations.md` promise of "latency, never correctness" is not
true yet. Category: **derived** (reproduced). Confidence: high that the
cycle exists. Its frequency in production is unknown; it needs pool
saturation plus multi-leg queries.

Fix: make the reservation a reservation of real connections. The
simplest version gives the fan-out its **own** `clickhouse.Open` pool
sized `FanoutConns` (taken out of `max_open_conns`, or added to it and
documented), so a successful `TryAcquire(n)` guarantees *n* connections
that nothing else can take. The alternative routes every ClickHouse cursor
through one pool-wide `semaphore.Weighted(max_open_conns)` (single reads
`Acquire(1)` with the context, the fan-out `TryAcquire(n)`); that is more
invasive, because ingest, tail, ruler and stats share the pool. Add the
probe as a regression test: binary op, small pool, small blocks, 10
concurrent, looped a few times, since the failure is timing-dependent.

### R2 (medium): `optimize_skip_unused_shards` does nothing for the single-stream log read

`SelectLogs` wraps the context in `perStreamContext` when `single`, but it
never sets `rq.streamID`. Its SQL therefore still filters with
`stream_id GLOBAL IN (SELECT id FROM lh_stream_ids)` (external table),
which the shard-pruning analysis cannot evaluate. Probe
`TestReviewLogRouting`, on six single-stream log queries on the cluster,
counted the remote `logs_local` reads per query in
`clusterAllReplicas(…, system.query_log)`:

| stream on the initiator's shard | remote shard queries | `Settings['optimize_skip_unused_shards']` |
|---|---|---|
| yes (s00, s11, s12) | **1** (expected 0 if routed) | `1` |
| no (s01, s06, s07) | 1 | `1` |

The setting arrives, but it has no effect. The commit message of
`a321011` ("the single-stream log read too") overstates it. The CHANGELOG
line is accurate, because it only claims the routing for the sample
fan-out. No test covers the log path. Fix: under `single`, set `rq.streamID =
&r.ids[0]` so that the log read renders `stream_id = ?` like the fan-out
does. That also drops the external-table upload. Then extend
`TestSelectSamples_FanoutRoutesToOwningShard`'s query-log assertion to a
single-stream log query. Category: **official** (documented: the setting
needs a sharding-key condition it can evaluate). Confidence: high
(measured).

### R3 (low): `fanoutSemaphore` comment contradicts the code

`querier.go`: "a zero or negative FanoutConns disables the fan-out (the
reservation can never succeed)". The code does `if n <= 0 { n =
q.fanoutLimit() }`, which *enables* a 16-connection budget, as the field's
own doc ("0 means FanoutMax") and `server.go` ("a zero budget would
default to the fan-out limit") say. Fix the comment and drop the dead
`max(n, 0)`.

### R4 (info): keyset reads still over-read by about 6×

The F2 fix prunes the granules *before* the continuation point, which was
the defect (496 618 → 308 202 rows read for ten 5 000-row chunks). Ten
chunks over 50 000 rows still read about 30 000 rows each, though, so
in-order reading with `LIMIT` reads several granules past the point where
the chunk is full. This is ClickHouse read granularity, not the predicate,
and it is linear in the chunk count, not quadratic. Worth one experiment
(`max_threads = 1` or `max_block_size` = the chunk `LIMIT` on keyset reads,
measured with the same `read_rows` assertion), but it does not block the
merge. Category: **field**; heuristic, since the effect depends on the
part layout and the granule size.

### R5 (info): the soak criterion moved from mean to median (`8e5572a`)

The reason given is sound. The post-GC heap sampled while a fan-out is
in flight includes its ~3 MB-per-stream driver buffers (F9), and a
quarter's mean is dominated by such samples. A steady leak still raises
the median. But this loosens a gate, and the spike it hides was
introduced by this branch's fan-out. The commit says the pre-fix baseline
fails the same way, and the tierb branch passed a 4-minute soak with the
mean. That is acceptable as long as it is recorded. Consider also asserting
the end-of-run in-use heap (the commit quotes 13.2 MiB), so an
idle-floor leak has two independent signals.

### Gate results at `8e5572a`

Same environment as §2: the 2×2 cluster through ch1. These gates prove
the branch is correct at normal load; none of them exercises R1 or R2.

| Gate | Result |
|---|---|
| `LOKIHOUSE_TEST_CH_CLUSTER=lokihouse make check` | PASS: golangci-lint 0 issues, govulncheck clean (allowlisted `GO-2026-5115` only), total coverage **88.5 %** |
| `go test -race -count=1 ./internal/chstore/...` (cluster) | PASS (300 s), including the new pool and fan-out tests |
| oracle `off` / `downstream` / `on` | PASS (104.0 s / 42.4 s / 41.5 s) |
| oracle, cluster | PASS (52.3 s) |
| soak, `LOKIHOUSE_SOAK_DURATION=4m` (new median criterion) | PASS (242.5 s) |

Not run: `make bench`, the full 10-minute soak.

### Recommendations

1. **R1 before merge:** back the reservation with real connections (a
   dedicated fan-out pool is the smallest change), add the lockstep
   binary-op regression test, and keep the `operations.md` "never
   correctness" sentence only once that test passes.
2. **R2:** render `stream_id = ?` on the single-stream log read and test
   the routing; or remove `perStreamContext` from `SelectLogs`, since it
   currently does nothing there.
3. **R3:** fix the comment.
4. R4/R5: optional follow-ups.

---

# First pass (HEAD `8099b8e`)

Range: `b20bb0c` (release 0.12.0) .. `8099b8e`. Code commits:

- `ccf82ba` feat: single-stream log queries read in the part's physical order
- `01893fe` refactor: the sample hash becomes the row's `entry_hash`
- `29051c1` feat: sample queries of ≤ 16 streams fan out, one goroutine and
  one ClickHouse query per stream, merged in Go; no `FINAL` on that path
- `1117235` docs: post-0.12.0 benchmark run in `docs/benchmark.md`
- `8099b8e` docs: commits the forgotten implementation review of
  `tierb-dedup-final-parallel`. That review stays in git history at `8099b8e`;
  this file replaces it.

Skills applied: `clickhouse-best-practices` (rules cited by name) and
`clickhouse-architecture-advisor` (workload summary, provenance labels).
No plan or spec document exists for this branch; the commit messages are
the only statement of intent, so the review checks the code against them.

## 1. Verdict

**Not merge-ready.** The single-stream part-order read is a sound idea, and
read-in-order was verified on the owning shard. But the sample fan-out has
one **blocking** defect. Because the merge needs a row from every stream
before it can emit anything, a query holds some pool connections while it
waits for others. Concurrent fan-out queries then deadlock until the
connection-acquire timeout. This was reproduced on the default pool
(**8 of 12 concurrent queries failed**; the global read failed none). There
are also two medium performance regressions: the single-stream keyset
continuation cannot use the primary key, and each stream re-runs the whole
window plan. The docs are behind the code in three places.

| # | Severity | Finding |
|---|---|---|
| F1 | **high** | Fan-out exhausts the ClickHouse connection pool: circular wait, `acquire conn timeout` |
| F2 | medium | Single-stream keyset continuation `(stream_id, ts, entry_hash) > (…)` is not used by the primary key, so every chunk rereads from the window start |
| F3 | medium | Fan-out runs the global window plan once per stream: queries × 16, mostly empty |
| F4 | medium | `docs/divergences.md` duplicate rule is now wrong for ≤ 16 streams; `docs/operations.md` does not mention the per-query connection, goroutine and memory multiplier |
| F5 | low | On a cluster, every per-stream query goes to every shard; `optimize_skip_unused_shards` would route it to one |
| F6 | low | `sampleFanoutMax` is new package-level mutable state (AGENTS.md *Structure*) |
| F7 | low | The sample hash no longer follows Loki's definition (labels + line); not observable today |
| F8 | info | `docs/benchmark.md` now records an unreleased run; the README summary still shows 0.12.0 numbers |
| F9 | info | Per-query driver buffering grows 16× on the fan-out path |

## 2. Verification results (executed in this review)

All AGENTS.md gates for a query/SQL change pass. Every gate ran against the
local 2×2 cluster (`LOKIHOUSE_TEST_CH_ADDR=localhost:19001`). The cluster
oracle also set `LOKIHOUSE_TEST_CH_CLUSTER=lokihouse`; the three
`oracle-all` modes ran against ch1 without it. No dedicated single server
on `localhost:9000` is published here, so ch1 stood in for it. The gates
prove that the branch is correct on small data; they do not cover F1–F3,
which only appear under load, on large streams or on paged single-stream
queries.

| Gate | Command | Result |
|---|---|---|
| fmt, vet, lint, vuln, coverage | `LOKIHOUSE_TEST_CH_CLUSTER=lokihouse make check` | PASS: golangci-lint 0 issues; govulncheck clean (only the existing allowlisted `GO-2026-5115`); all packages pass; total coverage **88.4 %** (min 85 %) |
| race, chstore, cluster | `go test -race -count=1 ./internal/chstore/...` | PASS (309 s) |
| oracle, pushdown off | `LOKIHOUSE_ORACLE_PUSHDOWN=off go test -tags oracle ./test/oracle/...` | PASS (49.5 s) |
| oracle, downstream | `LOKIHOUSE_ORACLE_PUSHDOWN=downstream …` | PASS (47.3 s) |
| oracle, on | `go test -tags oracle ./test/oracle/...` | PASS (106.9 s) |
| oracle, cluster | `LOKIHOUSE_TEST_CH_CLUSTER=lokihouse go test -tags oracle ./test/oracle/...` | PASS (58.8 s) |

Not run: `make bench` (60 min), `make soak`, the mkdocs build.

Live ClickHouse checks (cluster `lokihouse`, 2 shards × 2 replicas,
`clickhouse-server 26.9.14.10`), run from throwaway tests in a separate
worktree under `/tmp`:

| Check | Result |
|---|---|
| `EXPLAIN indexes=1` per-stream query, owning shard, `ORDER BY stream_id ASC, ts ASC, entry_hash ASC` | `Read type: InOrder`, prefix sort = result sort, so no full sort. The claim in `ccf82ba`/`29051c1` holds |
| same, `DESC` | `Read type: InReverseOrder` |
| same, non-owning shard | `ReadNothing` after key analysis. Cheap, but still a remote round trip (F5) |
| initiator plan without / with `optimize_skip_unused_shards = 1` | `Union` of local `Sorting` + `ReadFromRemote`, merged / a single `ReadFromRemote` to the owning shard (F5) |
| keyset predicate `(stream_id, ts, entry_hash) > (s, X, 0)` | PK condition omits it entirely, **9/15 granules**, the same as no predicate (F2) |
| equivalent `ts > X OR (ts = X AND entry_hash > 0)` | PK condition uses it, **3/15 granules** (F2) |
| `optimize_skip_unused_shards` default | `0` |
| `preferred_block_size_bytes` / `max_block_size` defaults | `1000000` / `65409`. With ~300-byte lines a block holds ~3 000 rows, which drives F1 |
| 16 streams × 50 000 rows, `max_open_conns = 4` | fan-out fails after `dial_timeout`: `clickhouse: acquire conn timeout` (F1) |
| 12 concurrent 16-stream queries, default pool (100/20, 5 s), 3 000-row blocks | fan-out: **8/12 failed** `acquire conn timeout`; global read (`SetSampleFanoutMax(0)`): 0/12 (F1) |

The repro test is kept at
`/tmp/claude-1000/-home-bzed-workspace-vibe-lokihouse/f67d784b-eada-4098-8770-1687c6456c80/scratchpad/zz_review_pool_test.go`.
It is not committed.

## 3. Workload summary (architecture-advisor)

- **Workload:** observability, a Loki-compatible log store on ClickHouse.
- **Latency target:** the branch targets the two slowest benchmark queries.
  `log filter` 9 → 8 ms and `unwrap quantile` 72 → 54 ms on the 200 000-line,
  50-stream benchmark (author's numbers; `make bench` not re-run here).
- **Data shape:** unchanged. `logs` is `ReplacingMergeTree ORDER BY (tenant,
  stream_id, ts, entry_hash)`, `PARTITION BY toDate(ts)`, with a text skip
  index on `line`. On a cluster, `Distributed` shards by `cityHash64(tenant,
  stream_id)`, so one stream lives on exactly one shard.
- **Primary query patterns changed:** (a) a log query on one stream reads
  in sort-key order with keyset chunks; (b) a metric query on ≤ 16 streams
  runs one unchunked in-order query per stream and window, concurrently, and
  merges them in Go. Above 16 streams the query keeps the 0.12.0 global
  `FINAL` read.
- **Operational constraints:** a bounded ClickHouse connection pool
  (`clickhouse.max_open_conns`, default 100; `dial_timeout` doubles as the
  pool-acquire timeout, default 5 s). Concurrent dashboards and ruler
  evaluations. Loki compatibility (oracle). AGENTS.md gates.
- **Likely operating bottleneck after this branch:** connections and
  ClickHouse query count, not CPU. That is where F1, F3 and F5 sit.

## 4. Key decisions reviewed

1. **Single-stream log reads in sort-key order (`rowQuery.single`).**
   Sound. Per `schema-pk-filter-on-orderby`, the query fixes the key prefix
   (`tenant = ?`, one `stream_id`) and orders by the rest of the sorting key,
   which is what lets ClickHouse use `optimize_read_in_order` and stop at the
   `LIMIT` (verified: `InOrder`/`InReverseOrder`). Category: **official**
   (read-in-order with a fixed key prefix is documented behaviour). The
   backward tie order inside one timestamp changes from `entry_hash ASC` to
   `DESC`. That is covered by the existing divergence (*Tie order inside one
   stream and timestamp*; the oracle compares such groups as multisets), so
   no new entry is needed. The continuation predicate is the problem (F2).
2. **Dropping `FINAL` on the fan-out path.** Correct in principle. With
   `ORDER BY stream_id, ts, entry_hash` (the full sorting key), equal
   `(ts, entry_hash)` rows are adjacent whatever the part layout, and
   `rowReader`'s group dedup removes them by exact line + metadata
   comparison. That is Loki's rule, and stricter than `FINAL`'s hash rule.
   Per `insert-optimize-avoid-final`, read-side `FINAL` was compliant
   before. Removing it where the client already dedups adjacent rows is
   also compliant, and per `query-index-skipping-indices` it lets the
   text index prune fully on unmerged parts (author's measurement: 8 192
   vs 32 768 rows read). Category: **derived**. The pinned exact-mode
   coverage from the previous review (F1 there) survives as
   `TestSelectSamples_TextIndexAboveFanoutMax` (17 streams, so the `FINAL`
   path, overlapping duplicate part).
3. **One goroutine and one ClickHouse query per stream, merged in Go.** The
   merge itself is correct. `rowKeyLess` matches the global `ORDER BY ts,
   stream_id, entry_hash`. Windows are contiguous, so per-stream order is
   preserved across windows. The linear scan over at most 16 heads is the
   right size (a heap would not pay off). Ownership is clean: `close`
   cancels and waits, `produce`/`Close` in `samplepipe.go` close the
   source on every path, and cancellation and stream errors are surfaced
   (`fill`). The resource model is the problem: F1 (connections), F3
   (window plan), F9 (buffers).
4. **Skip hourly stats below 2 h for the fan-out.** Reasonable: a per-stream
   in-order read has no sort to bound. But the same reasoning applies to
   every range, which is F3.
5. **`entry_hash` as the sample hash.** Saves an xxhash per row. It
   diverges from Loki's definition, but nothing consumes it today (F7).

## 5. Rules checked (clickhouse-best-practices)

| Rule | Status | Note |
|---|---|---|
| `schema-pk-filter-on-orderby` | Compliant for the row ranges. **Violation in spirit for the keyset predicate** (F2) | The window filter uses the ORDER BY prefix. The new continuation is a row-tuple comparison that the primary-key analysis does not decompose, so it filters rows but prunes no granules |
| `query-index-skipping-indices` | Compliant, improved | Without `FINAL` the text index prunes unmerged parts fully on ≤ 16 streams; above that, exact mode stays pinned by `TestSelectSamples_TextIndexAboveFanoutMax` |
| `insert-optimize-avoid-final` | Compliant | `FINAL` only on the > 16-stream metric read (with `do_not_merge_across_partitions_select_final = 1`); no `OPTIMIZE … FINAL` |
| `insert-mutation-avoid-update` / `-delete` | Compliant (no change) | `ReplacingMergeTree` semantics are now applied client-side on the fan-out path, by exact comparison; deletes unchanged |
| `schema-pk-plan-before-creation`, `schema-pk-cardinality-order`, `schema-pk-prioritize-filters` | Compliant (no change) | Schema untouched; the branch makes better use of the existing key |
| `schema-partition-*` | Compliant (no change) | Duplicates share `ts`, hence the day partition and the window |
| `schema-types-*`, `schema-json-when-to-use` | Not applicable | No column changes |
| `query-join-*` | Not applicable | No joins; the fan-out drops the external-table `IN` for `stream_id = ?` |
| `query-mv-incremental` | Compliant (no change) | `stream_stats_hourly_mv` still feeds window planning; the fan-out skips it below 2 h |
| `insert-*` (batch, async, native) | Not applicable | Ingest unchanged |

## 6. Findings

Severity: **high** = blocks merge; **medium** = fix before merge or in a
follow-up before release; **low** = harden or document; **info** = record.

### F1 (high): the fan-out deadlocks on the connection pool

`streamReader.next` (`internal/chstore/fanout.go:178`) needs the head
row of **every** stream before it can emit one. Each stream goroutine
(`read`, `fanout.go:83`) holds a pool connection while its ClickHouse
query streams. clickhouse-go releases a connection only after the server has
sent the last block (`conn_query.go`: `release` runs after `process`), and it
buffers just 2 blocks (`BlockBufferSize`). Then the stream's channel
(capacity 1) and the batch under construction fill, so a stream with more
than ~4 blocks in a window (≈ 12 000 rows of 300-byte lines at
`preferred_block_size_bytes = 1 MB`) **blocks while holding its
connection**. Streams that could not get a connection wait in the pool's
`acquire`. The consumer waits for them, and the holders wait for the
consumer. That is a circular wait, broken only by `dial_timeout` (default
5 s), which fails the query with `clickhouse: acquire conn timeout`.

- One query deadlocks on its own when `max_open_conns < 16` (reproduced
  with 4). The config only requires `max_open_conns > 0`.
- On the default pool (100), 12 concurrent 16-stream metric queries
  (dashboards plus ruler) need 192 connections: **8 of 12 failed**
  (reproduced). The 0.12.0 global read needs one connection per query and
  failed none.
- The failure is load-dependent and invisible in the unit tests, which use
  small streams (one block, connection released early).

Category: **derived** (from documented driver and pool behaviour plus the
reproduction). Confidence: high.

Fix options, in order of preference:

1. Reserve the connections up front, all or nothing. Use a process-wide
   weighted semaphore sized below `max_open_conns` (for example half of it,
   so other paths, such as the log path, tail, ruler and stats, keep
   headroom). Take `n = len(ids)` with `TryAcquire`; if that fails, fall
   back to the global read, which is correct, just slower. Release in
   `streamReader.close`. `golang.org/x/sync/semaphore` is already in the
   module graph (`go.mod` indirect v0.24.0, used by Loki's
   `pkg/querier/queryrange/limits.go`), so this adds no new dependency.
2. Whatever the bound, reject or clamp configurations where
   `max_open_conns` cannot hold one fan-out, or derive the fan-out limit
   from it.
3. Add the repro as a regression test: a small pool (`MaxOpenConns: 4`),
   `max_block_size` lowered in the connection settings, 16 streams of
   50 000 rows, and assert the query succeeds.

### F2 (medium): the single-stream keyset continuation prunes nothing

`keysetReader.continuation` (`internal/chstore/keyset.go:46-52`) renders
`(stream_id, ts, entry_hash) > (?, fromUnixTimestamp64Nano(?, 'UTC'), ?)`.
`EXPLAIN indexes=1` on the owning shard shows that the primary-key
condition **omits the tuple comparison**: 9/15 granules, identical to no
predicate. The global form (`ts > X OR (ts = X AND …)`) is used: 3/15. With
`InOrder` reading and `LIMIT`, chunk *k* therefore reads and discards the
*k − 1* previous chunks again. A single-stream log query whose Go-side
filters drop most rows (for example a non-pushed-down `| json | …` filter on
a sparse match) pages through many chunks, and its read cost becomes
quadratic in the chunk count within a window. Before this branch, the same
query used the prunable global predicate.

Fix: `stream_id` is constant under `single`, so drop it and use the
prunable form:
`AND (ts > fromUnixTimestamp64Nano(?, 'UTC') OR (ts = fromUnixTimestamp64Nano(?, 'UTC') AND entry_hash > ?))`
(`<` for backward). Pin it with a `query_log` `read_rows` test across 3+
chunks. Per `schema-pk-filter-on-orderby`. Category: **derived**
(measured). Confidence: high.

### F3 (medium): each stream runs the whole global window plan

The fan-out plans windows for all streams together (`defaultWindowRows =
250 000` rows over all streams) and then every stream opens **every**
window (`TestSelectSamples_FanoutOpensWindowsPerStream` pins `2 × single`).
On a long range this multiplies ClickHouse queries by the stream count. For
example, 7 days at 1 M rows/h is about 670 windows, so ≈ 10 700 sequential
queries across 16 goroutines (≈ 670 per goroutine). Most of them return
little or nothing for a small stream, and each costs a round trip (on a
cluster, one per shard, F5). The branch's own argument, that a per-stream
in-order read has no sort to bound, applies to any range, not just ≤ 2 h.

Fix: give the fan-out a single window over the whole range, and skip
`HourlyStats` entirely. If memory on very long ranges is a concern, plan
windows per stream from that stream's own hourly counts. Check with `make
soak` and a long-range `query_log` count. Category: **derived**.
Confidence: medium; no long-range measurement was made here.

### F4 (medium): docs behind the code

- `docs/divergences.md:25` still says metric queries remove duplicates with
  `FINAL` and merge different entries that share a hash. With ≤ 16 streams
  they now dedup in Go by exact line and metadata, like Loki. Only > 16
  streams keep the hash rule. AGENTS.md requires the divergence entry to
  match observable behaviour in the same change.
- `docs/operations.md` (`## Limits` → `### Query`) documents the
  `min(GOMAXPROCS, streams)` worker multiplier but not the new one. Each
  fan-out query now adds up to 16 goroutines, **16 pool connections**, and
  on a cluster 16 × shards ClickHouse queries. Operators sizing
  `clickhouse.max_open_conns` and ClickHouse `max_concurrent_queries` need
  this (ties to F1).

### F5 (low): per-stream queries fan out to every shard

`stream_id = ?` together with `tenant = ?` determines the shard
(`cityHash64(tenant, stream_id)`). With the default
`optimize_skip_unused_shards = 0`, each per-stream query still goes to all
shards, where the non-owning ones `ReadNothing`. On the 2-shard test
cluster a 16-stream metric query costs 32 shard queries instead of 16. The
same applies to the single-stream log path. Setting
`optimize_skip_unused_shards = 1` on these queries (through
`clickhouse.WithSettings` on the context, which avoids the keyset `LIMIT`
syntax issue) routes them to the owning shard: verified with `EXPLAIN`, a
single `ReadFromRemote`. Category: **official** (the setting is documented
for this case). Validation: count `is_initial_query = 0` rows in
`clusterAllReplicas(…, system.query_log)` per query.

### F6 (low): new package-level mutable state

`var sampleFanoutMax = 16` (`fanout.go:15`) exists only so tests can tune it.
AGENTS.md allows no package-level mutable state beyond collectors and values
Loki forces. `logChunkSize` is an existing precedent, but `Querier` already
carries test-tunable knobs (`WindowRows`, `Workers`). A `FanoutMax` field
(0 → 16) follows that pattern, and it also removes the global
`Set…`/`Cleanup` dance in four tests.

### F7 (low): the sample hash no longer follows Loki's definition

`01893fe` sets `Sample.Hash = entry_hash` (cityHash64 of line and
structured metadata). Loki's chunk iterators use `hasher.Hash(lblStr,
line)`, the *extracted* sample labels plus the line (`pkg/chunkenc/memchunk.go:1428`,
`unordered.go:364` at `bc1962860a16`). Loki's `mergeSampleIterator`
dedups by this hash within one stream hash and timestamp. The two
definitions differ when a query drops the metadata (`| drop …`): Loki's
hashes collide, while `entry_hash` stays distinct. lokihouse currently
feeds no sample merge that dedups (`internal/multitenant` uses the sort
iterators), so the difference is not observable, and calling the commit
a refactor is defensible. Record the reason in a comment, so a future
path that merges sample iterators does not inherit a silent semantic
difference. Category: **derived**.

### F8 (info): benchmark page

`1117235` rewrites `docs/benchmark.md` with an unreleased run. The page and
the previous plan say it records release runs, and the README summary
(`8690724`) still shows the 0.12.0 numbers, so the two now disagree. The
text also says lokihouse is "faster than Loki on every query except `log
filter`", but `labels` and `series` (0 s vs 2 ms) are behind too. Consider
reverting `1117235` and re-running at the next release, or labelling the
section as a pre-release measurement.

### F9 (info): per-query buffering grows with the fan-out

Each stream holds up to 2 buffered driver blocks plus one being decoded
(≈ 1 MB each at default block settings), 1 batch in its channel, 1 under
construction and 1 at the consumer (3 × 1 024 rows). For 16 streams that
is ≈ 48 MB of driver blocks per query, against ≈ 3 MB for the global read.
It is bounded, but it belongs in the F4 operations note.

### What is good

- The merge, cancellation and close paths are tight and tested
  (`fanout_internal_test.go`: order, stream error, cancel, close stops
  producers), under `goleak` and `-race`.
- Result-equality tests compare the fan-out with the global read across
  window sizes and worker counts (`TestSelectSamples_FanoutSameResult`).
  The query shape is pinned through `query_log` (no `FINAL`, no external
  table, sort-key `ORDER BY`), and so is the above-limit fallback.
- `rowQuery.orderBy` and the commit messages explain the *why*, and the
  CHANGELOG entries are present.

## 7. Recommendations

1. **F1 before merge:** bound the fan-out with an all-or-nothing connection
   reservation and fall back to the global read; add the pool-exhaustion
   regression test.
2. **F2 before merge:** use the prunable `ts`/`entry_hash` continuation for
   `single`, and pin `read_rows` across chunks.
3. **F3:** read the fan-out in one window (or per-stream windows); re-run
   `make soak` on a long range.
4. **F4:** update `docs/divergences.md` (duplicate rule by stream count) and
   `docs/operations.md` (connections, goroutines and memory per fan-out
   query), in the same change as the fixes.
5. **F5/F6/F7:** `optimize_skip_unused_shards = 1` on per-stream queries;
   move `sampleFanoutMax` onto `Querier`; comment the hash choice.
6. Re-run `make bench` after the fixes; the F1 bound may cost some of the
   `unwrap quantile` gain under load, which is the correct trade.

## 8. Bottom line

The direction is right. Reading one stream in sort-key order is the
cheapest access path the `logs` key offers (verified `InOrder` /
`InReverseOrder`), and the client-side dedup makes dropping `FINAL` safe
while improving text-index pruning. But the fan-out turns one pooled
connection per metric query into sixteen, held while waiting on each
other, and under realistic concurrency that fails queries outright (8/12
reproduced on the default pool). Fix F1 and F2 and update the docs before
merging. F3 and F5 are where the remaining ClickHouse cost of this design
sits.
