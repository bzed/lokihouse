# Parallel evaluation for engine-run metric queries: design

Date: 2026-10-25. Status: proposed.

## Intent

Metric queries that the SQL push-down (Tier A) cannot answer run in
lokihouse's copy of Loki's engine (Tier B): `unwrap` aggregations,
`quantile_over_time`, and queries with parsers or label filters that Tier A
does not cover. Each such query runs on one goroutine. On the benchmark,
`unwrap quantile` takes 70 ms in lokihouse against 57 ms in Loki, and the
engine's own aggregation is ≈38 ms of the 70. On longer ranges the gap grows,
because Loki evaluates pieces of the query in parallel and lokihouse does not.

lokihouse is an open-source drop-in for Loki, so the target user is anyone
who points Grafana at it: dashboards over hours to days, error-ratio panels
built from binary operations, `unwrap` latency panels. The goal is to use
the machine's cores for those queries, with the same answers and Loki's
configuration keys.

## What the measurements decided

The idea was first framed as stream sharding: Loki's `ShardMapper` and
`DownstreamEngine`, with each shard evaluated in parallel. The same data set
(200 000 lines, 10 streams, the final hour) was timed against four Loki
configurations on one host. All four ran at once, so only the ratios within
a row matter:

| query | neither | stream sharding only | time splitting only | both |
|---|---:|---:|---:|---:|
| `unwrap quantile` | 115 ms | 116 ms | 102 ms | 99 ms |
| `count_over_time` | 89 ms | 69 ms | 99 ms | 62 ms |
| `rate regex` | 30 ms | 18 ms | 27 ms | 15 ms |
| `topk` | 55 ms | 34 ms | 51 ms | 29 ms |

- Loki does not stream-shard `quantile_over_time` by default
  (`ShardMapper.quantileOverTimeSharding` is false; sharded quantiles are
  sketches, so they are approximate). Its `unwrap quantile` gain comes from
  **splitting by time** (`split_queries_by_interval`, default `1h`).
- Stream sharding pays off on `count_over_time`, `rate` and `topk`. lokihouse
  already answers those in SQL, 2–4× faster than Loki.

Hence this design: **split engine-run metric range queries by time and
evaluate the pieces in parallel**, and **run the independent parts of a
query (its downstream leaves) in parallel**. Stream sharding is a non-goal
(below).

## Goals

1. **Time-split range queries.** A metric range query that runs on Tier B is
   cut into time pieces ("legs"), each evaluated by Tier B on its own
   goroutine. The legs' matrices are merged into one result.
2. **Parallel leaves.** `pushdown`'s downstreamer evaluates its downstream
   queries (the leaves `pushdown.Map` produces, Tier A or Tier B)
   concurrently instead of in a loop. This helps binary operations and
   multi-leaf queries such as `sum(rate(a)) / sum(rate(b))`.
3. **Exact answers.** Every result equals the unsplit, sequential
   evaluation of the same query on the same data: the same series and
   timestamps, and the same values. The merge only moves points and never
   re-aggregates. Values are compared with the oracle's relative tolerance
   (1e-9), not bit for bit: Loki's engine builds each step's vector by
   iterating a Go map (`range_vector.go:203`), so float sums can differ in
   the last bits between two *unsplit* runs of the same query.
4. **Loki's knobs.** Honour Loki's existing per-tenant limits:
   `limits_config.split_queries_by_interval` (default `1h`; `0` disables
   splitting) and `limits_config.max_query_parallelism` (default 32, the
   most legs and leaves a request evaluates at once). lokihouse parses both
   already and ignores them today.
5. **Safe under load.** Parallel pieces must not deadlock the ClickHouse
   connection pool and must not starve ingest, tail or the ruler.

## Non-goals

- **Stream sharding** (`ShardMapper`, shard-aware `SelectSamples`): no
  measured benefit for the queries that reach Tier B, and approximate for
  quantiles. Revisit only with a measured Tier B workload that it helps.
- **Log queries** (stream results with a limit): they read in keyset chunks
  and stop at the limit; splitting would read more, not less.
- **Time-splitting instant queries**, including rule evaluation: Loki splits
  these only for associative aggregations, via a rewrite, and Tier A already
  pushes most instant metric queries down to SQL. Their leaves still run in
  parallel (Goal 2).
- **Multi-tenant requests** (`multi_tenant_queries_enabled`, several
  tenants): no splitting and no parallel leaves. `multitenant.Querier`
  already opens one cursor per tenant per selector at once, which makes a
  piece's connection need unbounded. They run exactly as today. (Our call.)
- **Splitting Tier A (SQL) leaves.** ClickHouse parallelises those itself.
- **Loki's query frontend** (`queryrange.NewMetricTripperware`): it brings
  the index-stats handler, the size limiter, retries and the results cache.
  Its splitter (`metricQuerySplitter`) is unexported, so it cannot be used
  without the whole stack.

## Anti-goals (a failure even if it is faster)

- Any answer that differs from unsplit evaluation. The oracle compares
  lokihouse with an unsplit Loki, so it catches this.
- A request that deadlocks or times out on the connection pool when it
  would have succeeded sequentially. This is how the sample fan-out failed
  (`REVIEW.md` §0000, F1/R1).
- A changed log-query or Tier A path.

## How it works

### Where splitting happens

Tier B is reached in two ways today (`internal/pushdown/engine.go`):

- `Engine.Query` returns `e.TierB.Query(p)` for the whole query: mode `off`,
  log expressions, multi-tenant requests, or no eligible Tier A leaf.
- `downstreamer.Downstream` → `execOne` runs a non-eligible leaf on Tier B.

Both go through one new **splitting Tier B** with the same shape as
`*logql.QueryEngine`: `Query(p logql.Params) logql.Query`. When the query is
a sample expression, a range query (`step > 0`, `start < end`), and the
split produces more than one leg, its `Exec` runs the legs. Otherwise it
returns `TierB.Query(p)` unchanged. Log expressions and instant queries
never split.

### Split points (our call: keep the query's step grid)

Loki's frontend rounds `start` down and `end` up to a multiple of `step`
before it splits (`alignStartEnd`), so split Loki returns different
timestamps than unsplit Loki whenever `start` is not step-aligned. lokihouse
keeps the query's own grid instead: evaluation timestamps are
`start + k·step` for `k = 0 … ⌊(end−start)/step⌋`, exactly as unsplit, and
legs are contiguous runs of those timestamps. This is what makes Goal 3
hold. It is recorded in `docs/divergences.md` as a difference from Loki's
frontend.

Leg boundaries follow Loki's rules, adapted to the original grid:

- **Interval:** `split_queries_by_interval` for the tenant.
- **Never finer than the largest range vector:** if the expression's largest
  `[range]` exceeds the interval, use that range as the interval, as
  `reduceSplitIntervalForRangeVector` does. Offsets do not change the
  interval.
- A leg ends at the last grid timestamp before the next wall-clock multiple
  of the interval. The next leg starts at the following grid timestamp.
  When `step ≥ interval`, every grid timestamp is its own leg.
- One leg in total means no split.

Each leg is the original `logql.Params` with `Start`/`End` overridden. The
engine computes the read range itself (`start − range − offset … end`), so
a leg reads exactly what its timestamps need.

### Merge

The legs return `promql.Matrix` results with disjoint timestamps. Merge by
series (labels): concatenate the points in leg order (they are already
ascending), then sort the matrix the way Loki's engine does (`sort.Sort` on
`promql.Matrix`, `engine.go:530/570/690`). Statistics merge with
`stats.Result.Merge`. `Summary.Splits` is set to the number of legs, as
Loki's split middleware reports it (`stats.Context.AddSplitQueries`).
Warnings are de-duplicated.

**Series limit.** A leg that hits `max_query_series` fails as the unsplit
engine would, and the request fails with that error. If the legs pass but
the merged result has more than `max_query_series` series, a normal request
fails with the engine's series-limit error, built the way the engine builds
it (`engine.go:508-556`). A **Logs Drilldown** request
(`httpreq.IsLogsDrilldownRequest`) does not fail there: the unsplit engine
returns the first `max_query_series` series in step order with a
partial-results warning, which merged legs cannot reproduce. So the split
result is discarded and the query is evaluated once more, unsplit. This is
exact, and it costs a second evaluation only on the overflow path. (Our
call.)

### Concurrency and the connection pool

Two limits, and one rule that keeps them deadlock-free.

A **piece** is one unit of work this design runs on an extra goroutine: a
Tier B leg of a query that split into two or more legs, or a leaf the
downstreamer runs concurrently with a sibling. Work that is not a piece (an
unsplit Tier B query, a query with one leaf, log queries, the ruler's
single-leaf instant queries, multi-tenant requests) runs exactly as today
and reserves nothing.

- **Per request:** at most `max_query_parallelism` pieces of one request run
  at once. A worker pool feeds legs lazily, so a 7-day query at a 1-hour
  step never creates 168 goroutines at once.
- **Per process:** a weighted semaphore bounds the ClickHouse connections
  held by pieces across all requests. It is sized
  `max(1, max_open_conns / 2)`, the same headroom rule the dropped fan-out
  used. A piece's need is its **peak simultaneous connections**: one cursor
  per log selector in its expression, plus one for the short lookups
  (`ResolveStreams`, the window stats, delete requests) that a
  `SelectSamples` call runs while a sibling selector's cursor is open. So a
  Tier B leg of `a / b` needs 3, a single-selector leg 2, and a Tier A leaf 1.
- **Never queue (our call):** a piece takes its reservation with
  `TryAcquire`, all units at once. If that fails, the piece does not wait:
  the coordinator runs it itself, inline and sequentially, which is today's
  behaviour. So under load, parallelism degrades to sequential evaluation
  instead of queueing. The ruler and dashboards never wait behind each other
  for a reservation, and with spare capacity they get parallelism. A piece
  whose need exceeds the whole budget always runs inline.
- **Why there is no cycle:** reserved pieces together hold at most half the
  pool, and each one's peak fits inside its own reservation. Coordinators
  hold no reservation and no connection while they wait for pieces. Inline
  work behaves exactly as today. That includes the 0.12.0 limitation of an
  unsplit multi-cursor query waiting on a full pool, which this design
  neither adds to nor fixes (`REVIEW.md` §000/N4).

`Accumulate` on Loki's accumulators is not safe for concurrent use: results
are handed to it from one goroutine (a mutex, or a results channel), in any
order. `BufferedAccumulator` stores by index, and the sketch accumulators
merge commutatively.

The first error cancels the request's remaining pieces and is returned.
Cancelling the request context cancels every piece. Goroutines are owned by
the request and waited for before `Exec` returns.

### Observability

- The histogram `loki_query_frontend_partitions` (Loki's name, help and
  buckets, `split_by_interval.go:61-75`) observes the number of legs per
  split query. Loki's `SplitByMetrics` keeps its field unexported, so
  lokihouse declares its own collector with the same definition.
- `Summary.Splits` in the response statistics, as above.

## Order

Build this after the delete-requests cache
(`docs/superpowers/plans/2026-10-25-delete-requests-cache.md`). Without
the cache, every leg makes its own `delete_requests` round trip (168 for a
7-day query at 1-hour legs). Benchmark acceptance is measured with the
cache in place.

## Testing and acceptance

- **Exactness property test (Tier B, ClickHouse-backed):** for randomised
  `start` (aligned and unaligned), `end`, `step` (below, equal to and above
  the interval), range vectors (shorter and longer than the interval) and
  `offset`, the split result equals the unsplit result of the same engine on
  the same data, within the oracle's tolerance (`test/oracle/compare.go`,
  relative 1e-9). The cases include binary operations, `topk`,
  `quantile_over_time` and `absent_over_time`.
- **Series limit:** a normal request whose merged result exceeds
  `max_query_series` fails with the engine's error; a Drilldown request
  returns the unsplit partial result and warning.
- **Degradation:** with the budget exhausted, pieces run inline, the
  results are unchanged, and no request waits for a reservation.
- **Oracle:** the existing corpus runs with splitting on (lokihouse's
  default) against unsplit Loki in all three push-down modes and on the
  cluster. Add corpus range queries that cross several interval boundaries
  with an unaligned `start`.
- **Pool safety:** a regression test in the shape of the fan-out's
  `TestSelectSamples_FanoutMultiLegQueriesNeverDeadlockPool` (a small pool,
  concurrent binary-operation range queries that each split into several
  legs) completes without `acquire conn timeout`.
- **Benchmark:** `make bench` gains a data-span setting
  (`LOKIHOUSE_BENCH_SPAN`, default `1h`). Acceptance: `unwrap quantile` at
  the default span is at or below sharded Loki's median, and a `6h` run is
  recorded in `docs/benchmark.md` to show how the gain scales with range.
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`
  and `make soak` pass. The soak covers the long-lived query path.

## Docs and changelog

- `CHANGELOG.md` `Unreleased`: engine-run metric range queries are split by
  `split_queries_by_interval` and evaluated in parallel up to
  `max_query_parallelism`; the leaves of a query run in parallel.
- `docs/divergences.md`: the step-grid decision above.
- `docs/operations.md`: the two limits, the connection budget (half of
  `max_open_conns`), and how to disable splitting (`split_queries_by_interval: 0`).

## Left to the builder

File layout inside `internal/pushdown`, type and function names, the worker
pool mechanics (channel or `errgroup` with a limit), and how the splitting
Tier B gets the limits and the process-wide semaphore (constructor
parameters wired in `internal/server`).
