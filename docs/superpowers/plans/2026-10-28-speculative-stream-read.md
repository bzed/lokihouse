# Speculative Stream Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A log query whose stream set was resolved before starts its `logs` read immediately, overlapping the `streams` lookup instead of waiting for it, with results identical to today.

**Architecture:** `SelectLogs` (`internal/chstore/querier.go`) runs `ResolveStreams` (one `streams` query, ≈2 ms) and only then the first keyset chunk of the `logs` read. A new per-`Querier` LRU (`StreamSetCache`) remembers the resolved `[]Stream` per (tenant, matchers, UTC days of the range). On a cache hit, `SelectLogs` starts `ResolveStreams` in a goroutine and, in parallel, builds the log iterator from the cached set and primes its first read window (the first chunk query runs). It then waits for the fresh set: if its sorted IDs equal the cached ones, the primed iterator is returned; otherwise the primed iterator is closed (cancelling its query), the cache is updated and the iterator is rebuilt from the fresh set. Every answer is validated by the fresh lookup, so the cache key only affects the hit rate, never correctness.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9, `github.com/hashicorp/golang-lru/v2` v2.0.7.

**Spec:** No separate spec file. The analysis comes from the 2026-10-28 session (memory `ch-per-query-floor`), on the `make bench` data (200 000 lines, `{app="app1"} |= "status=500"`, limit 1000, `lhb-ch` local disk):
- lokihouse 7.1 ms p50 against Loki 5.1 ms. `system.query_log`: `streams` lookup 2.06 ms, 0.3 ms gap, `logs` read 3.15 ms; Go ≈1.5 ms.
- ClickHouse's fixed cost per query is ≈0.6 ms (`SELECT 1`) to ≈1.2 ms (small MergeTree select); about 1.45 ms of the `streams` lookup is parse/analysis/plan/pipeline. No setting reduces it.
- Folding the lookup into the `logs` query as `stream_id IN (SELECT …)` took 5.2 ms (no gain) and loses the single-stream read-in-order path.
- `stream_id` is `labels.StableHash(labels)` (`StreamIdentity`, `internal/chstore/stream.go`), so equal ID sets imply equal labels.
- Expected effect: latency becomes max(lookup, first chunk) instead of their sum, ≈−2 ms on this query (≈5 ms, level with Loki).

## Global Constraints

- Only `SelectLogs` speculates. `SelectSamples`, Tier A push-down SQL, `series`, `labels` and tail-specific code are unchanged (tail reaches `SelectLogs` and benefits without changes).
- Results must be identical with and without the cache. The only new observable difference: on a mismatch, the discarded speculative read's rows still count in `totalLinesProcessed`/`totalBytesProcessed`. Add that sentence to the existing "Query stats" bullet in `docs/divergences.md`.
- `StreamSetCache` constants: `streamSetCacheEntries = 256` entries; sets with more than `maxCachedStreams = 1000` streams, or with none, are not cached (`put` removes the key). No config key, flag or setting.
- A nil `*StreamSetCache` disables speculation; `Querier.StreamCache` nil keeps today's path, so existing tests and callers are unaffected. `internal/server` sets it.
- Metric: `lokihouse_query_stream_speculations_total` counter, label `result` ∈ `miss`, `hit`, `mismatch`, registered with `promauto` (package-level collector, allowed by AGENTS.md). Not incremented when the cache is nil.
- The resolve goroutine is always received from before `SelectLogs` returns (no leak; goleak runs in `internal/chstore/main_test.go`).
- `github.com/hashicorp/golang-lru/v2` is already in `go.mod` as indirect at v2.0.7, the version in Loki's module graph; `go mod tidy` makes it direct. No other new dependency.
- Query code changed: before pushing run `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`. Long-lived query path: `make soak` too.
- CHANGELOG line under `## [Unreleased]` in the commit that wires it in (Task 3).

## Review Focus

1. **A stream appears between two identical queries.** Expected: the second query returns the new stream's lines, ordered globally; the single-stream speculative read (`stream_id = ?`, key order) is discarded for the multi-stream read. Task 3 `TestSelectLogs_SpeculationMismatchSeesNewStream`.
2. **The cached set holds streams that no longer exist** (retention, or a stale entry). Expected: answer from the fresh set; key replaced. Task 3 `TestSelectLogs_SpeculationStaleSet` plants a fake set; Task 1 `TestStreamSetCache_PutRemoves` covers removal on empty/over-cap sets.
3. **Many concurrent identical queries share the cache.** Expected: all return the same result, no race under `-race`. Task 3 `TestSelectLogs_SpeculationConcurrent`.
4. **The request is cancelled while both the lookup and the speculative chunk run.** Expected: `SelectLogs` returns an error satisfying `errors.Is(err, context.Canceled)`, both queries stop, no goroutine survives. Task 3 `TestSelectLogs_SpeculationCancelled`.
5. **Ranges over one hour** (windows planned from `HourlyStats`, a query that also uses the cached IDs). Expected: the same entries as without the cache. Task 3 `TestSelectLogs_SpeculationHitSameResult` runs a 1 h and a 3 h range.

---

### Task 1: Stream set cache

**Files:**
- Create: `internal/chstore/streamcache.go`
- Create: `internal/chstore/streamcache_internal_test.go`
- Modify: `go.mod`, `go.sum` (`go mod tidy`)

**Interfaces:**
- Produces:
  - `type StreamSetCache struct` (wraps `*lru.Cache[string, []Stream]`; the lru is thread-safe, say so in the doc comment)
  - `func NewStreamSetCache() *StreamSetCache`
  - `func streamSetKey(tenant string, ms []*labels.Matcher, from, through time.Time) string`: tenant, the matchers' `String()` sorted, and `from.UTC()`/`through.UTC()` truncated to the UTC day (the days `ResolveStreams` filters on), joined with `"\x00"`. Doc comment: a bad key only costs hit rate, the fresh lookup validates every hit.
  - `func (c *StreamSetCache) get(key string) ([]Stream, bool)`: nil receiver → `nil, false`.
  - `func (c *StreamSetCache) put(key string, streams []Stream)`: nil receiver no-op; stores when `1 <= len(streams) <= maxCachedStreams`, otherwise removes the key.

- [ ] **Step 1: Write the failing tests** in `streamcache_internal_test.go` (package `chstore`):
  - `TestStreamSetKey`: with `a := labels.MustNewMatcher(labels.MatchEqual, "app", "a")`, `b := labels.MustNewMatcher(labels.MatchRegexp, "env", "p.*")`, day `d := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)`:
    - `streamSetKey("t", {a, b}, d.Add(time.Hour), d.Add(2*time.Hour)) == streamSetKey("t", {b, a}, d.Add(3*time.Hour), d.Add(5*time.Hour))`
    - differs when the tenant is `"u"`, when `b` is dropped, and when `through` is `d.Add(25*time.Hour)`.
  - `TestStreamSetCache_PutGet`: put 2 streams, `get` returns them and `true`; unknown key → `false`.
  - `TestStreamSetCache_PutRemoves`: after a stored put, `put(key, nil)` and `put(key, make([]Stream, maxCachedStreams+1))` each leave `get(key)` → `false`.
  - `TestStreamSetCache_Nil`: `var c *StreamSetCache; c.put("k", []Stream{{ID: 1}}); _, ok := c.get("k")` → `ok == false`, no panic.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/chstore -run 'TestStreamSet' -v`
Expected: FAIL to compile (`undefined: streamSetKey`, `NewStreamSetCache`).

- [ ] **Step 3: Implement `streamcache.go`** with the interfaces above and constants `streamSetCacheEntries = 256`, `maxCachedStreams = 1000` (comment: worst case ≈256 000 cached streams). `lru.New` only fails for a size ≤ 0, so `NewStreamSetCache` panics on that error (init-time programmer error, allowed by AGENTS.md). Run `go mod tidy`.

- [ ] **Step 4: Run to verify they pass**

Run: `go test -race ./internal/chstore -run 'TestStreamSet' -v`
Expected: PASS. `git diff go.mod` shows only `github.com/hashicorp/golang-lru/v2 v2.0.7` moving to the direct block.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/streamcache.go internal/chstore/streamcache_internal_test.go go.mod go.sum
git commit -m "perf: add a stream set cache for speculative log reads"
```

---

### Task 2: Prime a window reader

**Files:**
- Modify: `internal/chstore/windows.go` (`windowReader`)
- Test: `internal/chstore/windows_internal_test.go`

**Interfaces:**
- Produces: `func (r *windowReader) prime()`: runs `r.next()` once and remembers its result; the following `next()` returns that remembered result without advancing, later calls behave as before. `cur()`/`errOf()` are unaffected (the cursor and `r.err` hold the primed state). Calling `prime` twice, or after `next`, is not supported; say so in the doc comment.

- [ ] **Step 1: Write the failing tests** in `windows_internal_test.go`, using a new test fake `fakeRows struct{ rows []row; i int }` implementing `rowSource` (no fake exists yet), with `windowReader.open` returning one `fakeRows` per window:
  - `TestWindowReader_PrimeSameRows`: two windows with rows `[1,2]` and `[3]` (by `streamID`). A primed reader yields `1,2,3` then `false`, the same as an unprimed one, and `opened == 1` right after `prime()`.
  - `TestWindowReader_PrimeEmpty`: windows with no rows: after `prime()`, `next()` → `false`, `errOf()` → nil.
  - `TestWindowReader_PrimeError`: `open` returns `errBoom` for the first window: after `prime()`, `next()` → `false` and `errors.Is(errOf(), errBoom)`.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/chstore -run 'TestWindowReader_Prime' -v`
Expected: FAIL to compile (`r.prime undefined`).

- [ ] **Step 3: Implement `prime`** with two fields on `windowReader`: `primed bool` (a remembered result is pending) and `primedOK bool`.

- [ ] **Step 4: Run to verify they pass**

Run: `go test -race ./internal/chstore -run 'TestWindowReader|TestSelectLogs' -v`
Expected: PASS (existing window and log tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/windows.go internal/chstore/windows_internal_test.go
git commit -m "perf: let a window reader start its first read early"
```

---

### Task 3: Speculative `SelectLogs`

**Files:**
- Modify: `internal/chstore/querier.go` (`Querier`, `openRows`, `SelectLogs`)
- Create: `internal/chstore/speculate.go` (`speculateLogs`, the metric)
- Modify: `internal/chstore/export_test.go`
- Test: `internal/chstore/querier_test.go` (or a new `speculate_test.go`, package `chstore_test`)
- Modify: `internal/server/server.go:232` (`newAPI`)
- Modify: `docs/operations.md` (metrics table), `docs/divergences.md` (Query stats bullet), `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 1 `NewStreamSetCache`, `streamSetKey`, `get`, `put`; Task 2 `(*windowReader).prime`.
- Produces:
  - `Querier.StreamCache *StreamSetCache`: doc comment: nil disables speculation.
  - `func (q *Querier) resolvedFor(tenantID string, sel syntax.LogSelectorExpr, req interface{ GetStart() time.Time; GetEnd() time.Time }, dir logproto.Direction, streams []Stream) resolved`: the part of today's `openRows` after `ResolveStreams`; `openRows` becomes `ResolveStreams` + `resolvedFor` (SelectSamples unchanged).
  - `func (q *Querier) logIter(ctx context.Context, sel syntax.LogSelectorExpr, p logql.SelectLogParams, pipeline log.Pipeline, tenantID string, streams []Stream, prime bool) (iter.EntryIterator, error)`: today's `SelectLogs` body from `openRows` on, taking the streams; `iter.NoopEntryIterator` for no streams; `prime` calls `wr.prime()` before returning.
  - `func (q *Querier) speculateLogs(ctx context.Context, sel syntax.LogSelectorExpr, p logql.SelectLogParams, pipeline log.Pipeline, tenantID, key string, cached []Stream) (iter.EntryIterator, error)`
  - `streamSpeculations` (`*prometheus.CounterVec`, name `lokihouse_query_stream_speculations_total`, help `Log queries by outcome of the speculative read on a cached stream set.`, label `result`).
  - export_test.go: `func StreamSpeculations(result string) float64` (via `testutil.ToFloat64`), and `func SeedStreamSet(c *StreamSetCache, tenant string, ms []*labels.Matcher, from, through time.Time, streams []Stream)` calling `c.put(streamSetKey(...), streams)`.

`SelectLogs` after the deletes/pipeline setup: tenant → `key` → `q.StreamCache.get(key)`; hit → `speculateLogs`; miss (cache non-nil) → count `miss`, `ResolveStreams`, `put`, `logIter(..., false)`.

`speculateLogs` (the order matters; `ms := sel.Matchers()` is taken before the goroutine starts):

```go
fresh := make(chan resolvedStreams, 1) // {streams []Stream; err error}
go func() { s, err := ResolveStreams(ctx, q.Conn, tenantID, ms, p.Start, p.End); fresh <- resolvedStreams{s, err} }()
it, specErr := q.logIter(ctx, sel, p, pipeline, tenantID, cached, true)
r := <-fresh // always received: the goroutine never outlives the call
switch {
case r.err != nil:              close it (if non-nil); return nil, r.err
case sameIDs(r.streams, cached): count hit; return it, specErr
default:                         close it; count mismatch; q.StreamCache.put(key, r.streams)
                                 return q.logIter(ctx, sel, p, pipeline, tenantID, r.streams, false)
}
```

`sameIDs` compares `sortedIDs` of both (equal IDs mean equal labels: `StreamIdentity`). Closing the speculative iterator cancels its ClickHouse query (`windowReader.close` → `cancel`).

- [ ] **Step 1: Write the failing tests** (ClickHouse via `chtest.New`, tenant `t1`, data seeded with `seed` at `t0`; each test takes metric deltas around its queries; build `q := &chstore.Querier{Conn: conn, StreamCache: chstore.NewStreamSetCache()}` and compare against `ref := &chstore.Querier{Conn: conn}` through `engine`/`run`):
  - `TestSelectLogs_SpeculationHitSameResult`: one stream `{app="a"}`, 5 entries over 3 h. For ranges `[t0, t0+1h)` and `[t0-1h, t0+2h)`, query `{app="a"} |= "x"` BACKWARD limit 100 twice on `q`: both results equal `ref`'s; per range, `miss` +1 then `hit` +1.
  - `TestSelectLogs_SpeculationMismatchSeesNewStream`: seed `{app="a", pod="1"}`, query `{app="a"}` once (caches 1 stream). Seed `{app="a", pod="2"}` with entries interleaved in time. Second query equals `ref` (both streams, global order) and `mismatch` +1; a third query counts `hit` +1.
  - `TestSelectLogs_SpeculationStaleSet`: seed `{app="a"}`; `chstore.SeedStreamSet(q.StreamCache, "t1", matchers of {app="a"}, start, end, []chstore.Stream{{ID: 42, Labels: labels.FromStrings("app", "a")}})`; the query equals `ref` and `mismatch` +1.
  - `TestSelectLogs_SpeculationConcurrent`: warm the cache with one query, then 8 goroutines run the same query; all results equal `ref` (collect with a `sync.WaitGroup`, assert after `Wait`).
  - `TestSelectLogs_SpeculationCancelled`: warm the cache, then call `q.SelectLogs` directly with a cancelled `user.InjectOrgID` context and `logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{Selector: `{app="a"}`, Start, End, Limit: 100, Direction: logproto.BACKWARD, Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="a"}`)}}}`: `require.ErrorIs(t, err, context.Canceled)`; if an iterator is returned, `Next()` is false and `Close()` succeeds. goleak in `TestMain` checks for leaks.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/chstore -run 'TestSelectLogs_Speculation' -v`
Expected: FAIL to compile (`unknown field StreamCache`, `undefined: chstore.StreamSpeculations`).

- [ ] **Step 3: Implement** the interfaces above in `querier.go` and `speculate.go`, and the hooks in `export_test.go`.

- [ ] **Step 4: Run to verify they pass**

Run: `go test -race ./internal/chstore -count=3 -run 'TestSelectLogs|TestSelectSamples' -v`
Expected: PASS three times, no race, no goleak report.

- [ ] **Step 5: Wire it in.** In `newAPI`, `q := &chstore.Querier{Conn: conn, Deletes: dels, StreamCache: chstore.NewStreamSetCache()}`. Add the metric row to the `docs/operations.md` metrics table (`counter`, label `result`, description as the help text plus "`hit`: the cached stream set was still current, the read started without waiting for the stream lookup"). Append to the "Query stats" bullet in `docs/divergences.md`: "When a log query's cached stream set turns out stale, the discarded speculative read is counted too." Under `## [Unreleased]` in `CHANGELOG.md`: "Log queries whose stream set is already known start reading lines while the stream lookup runs, instead of after it (about 2 ms less per query); new metric `lokihouse_query_stream_speculations_total`."

- [ ] **Step 6: Verify**

Run: `make check && make test-cluster && make oracle-all && make oracle-cluster`
Expected: all pass, coverage ≥ 85 %. Then `make soak`: heap and goroutine profiles flat, as in the previous soak.

- [ ] **Step 7: Commit**

```bash
git add internal/chstore internal/server/server.go docs/operations.md docs/divergences.md CHANGELOG.md
git commit -m "perf: overlap the stream lookup with the first log read"
```

---

### Task 4: Benchmark and record

**Files:**
- Modify: `docs/benchmark.md` (Result section, `log filter` bullet), `README.md` (Speed tables and the sentence below them)

- [ ] **Step 1: Run** `make bench` (servers via `test/bench/up.sh`). Expected: the run passes (every answer matches Loki's), and `log filter` for lokihouse is lower than the 8.1 ms recorded at 0.13.0.

- [ ] **Step 2: Confirm the overlap.** On `lhb-ch`, in `system.query_log` for the bench database, the `logs` query of a `log filter` request starts before the `streams` query of the same request finishes (`query_start_time_microseconds` of the `logs` query < `event_time_microseconds` of the `streams` query), and `/metrics` of the bench lokihouse shows `result="hit"` far above `mismatch`.

- [ ] **Step 3: Record.** Replace the result tables in `docs/benchmark.md` with this run (note: from `main` with the speculative stream read; keep the remote S3 columns labelled as the earlier run). Rewrite the `log filter` bullet: the lookup now overlaps the first chunk, give the new ratio. Update the README tables and the "up to 1.6× slower on the plain log filter" sentence to the measured numbers.

- [ ] **Step 4: Commit**

```bash
git add docs/benchmark.md README.md
git commit -m "docs: record benchmark with the speculative stream read"
```
