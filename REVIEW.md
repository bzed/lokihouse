# Review: branch `perf/part-order-stream-fanout` (HEAD `697fdc3`)

Reviewer: Claude Opus 5.5 (second pair of eyes)
Date: 2026-10-25

This file holds four passes, newest first. §000 re-reviews `7311613..697fdc3`
against N1–N4. §00 re-reviews `918a35c..9919600` against R1–R5. §0 is the
re-review of the fix commits `de5cc68..8e5572a` against the first pass's
findings. §1–§8 are the first pass at `8099b8e`. The older passes are kept
unchanged so that the finding numbers stay stable.

## 000. Fourth pass: the N fixes (HEAD `697fdc3`)

Commits: `7311613` (N1, N2, N3: docs), `f573397` (N4), `697fdc3` (N4: a
follow-up after a CI failure). Method: as before. I read every changed
line, then ran the new regression test against both the old and the new
code, on a single node and on the 2×2 cluster, in worktrees under `/tmp`.
The probe is not committed. A copy is at
`/tmp/claude-1000/-home-bzed-workspace-vibe-lokihouse/f67d784b-eada-4098-8770-1687c6456c80/scratchpad/zz_review5_test.go`.

**Verdict: merge-ready.** All the fixes I claimed are verified. Two
optional notes follow (N5, N6).

| Finding | Claimed fix | Status |
|---|---|---|
| N1 package state | `Querier.FanoutMaxSamples` (0 = 2²⁰); `maxFanoutSamples` and `SetMaxFanoutSamples` removed; the over-cap test sets the field | **Fixed**. No reference left. `logChunkSize` stays, which I had only suggested as optional |
| N2 over-cap double read / stats | documented in `operations.md` ("reads them twice … its query statistics count both reads") | **Addressed** by documentation, one of the two options I gave |
| N3 memory figure | `operations.md` and the field doc: "up to about 100 MiB at the cap" | **Fixed** |
| N4 flaky test, doc overclaim | budget 40 (every leg fans out), 20 000 rows per stream, 10 s `DialTimeout`; the CHANGELOG and `operations.md` narrowed to fanned-out legs; the global-read lockstep cycle recorded as a known limitation inherited from 0.12.0 | **Fixed**, verified below |

### N4: verification

| Check | Result |
|---|---|
| The test copied verbatim onto `8e5572a` (the streaming merge it guards against), single node | **red 3/3** (`acquire conn timeout`, ~11 s each), and red under `-race` too (13 s). The commit's "still reproduces" claim holds |
| HEAD, cluster, `-count=10`, while the `internal/api`, `internal/pushdown` and `internal/deletes` tests load the cluster | **green 10/10**, 3.3–4.6 s per run (three rounds) |
| HEAD, cluster, `-race -count=5`, same load | **green 5/5**, 7.0–7.6 s per run, so ≈2.5 s per round against the 10 s timeout |
| Probe `TestReviewAllLegsFanOut`: the test's shape, counting the queries in `query_log` | **40 per-stream queries, 0 `FINAL` queries**, 3/3 runs: every leg fans out, so the 0.12.0 global-read cycle no longer takes part |
| Full `test-cluster` | see the gates below |

The docs are accurate now. `operations.md` says "Fanned-out legs therefore
cannot deadlock the pool", and its known-limitation paragraph names the
lazy global read correctly. The CHANGELOG says "the fanned-out legs of a
request with several legs".

### N5 (low): the regression test has no headroom and does not check that it fanned out

The budget of 40 is exactly the peak demand (10 queries × 2 legs × 2
streams). If the test ever gains a concurrent query, or a leg gains a
stream, some legs fail `TryAcquire`, fall back to the lazy global read,
and the N4 flake returns silently. It would then look like an R1
regression again. Two cheap guards: give the budget headroom (for
example 64), and assert after the rounds that `queryStartsSince(…,
"FROM logs")` contains no `FINAL` query, the way
`TestSelectSamples_FanoutConcurrentPool` already does. That is what the
probe above measured.

### N6 (info): queueing, not deadlock, is the remaining pool risk of a fan-out

`697fdc3` records that CI failed the test at a 5 s `DialTimeout`: forty
10 MB transfers queued on four connections, and on a contended runner a
queued stream waited past the timeout. Raising the test's timeout to 10 s
and shrinking the volume is the right fix for the *test*, because what it
pins is the absence of a cycle. The same mechanism exists in production,
though. Each stream of a fan-out takes its own pool connection, and each
of those acquires has to succeed within `dial_timeout` (5 s by default). A
saturated pool therefore turns a fanned-out query into an acquire timeout
sooner than a one-query read, which needs one acquire. The budget cap at
half the pool bounds this. `operations.md` already tells operators to size
`max_open_conns` for the multiplier, and it could add "and `dial_timeout`
for the queueing". Category: **derived**. Confidence: medium. It only
matters under sustained pool saturation.

### Gate results at `697fdc3`

Single node (`localhost:9000`) and the 2×2 cluster through ch1. All gates
pass.

| Gate | Result |
|---|---|
| `make check` (single node) | PASS: golangci-lint 0 issues, govulncheck clean (allowlisted `GO-2026-5115` only), total coverage **88.1 %** |
| `test-cluster` (`go test -count=1 ./...` on the cluster) | PASS: every package, chstore 383.9 s |
| oracle `off` / `downstream` / `on` | PASS (109.4 s / 104.7 s / 108.7 s) |
| oracle, cluster | PASS (57.4 s) |
| soak, `LOKIHOUSE_SOAK_DURATION=4m` | PASS (242.6 s) |

Not run: `make bench`, the full 10-minute soak.

### Recommendations

1. Nothing blocks the merge.
2. N5: headroom in the test's budget and a no-`FINAL` assertion.
3. N6: optionally one sentence on `dial_timeout` in `operations.md`.
4. Still optional from earlier passes: R4 (keyset over-read), the global-read
   lockstep cycle (a known limitation since 0.12.0), and moving
   `logChunkSize` onto `Querier`.

---

## 00. Third pass: the R fixes (HEAD `9919600`)

Commits: `918a35c` (R2), `ad38085` (R1, R3), `4bba64c` (R5), `9919600`
(a test that only passed because of R2's bug). Method: as in §0. I read
every changed line, then re-ran the earlier probes plus three new ones
against HEAD, in a worktree under `/tmp`, on a single node and on the 2×2
cluster. The probes are not committed. Copies are at
`/tmp/claude-1000/-home-bzed-workspace-vibe-lokihouse/f67d784b-eada-4098-8770-1687c6456c80/scratchpad/zz_review2_test.go`
and `…/scratchpad/zz_review3_test.go`.

**Verdict: merge-ready once one test is fixed.** R1 and R2 are fixed and
verified live. R3 and R5 are fixed. R4 stays an optional follow-up. One
issue does need attention before the merge (N4). The new regression test
for R1 is **flaky on the cluster**: it failed in one of two full
`test-cluster` runs. The cause is not the fan-out. Most of the test's legs
never fan out, and they hit a lockstep cycle in the *global* read, which
0.12.0 already had. Three further small points (N1–N3) do not block the
merge.

| Finding | Claimed fix | Status |
|---|---|---|
| R1 multi-cursor deadlock | `ad38085`: the fan-out *drains* inside `SelectSamples`. Each stream goroutine reads its rows, extracts its samples and appends them to its own slice; the slices are merged in Go and served by a `sliceSampleIter`. A cap of 2²⁰ samples falls back to the global read | **Fixed for fanned-out legs**, see below. The regression test is flaky, and the doc overclaims for legs that fall back (N4) |
| R2 log routing | `918a35c`: under `single`, `rq.streamID = &r.ids[0]`, and the external-table upload is skipped | **Fixed**. Probe `TestReviewLogRouting` on the cluster: streams on the initiator's shard (s00, s11, s12) now cause **0** remote `logs_local` queries (before: 1); streams on other shards (s01, s06, s07) cause 1. `TestSelectLogs_SingleStreamRoutesToOwningShard` pins it (cluster only; it skips on a single node) |
| R3 comment | `ad38085`: the comment now says "zero means the default budget"; `max(n, 0)` removed | **Fixed** |
| R4 keyset over-read | — | Open, info (unchanged) |
| R5 soak criterion | `4bba64c`: a second signal, the post-GC heap after the workload stopped, must be ≤ 1.5× the warm-up floor | **Fixed**, as recommended |

`9919600` is a correct consequence of R2. `TestStreamSet_PrunesByPrimaryKey`
had written straight into the initiator's local table, which only worked
while the single-stream read still went to every shard. It now writes
through the Distributed table and runs `OPTIMIZE … ON CLUSTER`. It passes on
the cluster.

### R1: why the drain fixes it, and what was measured

The circular wait needed a connection to be held while its holder waited
on something else: the merge waiting for a stream's head, or the engine
waiting on a sibling leg. In `drainFanout` a goroutine waits on nothing.
It reads its cursor to the end, as fast as the server sends, and only
then does `SelectSamples` return, so a leg holds connections only for the
duration of its own transfer. A stream still queued in `acquire` waits
only for transfers that will finish on their own. No cycle remains, at any
pool size. The commit's argument against a dedicated pool, which was my
suggestion in §0, is correct: a leg could still hold one pool's
connections while its sibling waits. Draining removes the wait itself.

The streaming merge is gone, but the dedup it fed is not. The per-stream
cursors are still `openCursor` → `rowReader`, which drops exact duplicates
(line plus metadata within one `(ts, stream_id, entry_hash)` group).
`mergeSamples` orders by `(ts, stream_id)` and is stable within a stream.
That is the order the single-query read produces (`ORDER BY ts,
stream_id, entry_hash`), so the result is unchanged. The oracle passes on
it, in all three modes and on the cluster.

Probes at HEAD:

| Probe | Pool / timeout | Load | Result |
|---|---|---|---|
| `TestReviewLockstepBinop` (the §0 reproducer) | 4 conns, 3 s | 10 concurrent binary ops, two 2-stream legs, budget 2 | **0/10** failed (before: 9/10 in one of three runs); global-only: 0/10 |
| `TestReviewSaturation` (new) | 4 conns, **5 s** (production default) | 10 concurrent binary ops, two **8-stream** legs, 16 × 10 MB streams, budget 2 or 16, two rounds each | **0/10** in every round, 1.43–1.48 s per round; global-only 1.43–1.58 s |
| `TestReviewMultiLegStress` (new; the committed test's shape, counting failures) | 4 conns, 5 s | as `TestReviewLockstepBinop`, 6 rounds × 3 runs, while `internal/api` and `internal/pushdown` tests load the cluster | budget 2: **0/180**; global-only (`FanoutMax: 1`): **15/180** failed (`acquire conn timeout`), in 2 of 18 rounds. See N4 |

For fanned-out legs, the `operations.md` sentence "a busy pool degrades
latency, never correctness" is now true. It is not true for a leg that
reads globally; see N4.

The committed regression test,
`TestSelectSamples_FanoutMultiLegQueriesNeverDeadlockPool`, uses a 30 s
`DialTimeout`. That is fine for the drain, but against the old code it
would only have gone red after 30 s per stuck round. A 3–5 s timeout
catches the regression faster with the same signal. This is optional.

### N4 (medium): the R1 regression test is flaky, and the doc overclaims for global legs

`TestSelectSamples_FanoutMultiLegQueriesNeverDeadlockPool` **failed in one
of two full `test-cluster` runs** at `9919600`. The failing round reported
`fanout_test.go:561: Received unexpected error` after 35.7 s, which is the
test's 30 s acquire timeout plus the run. Run alone it passes 3/3, and the
second full run passed.

The cause is in the test's setup, not in the drain. The comment says
"every binary operation runs two 2-stream fan-outs in lockstep", but with
`FanoutConns: 2` the whole budget fits **one** 2-stream leg. Of the 20
legs in flight per round, at most one fans out at a time. The others fail
`TryAcquire` and take the global read, which is lazy: it opens its cursor
on the first `Next` and keeps its connection while the server is
mid-stream. The engine steps both legs of a binary operation in turn. So
with 10 queries × 2 global legs on a 4-connection pool, the first legs of
four queries can hold all four connections while each query's second leg
waits in `acquire`. That is the same circular wait as R1, on the 0.12.0
code path. `TestReviewMultiLegStress` confirms it: global-only failed
15/180 queries under load; the fan-out configuration failed 0/180. (The
§0 probe's global-only 0/30 was a quieter run.)

Consequences:

- The test is red/green on timing that has nothing to do with the code it
  pins. On CI it will flake, and when it does it reads as an R1
  regression.
- `operations.md` says "Requests with several legs therefore cannot
  deadlock the pool, even when the pool is smaller than one fan-out", and
  the CHANGELOG says "requests with several legs … cannot deadlock the
  connection pool". That holds only for legs that fan out. A leg above 16
  streams, a leg whose reservation fails, or a leg over the sample cap
  still can. This is **pre-existing** (0.12.0 has it), not a regression of
  this branch. The branch actually shrinks it, because fanned-out legs no
  longer take part.

Fix, before merge: make the test exercise what it claims. Give it a budget
that lets every leg fan out (for example `FanoutConns: 40` on the
4-connection pool; the drain makes a budget above the pool safe, as
`TestSelectSamples_FanoutBudgetAbovePoolSucceeds` shows), and use a 5 s
`DialTimeout`, so that a real regression fails fast. Then narrow the two
doc sentences to "fanned-out legs", and record the global-read lockstep
cycle as a known limitation. A later fix could drain the global read too,
below a size bound, or give the engine's legs one shared cursor budget.
Category: **derived** (reproduced). Confidence: high.

### N1 (low): `maxFanoutSamples` is package-level mutable state

`fanout.go` adds `var maxFanoutSamples = int64(1 << 20)`, which tests
overwrite through `SetMaxFanoutSamples`. It copies the existing
`logChunkSize` pattern word for word ("package state only so tests can
shrink it"), so it is not out of character. But AGENTS.md allows no
package-level mutable state except collectors and Loki-forced values, and
F6 moved the sibling knobs onto `Querier` for exactly that reason. No
chstore test calls `t.Parallel()`, so there is no race today. Fix: a
`Querier.FanoutMaxSamples` field (0 = 2²⁰), like `FanoutMax`. That also
removes the `t.Cleanup` reset in the tests.

### N2 (info): an over-cap fan-out reads twice and counts both reads in its stats

When the drain passes 2²⁰ samples it cancels, discards everything and
re-reads all streams with the global `FINAL` query. In `TestReviewMemory`
(16 streams, 800 000 samples, cap lowered to 400 000), the fallback took
**332–335 ms**, against 232 ms for the global read alone and 184–191 ms
for the fan-out under the cap. The discarded drain's ClickHouse progress
is still added to the query's `decompressed lines/bytes` statistics
(`WithQueryStats`), so such a query over-reports what it read. Both are
bounded (one wasted read of at most ~1 M samples) and documented ("a query
over the cap reads all streams in one query"). The stats double count
could be noted in `operations.md`, or avoided by giving the drain its own
`stats.Context` and merging it only on success.

### N3 (info): the memory figure for the drain counts one copy

`fanout.go` and `operations.md` give the cap as about 48 MiB (2²⁰ ×
48-byte `drainedSample`). At the cap the per-stream slices and the merged
slice coexist, so that is 2×, before the `append` growth slack. Each
sample also carries its own `s.Labels.String()` allocation until the
interning pass, which runs only after the merge. Measured, this does not
matter in practice. At 800 000 samples the fan-out's peak heap delta was
**196–208 MiB**, against **192–217 MiB** for the global read, because row
decoding (≈200-byte lines) dominates both. Interning inside each stream
goroutine (one map per stream) would remove the per-sample strings. The
doc could say "up to ~100 MiB at the cap".

### Gate results at `9919600`

Single node (`localhost:9000`, ClickHouse 26.9.25.15) and the 2×2 cluster
through ch1.

| Gate | Result |
|---|---|
| `make check` (single node) | PASS: golangci-lint 0 issues, govulncheck clean (allowlisted `GO-2026-5115` only), total coverage **88.1 %** (88.5 % at `8e5572a`) |
| `test-cluster` (`go test -count=1 ./...` on the cluster), run 1 | **FAIL**: `TestSelectSamples_FanoutMultiLegQueriesNeverDeadlockPool` (N4); every other package ok |
| `test-cluster`, run 2 | PASS (chstore 383 s) |
| oracle `off` / `downstream` / `on` | PASS (138.6 s / 103.8 s / 46.6 s) |
| oracle, cluster | PASS (57.7 s) |
| soak, `LOKIHOUSE_SOAK_DURATION=4m` (median plus the new end-heap check) | PASS (242.7 s) |

Not run: `make bench`, the full 10-minute soak. (My own first cluster pass
broke on a shell-quoting mistake in the gate script and was discarded; the
runs above are the clean re-runs.)

### Recommendations

1. **N4 before merge:** let every leg of the regression test fan out
   (budget above the pool), shorten its `DialTimeout`, and narrow the
   "several legs cannot deadlock" sentences in `operations.md` and
   `CHANGELOG.md` to fanned-out legs. Record the global-read lockstep
   cycle (pre-existing since 0.12.0) as a known limitation.
2. N1: move the sample cap onto `Querier` (and, while at it,
   `logChunkSize`).
3. N2/N3: optional. Separate stats for the drain, per-stream label
   interning, a more accurate memory figure in `operations.md`.
4. R4: optional, as before.

---

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
