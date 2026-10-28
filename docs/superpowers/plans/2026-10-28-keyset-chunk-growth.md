# Keyset Chunk Growth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A log query that filters lines in Go needs fewer ClickHouse round trips. Each keyset chunk after the first is 4× the previous one, capped at 10 000 rows.

**Architecture:** `keysetReader` (`internal/chstore/keyset.go`) reads a log window as a chain of `LIMIT n` queries, each continuing from the last row's key. Today every chunk is `max(limit, 1000)` rows. A multi-stream read orders by `ts`, which is not a prefix of the sorting key, so every chunk is a full top-N scan of the window. When the pipeline drops rows in Go (for example `| logfmt | status >= 500`), the query repeats that scan once per 1000 rows. After each full chunk the reader now multiplies its chunk size by 4, capped at `maxLogChunkSize = 10_000`, and never shrinks it. The first chunk is unchanged, so queries whose first chunk satisfies the limit behave exactly as before.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9.

**Spec:** No separate spec file. The analysis comes from the 2026-10-28 session, on the `make bench` data (200 000 lines, 10 streams, 1 h, ClickHouse 26.9.14):
- `system.query_log` showed `logfmt filter` (`{env="prod"} | logfmt | status >= 500`, limit 1000) running a stream lookup plus 4 sequential 1000-row chunks (3.6, 3.8, 5.5 and 5.9 ms, each reading 147–178k rows). That is about 26 ms of a 30 ms request. Loki answers in 23–26 ms.
- On a copy of the data, `LIMIT 1000` took 4 ms and `LIMIT 4000` took 5 ms, both with lazy materialization (on by default). `LIMIT 16000` took 9 ms because it exceeds `query_plan_max_limit_for_lazy_materialization` (10000 on 26.9). With lazy materialization off, `LIMIT 4000` took 8 ms.
- Expected effect: the benchmark query needs 2 chunks (1000 + 4000) instead of 4, saving about 10 ms.

## Global Constraints

- Growth factor 4; cap `maxLogChunkSize = 10_000`. The cap equals ClickHouse 26.9's default `query_plan_max_limit_for_lazy_materialization`; above it ClickHouse reads `line` and `structured_metadata` for every scanned row. Say so in the constant's comment.
- A chunk is never smaller than the one before it. A limit above the cap (`chunkSize(limit) > maxLogChunkSize`) keeps its size, so it does not shrink.
- Results must stay identical for every chunk size: no new divergence, so `docs/divergences.md` is unchanged.
- Only log reads (`SelectLogs` → `keysetReader`) change. Sample reads (`openCursor` without `LIMIT`) are untouched.
- No new settings, flags or config keys.
- `README.md` and `docs/benchmark.md` hold uncommitted edits from another session. Do not stage or rewrite them in Tasks 1–2. Task 3 edits `docs/benchmark.md` only after those edits are committed; if they are still uncommitted, stop and ask.
- Query code changed, so before pushing run `make check`, `make test-cluster`, `make oracle-all` and `make oracle-cluster` (AGENTS.md).
- Tests never call `t.Parallel()` here: `SetLogChunkSize` is package state.

## Review Focus

1. **Continuation across a grown chunk boundary with duplicates and equal timestamps.** Expected: exactly the entries a single big chunk returns, in both directions, single- and multi-stream. `TestKeyset_SameResultForAnyChunkSize` and `TestKeyset_SingleStreamChunks` already run sizes 1, 3, 7 over 40–70 rows, so growth crosses several boundaries. Task 1 Step 4 re-runs them unchanged.
2. **Short-chunk end detection uses the size of the chunk just read, not the grown one.** Expected: a chunk that returns fewer rows than it asked for ends the read; a full chunk does not. Task 1's `TestKeyset_ChunksGrow` pins this: its last chunk is short and no further query follows.
3. **A limit above the cap** (`limit=20000`). Expected: chunks stay at 20 000 and never drop to 10 000. Task 1's `TestNextChunkSize` covers it.
4. **Single-stream reads** (physical order, `LIMIT` stops early) also grow. Expected: still pruned; `TestSelectLogs_SingleStreamKeysetPrunes` keeps its rows-read bound, with the chunk count updated (Task 1 Step 5).
5. **Cancelling mid-read** (client gone after the first chunk). Expected: unchanged, since growth only changes the next `LIMIT`. Existing cancellation tests cover it; nothing new to add.

---

### Task 1: Grow keyset chunks geometrically

**Files:**
- Modify: `internal/chstore/keyset.go`: add `maxLogChunkSize`, `nextChunkSize`, the growth step in `(*keysetReader).next`; update the `keysetReader` doc comment
- Test: `internal/chstore/keyset_internal_test.go` (`TestNextChunkSize`)
- Test: `internal/chstore/querier_test.go` (`TestKeyset_ChunksGrow`; update `TestSelectLogs_SingleStreamKeysetPrunes`)
- Modify: `CHANGELOG.md` (`## [Unreleased]` → `### Changed`)

**Interfaces:**
- Produces: `const maxLogChunkSize = 10_000` and `func nextChunkSize(n int) int` in package `chstore`. It returns `max(n, min(4*n, maxLogChunkSize))`.

- [ ] **Step 1: Write the failing unit test** `TestNextChunkSize` in `keyset_internal_test.go`:

```go
func TestNextChunkSize(t *testing.T) {
	for n, want := range map[int]int{1: 4, 4: 16, 1000: 4000, 2500: 10000, 4000: 10000, 10000: 10000, 20000: 20000} {
		require.Equal(t, want, nextChunkSize(n), "n=%d", n)
	}
}
```

- [ ] **Step 2: Write the failing integration test** `TestKeyset_ChunksGrow` in `querier_test.go`, next to the other `TestKeyset_*` tests:
  - Seed stream `{app="a"}` with 100 entries, `t0 + i*time.Second`, line `fmt.Sprint("l", i)`. Line `l0` is the only one containing `"hit"` (`"hit l0"`), so a BACKWARD read must reach the oldest row.
  - `chstore.SetLogChunkSize(1)`, with `t.Cleanup(func() { chstore.SetLogChunkSize(1000) })`.
  - Take `since := time.Now()`. Run `` `{app="a"} |~ "hit" or "zzz"` `` (an or-chain of regex filters is not pushed down, as in `TestSelectLogs_SingleStreamKeysetPrunes`) over `[t0, t0+time.Hour)`, BACKWARD, limit 1.
  - Assert the result is exactly one entry, `"hit l0"`.
  - Collect `queriesSince(t, conn, db, since, "FROM logs")` and extract each query's trailing `LIMIT (\d+)`. In the order the queries ran, the limits must be `[1, 4, 16, 64, 256]`. The chunks cover 1 + 4 + 16 + 64 = 85 rows; the 256-row chunk returns the last 15 rows, is short, and ends the read. Before the change this read needed 100 one-row chunks plus a final empty one.
  - `queriesSince` (`routing_test.go`) has no `ORDER BY`, so sort the extracted limits before comparing. The expected sequence is strictly increasing, so this loses nothing.

- [ ] **Step 3: Run both tests and confirm they fail**

Run: `go test ./internal/chstore -run 'TestNextChunkSize|TestKeyset_ChunksGrow' -count=1`
Expected: FAIL. `TestNextChunkSize` fails to compile (`undefined: nextChunkSize`). Comment it out briefly if you want to see `TestKeyset_ChunksGrow` fail on its own, with limits `[1 1 1 …]`.

- [ ] **Step 4: Implement `maxLogChunkSize` and `nextChunkSize(n int) int` in `keyset.go`**
  - In `(*keysetReader).next`, after the short-chunk check and the `k.rr.close()` of a full chunk, set `k.chunk = nextChunkSize(k.chunk)`. The short-chunk comparison must still see the size of the chunk just read.
  - Extend the `keysetReader` doc comment by one sentence: chunks after the first grow 4× up to `maxLogChunkSize`, so a read whose rows are mostly dropped in Go needs few round trips.

- [ ] **Step 5: Update `TestSelectLogs_SingleStreamKeysetPrunes`**

The first chunk is `max(5000, 2000) = 5000` rows; later chunks are capped at 10 000. The 50 000-row read now takes chunks of 5 000, 10 000, 10 000, 10 000, 10 000 and 10 000 (the last one returns 5 000). Change:
- the `"entry_hash >"` assertion from 9 to 5, message `"chunks 2-6 continue from the last row"`;
- the comments that say "ten chunks of 5 000 rows" to match.

Keep the `< 400000` rows-read bound: it still separates pruning from re-reading. Measure the new figure, and if it is far below 400 000, tighten the bound and update the comment's numbers to what you measured.

- [ ] **Step 6: Run the chstore tests**

Run: `go test ./internal/chstore -count=1 -race`
Expected: PASS, including `TestKeyset_SameResultForAnyChunkSize`, `TestKeyset_SubSecondTimestamps`, `TestKeyset_SingleStreamChunks`, `TestSelectLogs_NeverReadsFinal` unchanged.

- [ ] **Step 7: Add the changelog line** under `## [Unreleased]` → `### Changed`:

```markdown
- Log queries whose lines are mostly dropped by filters evaluated in lokihouse (for example `| logfmt | status >= 500`) read ClickHouse in growing chunks (4× per chunk, up to 10 000 rows) instead of fixed chunks of the query limit, so they need fewer round trips.
```

- [ ] **Step 8: Run `make check`**

Expected: PASS (lint, vet, govulncheck, race tests, coverage ≥ 85 %).

- [ ] **Step 9: Commit** (stage only these files, not the other session's `README.md` / `docs/benchmark.md`)

```bash
git add internal/chstore/keyset.go internal/chstore/keyset_internal_test.go internal/chstore/querier_test.go CHANGELOG.md
git commit -m "perf: grow keyset chunks 4x per round trip up to 10000 rows"
```

(`perf:` is already used in this repo's history.)

---

### Task 2: Cluster and oracle verification

**Files:** none changed, unless a failure needs a fix in Task 1's files.

**Interfaces:**
- Consumes: Task 1's commit.

- [ ] **Step 1: Run** `make test-cluster`. Expected: PASS. Single-stream routing (`perStreamContext`) and `GLOBAL IN` reads are unaffected.
- [ ] **Step 2: Run** `make oracle-all`. Expected: PASS (answers identical to Loki).
- [ ] **Step 3: Run** `make oracle-cluster`. Expected: PASS.
- [ ] **Step 4:** If any step fails, fix it in Task 1's files and amend or add a `fix:` commit. Do not loosen oracle comparisons.

---

### Task 3: Re-measure and record the benchmark

**Files:**
- Modify: `docs/benchmark.md`: the "Recent data, local disk" and "Object storage (S3 on RustFS)" tables and the `log filter`/`logfmt filter` commentary below them

**Interfaces:**
- Consumes: Task 1's commit; `docs/benchmark.md` with the other session's edits committed (see Global Constraints).

- [ ] **Step 1: Check the working tree.** `git status --short README.md docs/benchmark.md` must be empty. If not, stop and ask the user.
- [ ] **Step 2: Run** `make bench` (no other benchmark on the host at the same time; check with `pgrep -af 'oracle.test.*TestBench'`).
  Expected: `logfmt filter` lokihouse median well below the previous ~29–32 ms (target ≈ 20 ms). `log filter` is unchanged (its single read already fits in one chunk).
- [ ] **Step 3: Confirm the cause** on `localhost:19100` before the bench database is dropped, or from the run's `system.query_log`. A `logfmt filter` request issues one stream lookup and 2 `logs` queries, with `LIMIT 1000` then `LIMIT 4000`.
- [ ] **Step 4: Update `docs/benchmark.md`** with the new tables (date, one-run caveat as in the existing sections). Rewrite the `logfmt filter` sentences to the new ratios, and note in one sentence why: growing chunks, 2 round trips instead of 4.
- [ ] **Step 5: Commit**

```bash
git add docs/benchmark.md
git commit -m "docs: record benchmark with growing keyset chunks"
```
