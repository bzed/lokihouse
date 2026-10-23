# Time-Windowed Tier B Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier B log and sample reads walk the query range in consecutive time windows of bounded row count, so a sample query never sorts more than one window in memory and log-query keyset chunks re-scan only their window.

**Architecture:** `planWindows` cuts `[start, end)` into contiguous half-open windows from the per-hour entry counts in `stream_stats_hourly`. A `windowReader` (a `rowSource`) opens one window at a time, lazily, in query direction: a plain cursor per window for samples, a `keysetReader` per window for logs. Row SQL is rendered per window by `rowQuery.sql`.

**Tech Stack:** Go, clickhouse-go v2, ClickHouse 26.9, Loki `pkg/iter`, `pkg/logql`.

**Spec:** `docs/superpowers/specs/2026-10-23-clickhouse-query-review.md` (item 5)

**Order:** after `2026-10-23-ch-query-path-fixes.md` (uses `withStreamIDs`, `streamIDsIn`, `chtest.LogsRowsRead`).

## Global Constraints

- Follow `AGENTS.md`: `ctx` first on I/O, errors wrapped, every goroutine owned (this plan adds none), no package-level mutable state (window size is a `Querier` field, not a package variable), user-visible changes get a `CHANGELOG.md` line.
- Results must be identical to today's for every window size: same entries, same order, same dedup of exact duplicates.
- Default window target: `defaultWindowRows = 250_000` rows (≈ 85 MiB of sort state at the measured ≈ 340 B per row).
- Ranges of at most one hour use a single window and do not query `stream_stats_hourly`.
- `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` and `make soak` pass (long-lived query path).

## Review Focus

- Exact duplicates (same stream, `ts`, line, metadata) must still collapse to one entry when a window boundary falls next to them; duplicates share `ts`, so a boundary never separates them. Test in Task 2.
- A BACKWARD log query with `limit 1` over seven days must open only the newest window. Test in Task 3.
- Data with no `stream_stats_hourly` rows (stats lag or a pre-view import) must still be read: windows always cover the whole range. Test in Task 1.
- One hour holding more rows than the target is split into equal sub-windows, not read as one. Test in Task 1.
- Query bounds that are not hour-aligned (sub-second start and end) keep today's inclusive-start, exclusive-end semantics. Test in Task 2 (reuse `TestSelectLogs_SubSecondTimeBoundaries`'s data).

---

### Task 1: `planWindows`

**Files:**
- Create: `internal/chstore/windows.go`
- Test: `internal/chstore/windows_internal_test.go` (package `chstore`; the function holds real logic)

**Interfaces:**
- Produces:
  - `type window struct{ lo, hi int64 }` — Unix ns, half-open `[lo, hi)`.
  - `func planWindows(hours []HourStat, from, through int64, target uint64, dir logproto.Direction) []window`
  - `const defaultWindowRows = 250_000`

Algorithm (not determined by the signature):

```
sum Entries per hour (HourStat rows of all streams) for hours overlapping [from, through)
walk hours ascending, acc := 0, lo := from
  if an hour's count > target: close the open window at the hour start (if acc > 0),
     then split [max(hourStart,from), min(hourEnd,through)) into ceil(count/target) equal windows
  else if acc > 0 && acc+count > target: close [lo, hourStart), lo = hourStart, acc = count
  else acc += count
close [lo, through)
drop windows with lo >= hi; reverse for BACKWARD
```

- [ ] **Step 1: Write the failing tests**

`TestPlanWindows`, table-driven, `h = time.Hour.Nanoseconds()`, `from = 0` unless stated:

| case | hours (start→entries) | from, through | target | want (FORWARD) |
|---|---|---|---|---|
| no stats | — | 0, 3h | 10 | `{0,3h}` |
| cut at hours | 0→100, 1h→100, 2h→100 | 0, 3h | 150 | `{0,1h},{1h,2h},{2h,3h}` |
| merge small hours | 0→10, 1h→10, 2h→10 | 0, 3h | 150 | `{0,3h}` |
| split big hour | 0→1000 | 0, 1h | 300 | 4 equal windows of `h/4` |
| unaligned bounds | 0→100, 1h→100 | 30m+1ns, 1h30m | 150 | `{30m+1ns,1h},{1h,1h30m}` |

Plus `TestPlanWindows_Invariants` over random inputs (`testing/quick` or a seeded loop): windows are contiguous (`w[i].hi == w[i+1].lo`), first `lo == from`, last `hi == through`, every `lo < hi`; BACKWARD is the FORWARD list reversed.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run TestPlanWindows -v`
Expected: FAIL to compile, `undefined: planWindows`.

- [ ] **Step 3: Implement `planWindows` in `windows.go`**

- [ ] **Step 4: Run them to verify they pass**

Run: `go test ./internal/chstore -run TestPlanWindows -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/windows.go internal/chstore/windows_internal_test.go
git commit -m "feat: plan Tier B read windows from hourly stream stats"
```

---

### Task 2: `windowReader` for sample queries

**Files:**
- Modify: `internal/chstore/windows.go`, `internal/chstore/querier.go` (`openRows` → returns a `rowQuery`, `SelectSamples`, `Querier`), `internal/chstore/samples.go` (`sampleIter.rr` becomes `rowSource`), `internal/chstore/export_test.go`
- Test: `internal/chstore/querier_test.go`

**Interfaces:**
- Consumes: `planWindows`, `HourlyStats(ctx, conn, tenant, ids, from, through)`, `withStreamIDs`, `streamIDsIn` (Plan A).
- Produces:
  - `Querier.WindowRows uint64` — window target in rows; 0 means `defaultWindowRows`.
  - `type rowQuery struct { tenant string; filter Fragment; dir logproto.Direction }` with `func (q rowQuery) sql(w window) (string, []any)` rendering `SELECT stream_id, ts, line, structured_metadata, entry_hash FROM logs WHERE tenant = ? AND <streamIDsIn> AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts < fromUnixTimestamp64Nano(?, 'UTC') [AND filter] ORDER BY ts <ASC|DESC>, stream_id ASC, entry_hash`.
  - `func (q *Querier) windows(ctx context.Context, tenant string, ids []uint64, from, through time.Time, dir logproto.Direction) ([]window, error)` — one window for ranges ≤ 1 h, else `planWindows(HourlyStats(…), …, q.WindowRows or default, dir)`.
  - `type windowReader struct { ws []window; open func(window) (rowSource, error); opened int; … }` implementing `rowSource`; it opens the next window only when the current one is exhausted and closes each before opening the next.
  - `export_test.go`: `func OpenedSampleWindows(it iter.SampleIterator) int` (type-asserts `*sampleIter` → `*windowReader`).

- [ ] **Step 1: Write the failing tests**

`TestSelectSamples_SameResultForAnyWindowSize`: three hours of data in streams `{app="a"}` and `{app="b"}` (one entry per minute per stream, logfmt lines `level=<info|warn> n=<i>`), plus three exact duplicates of an entry at an hour boundary. For `WindowRows` in `{1, 7, 0}` run `count_over_time({app=~"a|b"}[5m])`, `sum by (level) (rate({app=~"a|b"} | logfmt [5m]))` and `bytes_over_time({app="a"}[1h])` over `[t0, t0+3h)` with step 1 m; all results equal the `WindowRows: 0` result, and the duplicate is counted once.

`TestSelectSamples_ReadsInWindows`: same data, `WindowRows: 60`, call `Querier.SelectSamples` directly for `count_over_time({app=~"a|b"}[5m])`, drain the iterator, assert `chstore.OpenedSampleWindows(it) >= 6`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'TestSelectSamples_(SameResult|ReadsIn)' -v`
Expected: FAIL to compile (`WindowRows`, `OpenedSampleWindows` undefined).

- [ ] **Step 3: Implement `rowQuery`, `Querier.windows`, `windowReader`; `SelectSamples` opens one `openCursor` per window under the `withStreamIDs` context**

- [ ] **Step 4: Run all chstore tests**

Run: `go test ./internal/chstore -v`
Expected: PASS, including `TestSelectSamples_StreamsInTimestampOrder` and `TestSelectSamples_SubSecondEnd`.

- [ ] **Step 5: Commit**

```bash
git add internal/chstore
git commit -m "perf: read sample queries in bounded time windows"
```

---

### Task 3: Log queries: keyset chunks inside windows

**Files:**
- Modify: `internal/chstore/querier.go` (`SelectLogs`), `internal/chstore/keyset.go` (`keysetReader` takes a `rowQuery` and a `window` instead of a pre-rendered `base`), `internal/chstore/export_test.go`
- Test: `internal/chstore/querier_test.go`

**Interfaces:**
- Consumes: `rowQuery.sql(window)`, `Querier.windows`, `windowReader` (Task 2).
- Produces: `export_test.go`: `func OpenedLogWindows(it iter.EntryIterator) int`.
- `keysetReader` inserts its continuation predicate into the SQL rendered for its window (the `" ORDER BY "` split stays).

- [ ] **Step 1: Write the failing tests**

`TestSelectLogs_SameResultForAnyWindowSize`: the data of Task 2's test; for `WindowRows` in `{1, 7, 0}` × `SetLogChunkSize` in `{1, 3, 1000}` × direction in `{FORWARD, BACKWARD}` × limit in `{5, 1000}`, query `{app=~"a|b"} | logfmt | level="warn"` over three hours; every result equals the default-settings result.

`TestSelectLogs_OpensWindowsLazily`: seven days of data, one entry per hour, `WindowRows: 10`; call `Querier.SelectLogs` BACKWARD with `Limit: 1`, take one entry, close; assert `chstore.OpenedLogWindows(it) == 1`.

Existing `TestKeyset_SameResultForAnyChunkSize` and `TestKeyset_SubSecondTimestamps` must keep passing unchanged.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore -run 'TestSelectLogs_(SameResultForAny|OpensWindows)' -v`
Expected: FAIL to compile (`OpenedLogWindows` undefined).

- [ ] **Step 3: Implement: `SelectLogs` builds a `windowReader` whose `open` returns a `keysetReader` for the window**

- [ ] **Step 4: Run tests, then the full gate**

Run: `go test ./internal/chstore -v && make check && make test-cluster && make oracle-all && make oracle-cluster && make soak`
Expected: all pass; soak heap and goroutine profiles flat.

- [ ] **Step 5: Changelog and commit**

`CHANGELOG.md` `### Changed`: "Log and metric queries that run in lokihouse's own engine read ClickHouse in time windows of at most 250 000 rows: metric queries with parsers (`| json`, `| logfmt`) no longer sort the whole range in ClickHouse memory."

```bash
git add internal/chstore CHANGELOG.md
git commit -m "perf: read log queries in time windows with keyset chunks per window"
```
