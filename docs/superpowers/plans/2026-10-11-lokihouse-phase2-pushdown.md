# Lokihouse Phase 2 (Push-down) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the common Grafana query shapes cheap: stream Tier-B rows instead of materialising them, read log queries in keyset chunks, derive SQL pre-filters from regex and pattern line filters, and compute `count_over_time` / `rate` / `bytes_over_time` / `bytes_rate` leaves entirely in ClickHouse ("Tier A"). Results stay identical to Loki: the existing oracle stays green and Tier A is property-tested against Tier B.

**Architecture:** Tier A hooks into Loki's own *downstream* machinery, the mechanism Loki uses for query sharding. `internal/pushdown` maps a sample expression by wrapping every range-aggregation leaf in `logql.DownstreamSampleExpr`. It then runs the mapped expression on `logql.DownstreamEngine`. Loki's evaluator does everything above the leaves (vector aggregations, binary ops, `label_replace`, `topk`, …), so that logic stays Loki's code. A `pushdown.Downstreamer` executes each leaf:
- **Tier A:** a whitelisted leaf becomes one ClickHouse `GROUP BY` over sub-buckets, and Go slides the window over those sub-buckets with Loki's exact arithmetic.
- **Tier B:** any other leaf runs on the unchanged Phase 1 engine.

Queries without an eligible leaf skip the mapping and run on the Phase 1 engine untouched.

**Tech Stack:** Go 1.27, `github.com/grafana/loki/v3` @ `fbc3f0770b8c` (unchanged), `clickhouse-go/v2` v2.49.0, ClickHouse 26.9, Go `regexp/syntax` for regex analysis.

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md` (§4 Tier A/B, §6 feature mapping, §7 correctness strategy, §7.2 regex)

## Global Constraints

All Phase 1 Global Constraints still apply (`docs/superpowers/plans/2026-10-10-lokihouse-phase1-foundation.md`). In addition:

- Never bump the Loki commit inside a Phase 2 task.
- SQL used as a **pre-filter** (Tier B) only needs to be a *necessary* condition: every row Loki would keep must pass it. SQL used as an **exact filter** (Tier A) must accept exactly the rows Loki's filter accepts. Each function that emits SQL documents which of the two it guarantees, in its doc comment.
- Tier A reads `FROM logs FINAL`. Since the Phase 1 review, `entry_hash = cityHash64(line, mapSort(structured_metadata))` is part of the sorting key, so `FINAL` deduplicates exactly on Loki's identity (stream, ts, line, metadata). Tier B keeps its read-time dedup in `rowReader`.
- Loki window semantics (from `pkg/logql/range_vector.go` and `evaluator.go` at the pinned commit), all in nanoseconds:
  - Steps: `t = start + k·step` for `k = 0…`, while `t ≤ end`.
  - Window: `(t − offset − range, t − offset]`. The left bound is exclusive and the right bound inclusive.
  - Output timestamp in ms: `t_shifted/1e6 + offset/1e6`, where `t_shifted = t − offset` and both divisions are integer and truncating.
- Values:
  - `count_over_time` = n.
  - `rate` = `float64(n) / range.Seconds()`.
  - `bytes_over_time` = Σ `len(line)` in bytes.
  - `bytes_rate` = `float64(bytes) / range.Seconds()`.

  An empty window emits no point.
- Series labels for a Tier A leaf come from Loki's own extractor. Build `count_over_time(<selector without pipeline>[range])`, take `Extractor().ForStream(streamLabels).ProcessString(0, "", structuredMetadata)`, and print the labels with `.String()`. No label-merging logic is re-implemented.
- Config gains `query.pushdown: on | off | downstream` (default `on`). `downstream` maps every query but runs all leaves on Tier B; it exists so the oracle can prove the mapping is semantics-preserving.
- Every task's tests must pass on the single node and on the 2×2 podman cluster (`make test`, `make test-cluster`). Tasks 5–8 also need `make oracle` and `make oracle-cluster`.

## Review Focus

1. **Entries exactly on a window boundary** (`ts == t − range` is excluded, `ts == t` included, also with `offset`): Tier A must agree with Loki. Owner: Task 6 (`TestTierA_WindowBoundaries`).
2. **Unmerged duplicate parts** (the same push acknowledged twice, before ClickHouse merges): Tier A must count the entry once. Owner: Task 6 (`TestTierA_CountsDuplicatesOnce`).
3. **`start`/`end` not aligned to the step or to a millisecond, and instant queries (`step = 0`):** the timestamps and points must match Loki. Owner: Task 7 (property generator draws ns-offset starts and instant queries).
4. **Regex line filters on non-ASCII or invalid-UTF-8 lines:** the exact-filter gate must reject every pattern whose ClickHouse `match()` result could differ from Go's. Owner: Task 4 (`TestRegexGate_FuzzAgainstClickHouse`).
5. **Series explosion in a Tier A leaf** (e.g. 200k streams): the leaf must fail with Loki's max-series error rather than exhaust memory. Owner: Task 6 (`TestTierA_MaxSeries`).

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/chstore/samples.go` (new) | Streaming `iter.SampleIterator` over the row cursor (Task 1) |
| `internal/chstore/querier.go` (modify) | `SelectSamples` uses `samples.go`; `SelectLogs` uses the keyset reader (Tasks 1, 2) |
| `internal/chstore/keyset.go` (new) | Chunked keyset reader for log queries (Task 2) |
| `internal/chstore/sqlbuild.go` (modify) | Necessary pre-filters for regex/pattern filters (Task 3); `ExactLineFilterSQL` (Task 6) |
| `internal/chstore/regexgate.go` (new) | `RegexExact(re) bool`: proves Go ≡ ClickHouse `match()` for a pattern (Task 4) |
| `internal/chstore/buckets.go` (new) | `BucketCounts`: the Tier A `GROUP BY` query (Task 6) |
| `internal/pushdown/mapper.go` (new) | Wrap range-aggregation leaves in `DownstreamSampleExpr` (Task 5) |
| `internal/pushdown/engine.go` (new) | `Engine.Query(ctx, params)`: choose plain vs downstream engine; `Downstreamer` (Task 5) |
| `internal/pushdown/tiera.go` (new) | Tier A leaf: eligibility, SQL, sliding window, result building (Task 6) |
| `internal/pushdown/property_test.go` (new) | Tier A == Tier B on random data × random queries (Task 7) |
| `internal/api/query.go`, `internal/config/config.go`, `cmd/lokihouse/main.go` (modify) | Wire `query.pushdown` (Task 5) |
| `test/oracle/*` (modify) | Run the corpus in each pushdown mode; push-down corpus; tier coverage (Task 8) |
| `internal/pushdown/bench_test.go`, `docs/phase2-status.md` (new) | Benchmark and status (Task 9) |

---

### Task 1: Stream Tier-B samples instead of materialising them

Today `SelectSamples` appends every sample of the query into a `map[string]*logproto.Series` before returning, so memory grows with the number of matching lines. Rows already arrive ordered by `ts`, which is all Loki's range-vector iterators need.

**Files:**
- Create: `internal/chstore/samples.go`
- Modify: `internal/chstore/querier.go` (`SelectSamples`)
- Test: `internal/chstore/querier_test.go`

**Interfaces:**
- Consumes: `openRows` / `rowReader` (Phase 1; `openCursor` from the Phase 1 review).
- Produces: `type sampleIter struct` implementing `iter.SampleIterator` (`Next() bool`, `At() logproto.Sample`, `Labels() string`, `StreamHash() uint64`, `Err() error`, `Close() error`).

- [ ] **Step 1: Write the failing test.** It interleaves two streams with equal timestamps and checks the series and the order. Add it to `querier_test.go`:

```go
func TestSelectSamples_StreamsInTimestampOrder(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(2 * time.Second), Line: "3"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(time.Second), Line: "2"}}},
	)
	q := &chstore.Querier{Conn: conn}
	expr, err := syntax.ParseSampleExpr(`count_over_time({app=~"a|b"}[1m])`)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	it, err := q.SelectSamples(ctx, logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: t0.Add(-time.Minute), End: t0.Add(time.Minute), Plan: &plan.QueryPlan{AST: expr},
	}})
	require.NoError(t, err)
	defer it.Close()
	var got []string
	var last int64
	for it.Next() {
		s := it.At()
		require.GreaterOrEqual(t, s.Timestamp, last, "timestamps never go backwards")
		last = s.Timestamp
		got = append(got, it.Labels())
	}
	require.NoError(t, it.Err())
	require.Equal(t, []string{`{app="a"}`, `{app="b"}`, `{app="b"}`, `{app="a"}`}, got)
}
```

(`plan` is `github.com/grafana/loki/v3/pkg/querier/plan`; reuse it if `querier_test.go` already imports it.)

- [ ] **Step 2: Run it.** `go test -count=1 -run TestSelectSamples_StreamsInTimestampOrder ./internal/chstore/`. Expected: FAIL, because the current code groups by series (`a, a, b, b` order).

- [ ] **Step 3: Implement `samples.go`.**

```go
package chstore

import (
	"github.com/cespare/xxhash/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
)

// sampleIter turns the ts-ordered row cursor into samples one row at a time.
// Loki's range-vector iterators only need samples in timestamp order; they group by Labels().
type sampleIter struct {
	rr        *rowReader
	byID      map[uint64]Stream
	extractor log.SampleExtractor
	ex        map[uint64]log.StreamSampleExtractor
	cur       logproto.Sample
	lbls      string
	hash      uint64
}

func (it *sampleIter) Next() bool {
	for it.rr.next() {
		r := it.rr.cur
		se, ok := it.ex[r.streamID]
		if !ok {
			se = it.extractor.ForStream(it.byID[r.streamID].Labels)
			it.ex[r.streamID] = se
		}
		s, ok := se.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !ok {
			continue
		}
		it.lbls = s.Labels.String()
		it.hash = r.streamID
		it.cur = logproto.Sample{Timestamp: r.ts.UnixNano(), Value: s.Value, Hash: xxhash.Sum64String(it.lbls + r.line)}
		return true
	}
	return false
}

func (it *sampleIter) At() logproto.Sample { return it.cur }
func (it *sampleIter) Labels() string      { return it.lbls }
func (it *sampleIter) StreamHash() uint64  { return it.hash }
func (it *sampleIter) Err() error          { return it.rr.err }
func (it *sampleIter) Close() error        { return it.rr.close() }
```

In `querier.go`, replace everything in `SelectSamples` after `openRows` with:

```go
	return &sampleIter{rr: rr, byID: byID, extractor: extractor, ex: map[uint64]log.StreamSampleExtractor{}}, nil
```

Delete the `xxhash` import from `querier.go` if it is now unused.

- [ ] **Step 4: Run the package tests and the oracle.** `go test -count=1 ./internal/chstore/ && make oracle`. Expected: PASS. If the oracle fails, the engine needed a guarantee the old iterator gave (e.g. ordering within equal timestamps). Diagnose with superpowers:systematic-debugging before changing anything.

- [ ] **Step 5: Commit.** `git commit -am "perf(chstore): stream Tier-B samples instead of materialising every series"`

---

### Task 2: Keyset-chunked log reads

A log query with `limit=100` over 30 days currently asks ClickHouse to sort the whole range. Cancel-on-close stops the transfer, but not the server-side full sort. Read in chunks with `LIMIT`, continuing from the last row's key, so ClickHouse can use top-N sorting.

**Files:**
- Create: `internal/chstore/keyset.go`
- Modify: `internal/chstore/querier.go` (`openRows` returns the chunked reader for `SelectLogs`)
- Test: `internal/chstore/keyset_test.go`

**Interfaces:**
- Consumes: `openCursor(ctx, conn, sql, args...) (*rowReader, error)`.
- Produces: `type keysetReader struct` with `next() bool`, `cur row`, `err error`, `close() error`; it has the same shape as `rowReader`, so `entryIter` takes an interface `rowSource { next() bool; current() row; error() error; close() error }`. `row` gains `entryHash uint64`.

Ordering and continuation (ClickHouse row-value comparison):
- FORWARD: `ORDER BY ts ASC, stream_id ASC, entry_hash ASC`; next chunk `AND (ts, stream_id, entry_hash) > (?, ?, ?)`.
- BACKWARD: `ORDER BY ts DESC, stream_id ASC, entry_hash ASC`; next chunk `AND (ts < ? OR (ts = ? AND (stream_id, entry_hash) > (?, ?)))`.

Rows with an identical key are unmerged exact duplicates, so the strict comparison skipping them at a chunk boundary is the desired dedup. Chunk size is `max(limit, 1000)`. A chunk shorter than the chunk size ends the read.

- [ ] **Step 1: Write the failing test.** Equal-timestamp runs must straddle chunk boundaries, and the result must not depend on the chunk size. Expose the chunk size to tests with `internal/chstore/export_test.go`:

```go
package chstore

// SetLogChunkSize overrides the minimum chunk size (default 1000) for tests.
func SetLogChunkSize(n int) { logChunkSize = n }
```

The production rule lives in one function: `func chunkSize(limit uint32) int { return max(int(limit), logChunkSize) }`, with `var logChunkSize = 1000`. The test uses `limit` 1, so chunk sizes 1, 3 and 7 really apply. Add to `querier_test.go`:

```go
func TestKeyset_SameResultForAnyChunkSize(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	var es []logproto.Entry
	for i := range 50 { // 5 timestamps × 10 lines each
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i/10) * time.Second), Line: fmt.Sprint("l", i)})
	}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es}, logproto.Stream{Labels: `{app="b"}`, Entries: es[:20]})
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es[13:14]}) // exact duplicate in a second part
	ng := engine(t, &chstore.Querier{Conn: conn})
	t.Cleanup(func() { chstore.SetLogChunkSize(1000) })
	for _, dir := range []logproto.Direction{logproto.FORWARD, logproto.BACKWARD} {
		for _, limit := range []uint32{1, 25, 1000} {
			chstore.SetLogChunkSize(1000)
			want := run(t, ng, `{app=~"a|b"}`, t0.Add(-time.Minute), t0.Add(time.Minute), 0, dir, limit)
			for _, n := range []int{1, 3, 7} {
				chstore.SetLogChunkSize(n)
				got := run(t, ng, `{app=~"a|b"}`, t0.Add(-time.Minute), t0.Add(time.Minute), 0, dir, limit)
				require.Equal(t, want.Data, got.Data, "dir=%v limit=%d chunk=%d", dir, limit, n)
			}
			if limit == 1000 {
				total := 0
				for _, s := range want.Data.(logqlmodel.Streams) {
					total += len(s.Entries)
				}
				require.Equal(t, 70, total, "duplicate counted once")
			}
		}
	}
}
```

- [ ] **Step 2: Run it.** `go test -count=1 -run TestKeyset ./internal/chstore/`. Expected: FAIL to compile (`SetLogChunkSize` undefined).

- [ ] **Step 3: Implement.**
  1. Add `entry_hash` to the `SELECT` and `Scan` in `rowReader`.
  2. In `keyset.go`, build the base `WHERE` once in `openRows`, then implement `keysetReader.next()`: read from the current `rowReader`; at the end of a chunk, if it returned `chunk` rows, open the next one with the continuation predicate and the last row's `(ts, stream_id, entry_hash)` as arguments.
  3. Keep `rowReader`'s read-time dedup; it still drops equal `(ts, stream, line, metadata)` within a chunk.
  4. `SelectSamples` keeps the single unchunked cursor, because metric queries read everything anyway.

- [ ] **Step 4: Run.** `go test -count=1 ./internal/chstore/ && make oracle`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "perf(chstore): keyset-chunked log reads so ClickHouse can top-N sort"`

---

### Task 3: Necessary pre-filters from regex and pattern line filters

Tier B pushes only literal `|=`/`!=` today. A `|~ "error|timeout"` query reads every row of its streams. These SQL conditions are **necessary only**: Loki re-applies the real filter, so the only allowed failure mode is "too permissive".

**Files:**
- Modify: `internal/chstore/sqlbuild.go` (`lineFilterSQL`)
- Test: `internal/chstore/sqlbuild_test.go`, `internal/chstore/prefilter_property_test.go` (new)

**Interfaces:**
- Produces: `func regexNecessary(re string) Fragment`, `func patternNecessary(p string) Fragment` (unexported; used by `lineFilterSQL`).

Rules:
- `|~ re`:
  1. Parse with `syntax.Parse(re, syntax.Perl)`. If parsing fails or any node carries `syntax.FoldCase`, emit nothing.
  2. Let the top node be `n`, with `OpCapture` unwrapped. `OpLiteral` gives `position(line, lit) > 0`.
  3. `OpConcat` gives the AND of `position(line, lit) > 0` for each direct `OpLiteral` child, or for a capture wrapping one.
  4. `OpAlternate` whose every alternative is an `OpLiteral` gives the OR of the positions.
  5. Anything else emits nothing. Literals shorter than 2 bytes are skipped.
- `!~` emits nothing.
- `|> pattern`: split on `<…>` captures. Escape `\`, `%` and `_` in each literal piece, then emit `line LIKE '%p1%p2%…%'` from the non-empty pieces in order. With no literal pieces, emit nothing.
- `!>` emits nothing.
- `or` chains with a regex or pattern member: if any member yields nothing, the whole OR yields nothing.

- [ ] **Step 1: Write the table test** in `sqlbuild_test.go`:

```go
func TestLeadingLineFilterSQL_RegexAndPattern(t *testing.T) {
	for q, want := range map[string]string{
		`{a="b"} |~ "error"`:              "position(line, ?) > 0",
		`{a="b"} |~ "err(or)?: timeout"`: "position(line, ?) > 0 AND position(line, ?) > 0", // "err", ": timeout"
		`{a="b"} |~ "error|warn"`:        "(position(line, ?) > 0 OR position(line, ?) > 0)",
		`{a="b"} |~ "(?i)error"`:         "",
		`{a="b"} |~ "e.r"`:               "",
		`{a="b"} !~ "error"`:             "",
		`{a="b"} |> "<_> GET <path> 200"`: "line LIKE ?",
		`{a="b"} |> "<_>"`:               "",
		`{a="b"} |= "x" or "y" |~ "z.*"`: "(position(line, ?) > 0 OR position(line, ?) > 0)",
	} {
		sel, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		require.Equal(t, want, chstore.LeadingLineFilterSQL(sel).SQL, q)
	}
}
```

Assert the arguments too for the pattern case (`"% GET % 200%"`) and for the concat case (`"err"`, `": timeout"`).

- [ ] **Step 2: Write the necessity property test** (`prefilter_property_test.go`). For 2 000 seeded random (filter, line) pairs, it requires that Loki's filter accepting the line implies that ClickHouse evaluates the SQL fragment as true for that line. Build filters from a small generator over literals drawn from `{"err", "or", "timeout", "GET", "ä", "\xff"}` combined with `|`, `(…)?`, `.*`, and `<_>` for patterns. Lines are random concatenations of the same atoms plus noise bytes. Use Loki's filter:

```go
f, err := log.NewFilter(match, ty) // log.LineMatchRegexp or log.LineMatchPattern
ok := f.Filter([]byte(line))
```

and ClickHouse: `SELECT <fragment.SQL with line replaced by ?>` run with `[line] + fragment.Args`. Assert `!ok || sqlTrue`.

- [ ] **Step 3: Run both tests.** Expected: FAIL (regex filters produce no SQL yet).

- [ ] **Step 4: Implement** `regexNecessary` and `patternNecessary` and call them from `lineFilterSQL` for `log.LineMatchRegexp` and `log.LineMatchPattern`. Keep the comment on `LeadingLineFilterSQL` saying the result is *necessary, not exact*.

- [ ] **Step 5: Run.** `go test -count=1 ./internal/chstore/ && make oracle`. Expected: PASS.

- [ ] **Step 6: Commit.** `git commit -am "perf(chstore): necessary SQL pre-filters from regex and pattern line filters"`

---

### Task 4: Regex parity gate for exact SQL filters

Tier A needs **exact** filters. `|=`/`!=` literals are exact as `position()`. A regex is exact as ClickHouse `match(line, re)` only when RE2-in-ClickHouse and Go regexp agree on every input. This task builds a conservative syntactic gate and proves it with a fuzz test.

**Files:**
- Create: `internal/chstore/regexgate.go`
- Test: `internal/chstore/regexgate_test.go`

**Interfaces:**
- Produces: `func RegexExact(re string) bool`.

Gate (operate on `syntax.Parse(re, syntax.Perl)`, *not* simplified). Allowed ops:
- `OpLiteral`: every rune < 0x80 and no `FoldCase` flag.
- `OpCharClass`: every range bound < 0x80 and the class is not negated. A negated class shows up as ranges reaching `0x10FFFF`, which the bound rule already rejects.
- Structure and anchors: `OpConcat`, `OpAlternate`, `OpCapture`, `OpStar`, `OpPlus`, `OpQuest`, `OpRepeat`, `OpEmptyMatch`, `OpBeginText`, `OpEndText`.

Everything else returns false: `.`, line anchors, word boundaries, `\C`, Unicode classes, and any non-ASCII rune. Reason: ASCII-only atoms never match a byte ≥ 0x80 in either engine, whether the line is valid UTF-8 or not, so UTF-8 decoding differences cannot change the outcome.

- [ ] **Step 1: Write the gate table test.**

```go
func TestRegexExact_Table(t *testing.T) {
	for re, want := range map[string]bool{
		"error":            true,
		"err(or)?|warn":    true,
		"^GET /api/[a-z]+": true,
		`\d{3} ms$`:        true,
		"(?i)error":        false, // Go folds k→K (Kelvin), s→ſ
		"e.r":              false,
		"[^a]":             false,
		"größe":            false,
		`\bword\b`:         false,
		"(?m)^x":           false,
		"[":                false, // parse error
	} {
		require.Equal(t, want, chstore.RegexExact(re), re)
	}
}
```

- [ ] **Step 2: Write the fuzz-against-ClickHouse test** `TestRegexGate_FuzzAgainstClickHouse`. It is a seeded property test, not `go test -fuzz`, so it runs in CI.
  1. Generate 3 000 random regexes from an ASCII grammar (literals, `[a-c0-9]`, `\d`, `\s`, groups, `|`, `*+?{1,3}`, `^`, `$`), keeping those with `RegexExact == true`.
  2. Generate 200 lines: ASCII, valid multibyte UTF-8, and invalid bytes (`\xff`, a lone `\xc3`).
  3. For every (regex, line) pair, compare `regexp.MustCompile(re).MatchString(line)` with ClickHouse `SELECT match(?, ?)`.
  4. Batch the pairs into one query per regex: `SELECT arrayMap(l -> match(l, ?), ?)` with all lines as an array argument, so the test stays under ~10 s.
  5. Require zero mismatches, and print the first 10 mismatches on failure.

- [ ] **Step 3: Run.** Expected: FAIL (`RegexExact` undefined).

- [ ] **Step 4: Implement `RegexExact`** as a recursive walk over the parsed tree implementing the allow-list above.

- [ ] **Step 5: Run** the tests on the single node and the cluster. Expected: PASS. If the fuzz test finds a mismatch, **narrow the gate** (never widen it to make a test pass) and add the failing pattern to the table test.

- [ ] **Step 6: Commit.** `git commit -am "feat(chstore): regex parity gate for exact ClickHouse match() filters"`

---

### Task 5: Leaf mapper and Downstreamer (Tier B leaves only)

This task builds and proves the routing machinery with **no SQL changes**. Every leaf still runs on the Phase 1 engine. Done means the whole oracle passes in `query.pushdown: downstream` mode.

**Files:**
- Create: `internal/pushdown/mapper.go`, `internal/pushdown/engine.go`, `internal/pushdown/mapper_test.go`, `internal/pushdown/engine_test.go`
- Modify: `internal/config/config.go`, `internal/api/api.go`, `internal/api/query.go`, `cmd/lokihouse/main.go`

**Interfaces:**
- Consumes: `*logql.QueryEngine` (Phase 1 Tier B), `logql.NewDownstreamEngine`, `logql.DownstreamSampleExpr`, `logql.ParamsWithExpressionOverride`, `logql.NewBufferedAccumulator`.
- Produces:

```go
package pushdown

type Mode string // "on", "off", "downstream"

// Leaf executes one range-aggregation leaf in SQL. Task 6 provides the implementation.
type Leaf interface {
	Eligible(e *syntax.RangeAggregationExpr) bool
	Exec(ctx context.Context, e *syntax.RangeAggregationExpr, p logql.Params) (logqlmodel.Result, error)
}

type Engine struct {
	Mode   Mode
	TierB  *logql.QueryEngine
	TierA  Leaf // nil until Task 6
	Limits logql.Limits
	Logger log.Logger
}

// Query returns the query to execute for p: the mapped downstream query when
// the mode and the expression allow it, otherwise TierB.Query(p).
func (e *Engine) Query(ctx context.Context, p logql.Params) logql.Query

// Map wraps every *syntax.RangeAggregationExpr in logql.DownstreamSampleExpr.
// ok is false if the tree contains a node kind Map does not handle; the caller
// then runs the original query on Tier B.
func Map(expr syntax.SampleExpr) (mapped syntax.SampleExpr, leaves []*syntax.RangeAggregationExpr, ok bool)
```

Mapper node handling, on a freshly parsed expression (re-parse `p.QueryString()`, never mutate `p.GetExpression()`):
- `*syntax.RangeAggregationExpr` → `logql.DownstreamSampleExpr{SampleExpr: e}`.
- `*syntax.VectorAggregationExpr` → recurse into `.Left`.
- `*syntax.BinOpExpr` → recurse into `.SampleExpr` and `.RHS`.
- `*syntax.LabelReplaceExpr` → recurse into `.Left`.
- `*syntax.LiteralExpr` and `*syntax.VectorExpr` → unchanged.
- Any other type → `ok = false`.

`Engine.Query` decision:
- `Mode == off`, a log (non-sample) expression, or `!ok` → `TierB.Query(p)`.
- `Mode == on` with no leaf where `TierA != nil && TierA.Eligible(leaf)` → `TierB.Query(p)`.
- Otherwise: `logql.NewDownstreamEngine(logql.EngineOpts{}, e /* Downstreamable */, e.Limits, e.Logger).Query(ctx, logql.ParamsWithExpressionOverride{Params: p, ExpressionOverride: mapped})`.

The `Downstreamer` runs every query `q` in order:
- If `Mode == on`, `TierA.Eligible(leaf)` holds, and the expression is a `*syntax.RangeAggregationExpr`: run `TierA.Exec(ctx, leaf, q.Params)`.
- Otherwise: run `TierB.Query(q.Params).Exec(ctx)`.

It then calls `acc.Accumulate(ctx, res, i)` and returns `acc.Result()`.

- [ ] **Step 1: Write the mapper test** (`mapper_test.go`):

```go
func TestMap(t *testing.T) {
	for q, leaves := range map[string]int{
		`rate({a="b"}[5m])`:                                    1,
		`sum by (x) (rate({a="b"}[5m]))`:                       1,
		`sum(rate({a="b"}[5m])) / sum(rate({a="c"}[5m]))`:     2,
		`label_replace(rate({a="b"}[1m]), "d", "$1", "a", "(.*)")`: 1,
		`vector(1) + 2`:                                         0,
		`topk(3, sum by (x) (count_over_time({a="b"} | json [1m])))`: 1,
	} {
		expr, err := syntax.ParseSampleExpr(q)
		require.NoError(t, err)
		mapped, ls, ok := pushdown.Map(expr)
		require.True(t, ok, q)
		require.Len(t, ls, leaves, q)
		require.Equal(t, leaves, strings.Count(mapped.String(), "downstream<"), q)
	}
}
```

- [ ] **Step 2: Write the engine equivalence test** (`engine_test.go`). Seed a few streams with `chtest`. For each query in a list of 15 (vector aggs, binops with `on`/`ignoring`/`group_left`, `topk`, `label_replace`, `absent_over_time`, instant and range), require:

```go
want := tierB.Query(p).Exec(ctx)
got := (&pushdown.Engine{Mode: "downstream", TierB: tierB, Limits: o}).Query(ctx, p).Exec(ctx)
require.Equal(t, want.Data, got.Data, q)
```

- [ ] **Step 3: Run.** Expected: FAIL (package does not exist).

- [ ] **Step 4: Implement** `mapper.go` and `engine.go`, then wire them up:
  1. Config: `Query struct { Pushdown string \`yaml:"pushdown"\` } \`yaml:"query"\``, default `"on"`, validated to one of the three modes in `Load`.
  2. `api.API` gets `Queries interface{ Query(context.Context, logql.Params) logql.Query }`. `exec` calls `a.Queries.Query(r.Context(), p).Exec(r.Context())`.
  3. `main.go` builds `&pushdown.Engine{Mode: cfg.Query.Pushdown, TierB: engine, Limits: limits, Logger: logger}`.
  4. `api_test.go`'s `newServer` builds the same with mode `"off"`.

- [ ] **Step 5: Add the oracle mode.** `StartLokihouse` reads `LOKIHOUSE_ORACLE_PUSHDOWN` (default `on`) into the generated config. Add a Makefile target:

```make
oracle-downstream:
	LOKIHOUSE_ORACLE_PUSHDOWN=downstream go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
```

- [ ] **Step 6: Run.** `go test -count=1 ./... && make oracle-downstream && make oracle`. Expected: PASS. A failure in `oracle-downstream` means Loki's downstream evaluation of a leaf differs from evaluating it inline. Fix this by making `Map` return `ok=false` for that node shape (and add the shape to `TestMap` with `ok=false`). Never fix it by post-processing results.

- [ ] **Step 7: Commit.** `git commit -am "feat(pushdown): map range-aggregation leaves onto Loki's downstream engine (Tier B leaves)"`

---

### Task 6: Tier A range-aggregation leaf

**Files:**
- Create: `internal/chstore/buckets.go`, `internal/pushdown/tiera.go`, `internal/pushdown/tiera_test.go`
- Modify: `internal/chstore/sqlbuild.go` (add `ExactLineFilterSQL`), `cmd/lokihouse/main.go` (set `TierA`)

**Interfaces:**
- Consumes: `chstore.ResolveStreams`, `chstore.RegexExact`, `pushdown.Leaf`, `logql.Params`.
- Produces:

```go
package chstore

// ExactLineFilterSQL returns SQL accepting exactly the lines the pipeline's
// filters accept, or ok=false if the pipeline has any stage other than line
// filters that are literal (|=, !=, with or-chains) or regex passing RegexExact (|~, !~).
func ExactLineFilterSQL(expr syntax.LogSelectorExpr) (f Fragment, ok bool)

type BucketRow struct {
	StreamID uint64
	MD       map[string]string
	Bucket   int64  // window sub-bucket index, see BucketCounts
	Count    uint64
	Bytes    uint64
}

// BucketCounts groups the deduplicated (FINAL) rows of streams with origin < ts ≤ through
// into sub-buckets of width g ns: bucket = intDiv(toUnixTimestamp64Nano(ts) - origin + g - 1, g),
// so bucket j holds (origin + (j-1)·g, origin + j·g].
func BucketCounts(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, origin, through, g int64, filter Fragment) ([]BucketRow, error)
```

```go
package pushdown

type TierA struct {
	Conn       driver.Conn
	MaxBuckets int64 // default 100_000; above it the leaf is not eligible
	Limits     logql.Limits
}

func (a *TierA) Eligible(e *syntax.RangeAggregationExpr) bool
func (a *TierA) Exec(ctx context.Context, e *syntax.RangeAggregationExpr, p logql.Params) (logqlmodel.Result, error)
```

Eligibility (static, from the AST only):
- `Operation` ∈ {count_over_time, rate, bytes_over_time, bytes_rate}.
- `e.Grouping == nil` and `e.Left.Unwrap == nil`.
- `ExactLineFilterSQL(e.Left.Left)` returns ok.

The bucket cap is checked at `Exec` time. If it is exceeded, `Exec` returns the Tier B result for that leaf (it holds a `*logql.QueryEngine` for that purpose), so eligibility stays static.

Exec algorithm. Let `r = Interval`, `o = Offset`, `S = start − o` and `E = end − o` (both ns), and `st = step` (`0` for instant queries).
1. `g = gcd(st, r)`, with `g = r` when `st == 0`. `origin = S − r`. The number of buckets is `(E − origin)/g + 1`; if it exceeds `MaxBuckets`, fall back to Tier B.
2. Resolve the streams with `ResolveStreams(…, sel.Matchers(), time.Unix(0, origin), time.Unix(0, E))`. Then run `BucketCounts(…, origin, E, g, filter)`.
3. Group the rows by series key `(StreamID, sorted MD)`. Compute the labels per key once with Loki's extractor (see Global Constraints). If more series than `Limits.MaxQuerySeries(ctx, tenant)` exist, return Loki's error `logqlmodel.NewSeriesLimitError(max)`.
4. For each series, build `cnt[j]` and `bytes[j]` per bucket, then a prefix sum. For each step `k` with `t = S + k·st ≤ E` (once when instant), the window is buckets `j ∈ ((t − r − origin)/g, (t − origin)/g]`. Both bounds are exact integer divisions, because `g` divides `st`, `r` and `t − origin`. Then `n = Σ cnt` and `b = Σ bytes`. If `n == 0`, emit no point. Otherwise emit `promql.FPoint{T: t/1e6 + o/1e6, F: value}` with the values from Global Constraints.
5. A range query returns a `promql.Matrix` sorted by `labels.Compare`, an instant query a `promql.Vector`. Both are wrapped in `logqlmodel.Result{Data: …}`.

`BucketCounts` SQL:

```sql
SELECT stream_id, structured_metadata,
       intDiv(toUnixTimestamp64Nano(ts) - ? + ? - 1, ?) AS bucket,
       count() AS n, sum(length(line)) AS bytes
FROM logs FINAL
WHERE tenant = ? AND stream_id IN ? AND ts > fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC') [AND <exact filter>]
GROUP BY stream_id, structured_metadata, bucket
```

- [ ] **Step 1: Write the failing tests** in `tiera_test.go`. Each compares Tier A with Tier B (`pushdown.Engine` in mode `off`) on hand-built data:
  - `TestTierA_WindowBoundaries`: entries at exactly `t`, `t − range`, `t − range + 1ns` and `t + 1ns` for `count_over_time` and `rate`, with and without `offset 1m`, with step 15s and range 1m, then range 50s and step 20s (gcd 10s).
  - `TestTierA_CountsDuplicatesOnce`: push the same batch twice (two parts, no `OPTIMIZE`). Tier A must equal Tier B.
  - `TestTierA_MetadataSeries`: same stream, entries with different `structured_metadata`. The series labels must equal Tier B's.
  - `TestTierA_Filters`: `|= "a" != "b" or "c" |~ "x[0-9]+"`.
  - `TestTierA_Ineligible`: `count_over_time({a="b"} | json [1m])`, `sum_over_time(... | unwrap x [1m])`, `|~ "(?i)x"` → `Eligible == false`.
  - `TestTierA_MaxSeries`: 50 streams with `max_query_series: 10` must give an error whose message equals Tier B's.

  Equality: `require.Equal(t, tierB.Data, tierA.Data)`. Values must be bit-identical, because the arithmetic is the same.

- [ ] **Step 2: Run.** Expected: FAIL (types undefined).

- [ ] **Step 3: Implement** `ExactLineFilterSQL`, `BucketCounts` and `TierA`. `ExactLineFilterSQL` reuses `lineFilterSQL` for literals and adds `match(line, ?)` / `NOT match(line, ?)` when `RegexExact` holds. The `or` chain for `!~`/`!=` is the AND of the negations, which is exactly Loki's semantics (see the comment on `syntax.LineFilterExpr`). Set `TierA` in `main.go`.

- [ ] **Step 4: Run.** `go test -count=1 ./internal/... && make test-cluster`. Expected: PASS on both. On the cluster, `FINAL` runs per shard, which is correct because all rows of a stream share a shard.

- [ ] **Step 5: Commit.** `git commit -am "feat(pushdown): Tier A count/rate/bytes range aggregations in ClickHouse"`

---

### Task 7: Property tests Tier A == Tier B

**Files:**
- Create: `internal/pushdown/property_test.go`

**Interfaces:**
- Consumes: `pushdown.Engine` (modes `on` and `off`), `chtest.New`, `chstore.Writer`.

- [ ] **Step 1: Write the property test** `TestProperty_TierAEqualsTierB` (seeded and reproducible; on failure, print the seed and the query).

  Data, once per test: 6 streams, 3 000 entries over 2h. Timestamps cluster on round seconds, so many land exactly on window boundaries, and 5% of entries share a timestamp within a stream. 3% are exact duplicates, pushed in separate batches. 20% carry 1–2 structured-metadata keys. Lines are random words from 12 words.

  Queries: 300 draws per seed, 3 seeds.
  - op ∈ the four eligible ops
  - range ∈ {10s, 45s, 1m, 5m, 17m}
  - step ∈ {0 (instant), 1s, 15s, 60s, 7m}
  - start = data start + random ns in [0, 30m)
  - end = start + random in [0, 90m]
  - offset ∈ {0, 1m, 90s}
  - filter ∈ {none, `|= w`, `!= w`, `|= w1 or w2`, `|~ "w1|w2"`, `!~ "^w[0-9]"`}
  - wrapper ∈ {none, `sum by (app)`, `count`, `topk(2, …)`, `… / on() group_left sum(…)`}

  For each draw, the result with mode `on` must equal the result with mode `off`. Compare with `require.Equal` on `Data`, apart from the `topk` tie caveat from Phase 1: skip `topk` draws where Tier B has equal values at the k-th position.

  Also assert that at least 80% of the draws actually ran a Tier A leaf, using an exported counter `pushdown.TierALeaves` (an `atomic.Int64` incremented in `Exec`). That stops the test passing vacuously.

- [ ] **Step 2: Run.** `go test -count=1 -run TestProperty ./internal/pushdown/` (single node), then with the cluster env. Expected: PASS. Every failure is a real bug. Shrink it by hand to a `TestTierA_*` case in `tiera_test.go`, fix it, and keep the case.

- [ ] **Step 3: Commit.** `git commit -am "test(pushdown): property tests Tier A == Tier B"`

---

### Task 8: Oracle coverage for push-down

**Files:**
- Create: `test/oracle/corpus/pushdown.logql`
- Modify: `test/oracle/oracle_test.go`, `test/oracle/lokihouse.go`, `Makefile`

- [ ] **Step 1: Write the corpus** `pushdown.logql`, about 40 queries, `range:`/`instant:` prefixed as in `basic.logql`. Cover:
  - each eligible op
  - offsets
  - ranges that do not divide the 60s step (45s, 7m)
  - regex filters that pass and fail the gate
  - wrappers: `sum by`, `/ on()`, `topk` with distinct values, `label_replace`
  - leaves mixed with ineligible siblings (`count_over_time(… | logfmt [5m])`)

- [ ] **Step 2: Assert tier coverage.**
  1. Add a debug endpoint `GET /debug/lokihouse/tiera_leaves` that returns `pushdown.TierALeaves`. It is registered only when `LOKIHOUSE_DEBUG=1`, which the oracle sets.
  2. After the corpus, require that the counter grew by at least the number of eligible leaves in `pushdown.logql` × 2 tenants. Compute that number in the test by parsing each query and counting `Eligible` leaves with a `TierA{}` that needs no connection: `Eligible` is static.

- [ ] **Step 3: Add the Makefile targets** `oracle-all` (modes `off`, `downstream`, `on` on a single node) and `oracle-cluster` (mode `on`).

- [ ] **Step 4: Run.** `make oracle-all && make oracle-cluster`. Expected: PASS. Divergences from Loki that are not lokihouse bugs go into `docs/divergences.md` with their reason, as spec §7.4 requires, and need the owner's approval.

- [ ] **Step 5: Commit.** `git commit -am "test(oracle): push-down corpus, per-mode runs and Tier A coverage"`

---

### Task 9: Benchmark and phase status

**Files:**
- Create: `internal/pushdown/bench_test.go`, `docs/phase2-status.md`

- [ ] **Step 1: Write the benchmarks.** `BenchmarkLeaf/{tierA,tierB}/{count_over_time,rate_filtered,sum_by_rate}` seeds 1 000 000 entries over 200 streams and 24h once, using `INSERT … SELECT FROM numbers()` directly rather than the Writer, for speed. Each benchmark runs a 24h range query with a 1m step and a 5m range, and reports `ns/op` and `B/op`. Gate the benchmark with `testing.Short()` so it stays out of `make test`.

- [ ] **Step 2: Run** `go test -run '^$' -bench BenchmarkLeaf -benchmem ./internal/pushdown/` on the single node and record the numbers.

- [ ] **Step 3: Write `docs/phase2-status.md`.**
  - Exit criteria table: all of `make test`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` green, and the Tier A property test green on both topologies.
  - Benchmark table.
  - Decision record: vector aggregations stay in Loki's evaluator over Tier A leaves. Pushing `sum by` into SQL only reduces the series × steps transfer, while the leaf push-down removed the per-line transfer. Revisit only if the benchmark shows series transfer dominating at realistic cardinality.
  - What stays Tier B: parsers, unwrap, `line_format`, `(?i)` regexes, grouping inside range aggregations.

- [ ] **Step 4: Commit.** `git commit -am "docs: phase 2 status and push-down benchmark"`

---

## Phase 2 exit criteria

1. `make test`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` green.
2. `TestProperty_TierAEqualsTierB` green on both topologies, with ≥ 80% Tier A leaf coverage.
3. `docs/divergences.md` absent, or listing only owner-approved entries.
4. Benchmark shows Tier A beating Tier B on `count_over_time` over 1M entries (expected: at least an order of magnitude in both time and memory).

## Deferred (not Phase 2, recorded so it is not lost)

- Parser push-down (`| json k="a.b"` → `JSONExtract…`, `| logfmt`) and `unwrap` aggregations need a parity gate per parser. They go in Phase 2b, after the benchmark shows how much Tier B parser queries cost.
- `sum by` / `count by` vector aggregation in SQL: see the Task 9 decision record.
- Label-regex matchers resolved in SQL (`match(labels['x'], …)` behind `RegexExact`): only matters for tenants with very many streams; measure first.
