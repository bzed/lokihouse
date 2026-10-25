# Parallel Engine (time-split Tier B range queries) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split metric range queries that run on Loki's engine (Tier B) into time legs, evaluate the legs in parallel under a per-request and a process-wide connection budget, and merge them into exactly the unsplit answer.

**Architecture:** A new `pushdown.Splitter` has the shape of `*logql.QueryEngine` (`Query(p) logql.Query`). `pushdown.Engine` routes every single-tenant Tier B evaluation through it (the whole-query path and `execOne`). The splitter cuts the query's own step grid at wall-clock multiples of `split_queries_by_interval`, runs each leg with `TryAcquire`-or-inline semantics (never waits for a reservation), merges matrices by series, and falls back to one unsplit run when a Logs Drilldown request overflows `max_query_series`.

**Tech Stack:** Go, Loki at `LOKI_COMMIT` (`logql`, `logqlmodel`, `stats`, `metadata`, `httpreq`, `util/validation`), `golang.org/x/sync/semaphore` + `errgroup` (already in Loki's graph at v0.24.0), ClickHouse 26.9 for tests.

**Spec:** `docs/superpowers/specs/2026-10-25-parallel-engine-design.md`

## Global Constraints

- Reuse Loki's code; code copied from Loki starts with `// Copied from grafana/loki@bc1962860a1607eccb7e0c8ee206970b292b9e80 <path>; modifications: <list>`.
- Loki's names exactly: `split_queries_by_interval` (default `1h`, `0` disables), `max_query_parallelism` (default 32), `max_query_series`, histogram `loki_query_frontend_partitions` with Loki's help `Number of time-based partitions (sub-requests) per request` and buckets `prometheus.ExponentialBuckets(1, 4, 5)`.
- Exactness: split result equals the unsplit result — same series, same timestamps, values within relative 1e-9 (`requireDataEqual` in `internal/pushdown/engine_test.go`, `test/oracle/compare.go`).
- Evaluation timestamps are `start + k·step`, `k = 0 … ⌊(end−start)/step⌋`; never Loki's `alignStartEnd`.
- Never split: log expressions, instant queries (`step == 0` or `start == end`), multi-tenant requests, Tier A leaves.
- Process budget `max(1, max_open_conns / 2)` units; a leg's need is `(number of *syntax.LogRangeExpr in its expression) + 1`.
- A piece takes its reservation with `TryAcquire` only; on failure the coordinator runs it inline. Nothing ever blocks on a semaphore.
- No package-level mutable state except the Prometheus collector in `internal/server` (as `deleteClientMetrics`).
- `goleak.VerifyTestMain` in `internal/pushdown` (it now starts goroutines). No `t.Parallel()`, no sleeps to wait for state (`require.Eventually`).
- `make check` passes after every task; ≥ 85 % coverage. Conventional commit prefixes; commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Changes to query code also pass `make test-cluster`, `make oracle-all`, `make oracle-cluster` (Task 8) and `make soak` (Task 9).

## Plan decisions (where the spec left a choice, or the code contradicted it)

- **Goal 2 is already met by Loki.** `pushdown.Map` only produces `DownstreamSampleExpr` leaves, and `DownstreamEvaluator.NewStepEvaluator` calls `Downstream` with exactly one query per leaf. Loki's `newBinOpStepEvaluator` (`pkg/logql/evaluator.go:948`) already evaluates both sides of a binary operation on two goroutines (errgroup). Making the downstreamer's loop concurrent would change nothing. The plan does **not** touch the loop; Task 6 adds a test pinning that sibling leaves run concurrently today, so a Loki bump that changes it fails loudly. Therefore the only "pieces" are Tier B legs. Leaves stay unreserved coordinators, as today; they hold no connection while they wait (a `DownstreamSampleExpr` result is fully materialised).
- **The per-request limit is shared through the context.** `Engine.Query` wraps the returned query, and its `Exec` installs one request-slot semaphore (`max_query_parallelism` slots), so both sides of a binary operation and any nested splitter share it. `Splitter` installs one itself if none is present.
- **Per-request slots also use `TryAcquire`.** When the request's slots are full, the coordinator runs the leg inline instead of waiting. A request therefore runs at most `max_query_parallelism` leg goroutines plus its coordinators' inline legs. `max_query_parallelism ≤ 0` means zero slots: everything runs inline.
- **The histogram observes every range sample query that reaches the splitter with splitting enabled**, including 1-leg queries (as Loki's middleware does). It does not observe when the interval is `0`.
- **The whole split `Exec` runs under the tenant's `query_timeout`** (`SmallestPositiveNonZeroDurationPerTenant`, as `query.Eval`), so inline sequential legs cannot exceed it in total.
- **Leg errors are returned unwrapped** so that `errors.As`, the HTTP status mapping and the message stay the same as unsplit.
- **Drilldown overflow detection:** the rerun is triggered when any leg's `Warnings` contains Loki's `seriesLimitPartialResultsWarning(maxSeries)` text (copied) **or** the merged result has more than `maxSeries` series.
- **Oracle:** `TestOracle` runs lokihouse with `split_queries_by_interval: 15m`, so every 95-minute corpus range query splits into ≥ 7 legs. The default `1h` is covered by the property test and the benchmark (which compares answers). A new corpus kind `unaligned:` shifts `start` by 17 s and uses a 45 s step.

## Review Focus

1. **A range whose `end − start` is not a multiple of `step`** (Grafana sends these all the time): the last grid point is before `end`, and no leg may evaluate at `end` itself. Pinned in Task 1 (`TestSplitLegs` case "end off grid").
2. **All legs empty, or a series present only in some legs** (data gaps, a pod that started mid-range): the merge returns a non-nil empty `promql.Matrix` (JSON `[]`, not `null`), and partial series keep only their own points. Pinned in Task 2 (`TestMergeLegResults_EmptyAndPartialSeries`).
3. **Client disconnect or `query_timeout` in the middle of a split:** every leg stops, `Exec` returns the context error, and no goroutine outlives `Exec`. Pinned in Task 3 (`TestRunPieces_CancelStopsAll`) and Task 4 (`TestSplitter_QueryTimeoutCoversAllLegs`).
4. **A tiny interval over a long range** (`split_queries_by_interval: 1m`, 7 days ⇒ 10 080 legs): goroutines stay bounded by the request slots, and the coordinator feeds legs lazily. Pinned in Task 3 (`TestRunPieces_ManyPiecesBoundedConcurrency`).
5. **Tiny pools and odd limits** (`max_open_conns: 1`, `max_query_parallelism: 0`): the budget is never 0, every leg runs inline, and the answer is unchanged. Pinned in Task 3 (`TestNewConnBudget`, `TestRunPieces_ZeroSlotsRunsInline`) and Task 4 (`TestSplitter_ExhaustedBudgetRunsInline`).

---

### Task 1: Leg planning (grid, interval, connection need)

**Files:**
- Create: `internal/pushdown/legs.go`
- Test: `internal/pushdown/legs_test.go`

**Interfaces:**
- Produces:
  - `type leg struct{ start, end time.Time }`
  - `func splitInterval(expr syntax.Expr, configured time.Duration) time.Duration` returns 0 when `configured <= 0`, otherwise `max(configured, largest [range] in expr)`. Offsets are ignored.
  - `func splitLegs(start, end time.Time, step, interval time.Duration) []leg` requires `step > 0` and `interval > 0`.
  - `func connNeed(expr syntax.Expr) int64` returns the count of `*syntax.LogRangeExpr` nodes plus 1.
  - `type legParams struct{ logql.Params; start, end time.Time }` overrides `Start()` and `End()`.

- [ ] **Step 1: Write the failing tests**

```go
func TestSplitLegs(t *testing.T) {
	at := func(s string) time.Time { v, _ := time.Parse(time.TimeOnly, s); return time.Date(2026, 1, 1, v.Hour(), v.Minute(), v.Second(), 0, time.UTC) }
	cases := []struct {
		name          string
		start, end    string
		step, iv      time.Duration
		want          [][2]string
	}{
		{"unaligned start", "10:07:30", "12:20:00", 5 * time.Minute, time.Hour,
			[][2]string{{"10:07:30", "10:57:30"}, {"11:02:30", "11:57:30"}, {"12:02:30", "12:17:30"}}},
		{"end off grid", "10:00:00", "10:59:59", 15 * time.Minute, 30 * time.Minute,
			[][2]string{{"10:00:00", "10:15:00"}, {"10:30:00", "10:45:00"}}},
		{"start on boundary", "10:00:00", "11:00:00", 15 * time.Minute, time.Hour,
			[][2]string{{"10:00:00", "10:45:00"}, {"11:00:00", "11:00:00"}}},
		{"step equals interval", "10:30:00", "13:30:00", time.Hour, time.Hour,
			[][2]string{{"10:30:00", "10:30:00"}, {"11:30:00", "11:30:00"}, {"12:30:00", "12:30:00"}, {"13:30:00", "13:30:00"}}},
		{"step above interval", "10:00:00", "14:30:00", 90 * time.Minute, time.Hour,
			[][2]string{{"10:00:00", "10:00:00"}, {"11:30:00", "11:30:00"}, {"13:00:00", "13:00:00"}, {"14:30:00", "14:30:00"}}},
		{"one bucket", "10:05:00", "10:55:00", time.Minute, time.Hour,
			[][2]string{{"10:05:00", "10:55:00"}}},
	}
	// for each case: require.Equal(t, want-as-legs, splitLegs(...))
}

func TestSplitInterval(t *testing.T) {
	// configured 1h: `rate({a="b"}[5m])` → 1h; `rate({a="b"}[2h])` → 2h;
	// `rate({a="b"}[5m] offset 3h)` → 1h;
	// `rate({a="b"}[5m]) / rate({a="b"}[90m])` → 90m; configured 0 with `[2h]` → 0.
}

func TestConnNeed(t *testing.T) {
	// `quantile_over_time(0.9, {a="b"} | unwrap x [5m])` → 2
	// `count_over_time({a="1"}[1m]) / count_over_time({a="2"}[1m])` → 3
	// `vector(1)` → 1
	// `sum(rate({a="1"}[1m])) + sum(rate({a="2"}[1m])) * count_over_time({a="3"}[1m])` → 4
}

func TestLegParams(t *testing.T) {
	// legParams over a LiteralParams: Start()/End() are the leg's;
	// QueryString, Step, GetExpression are the original's.
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pushdown -run 'TestSplitLegs|TestSplitInterval|TestConnNeed|TestLegParams' -count=1`
Expected: FAIL to compile (`undefined: splitLegs` …).

- [ ] **Step 3: Implement `legs.go`**

`splitInterval` copies Loki's `maxRangeVectorAndOffsetDuration` (`pkg/querier/queryrange/split_by_interval.go:310`) with a Copied-from header; modification: offset dropped. In `splitLegs`, a leg is a maximal run of grid points `start + k·step` (`k` up to `⌊(end−start)/step⌋`) that share the bucket `ts.UnixNano() / interval.Nanoseconds()`. This is Loki's epoch-based bucketing, not `time.Truncate`. Walk the grid by bucket boundaries instead of point by point: from a point `t`, the leg's last point is the largest grid point below the next bucket boundary, capped at the last grid point. This keeps a 1 s step over 7 days cheap.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pushdown -run 'TestSplitLegs|TestSplitInterval|TestConnNeed|TestLegParams' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pushdown/legs.go internal/pushdown/legs_test.go
git commit -m "feat: plan time legs for engine-run range queries on the query's step grid"
```

---

### Task 2: Merging leg results

**Files:**
- Create: `internal/pushdown/merge.go`
- Test: `internal/pushdown/merge_test.go`

**Interfaces:**
- Produces: `func mergeLegResults(results []logqlmodel.Result) (logqlmodel.Result, error)`. Every `Data` must be a `promql.Matrix` (otherwise it returns an error naming the type). The function:
  - concatenates the points per series (keyed by `labels.Labels.String()`) in leg order, then sorts with `sort.Sort` on the `promql.Matrix`.
  - folds `Statistics` with `stats.Result.MergeSplit`, so `Summary.Splits == len(results)`.
  - de-duplicates `Warnings` and `Headers` through a fresh `metadata.NewContext` (`AddWarning`, `metadata.JoinHeaders`).

- [ ] **Step 1: Write the failing tests**

```go
func TestMergeLegResults_ConcatenatesAndSorts(t *testing.T) {
	// leg0: {a="2"} points T=1,2; {a="1"} T=1
	// leg1: {a="1"} T=3; {a="2"} T=3,4
	// want: Matrix [{a="1"}: 1,3], [{a="2"}: 1,2,3,4] (sort.Sort order), values carried unchanged
	// want: Statistics.Summary.Splits == 2
}

func TestMergeLegResults_EmptyAndPartialSeries(t *testing.T) {
	// all legs empty Matrix{} → got.Data is promql.Matrix, non-nil, len 0
	// series only in leg1 → only leg1's points
}

func TestMergeLegResults_DedupesWarnings(t *testing.T) {
	// leg0 Warnings {"w1","w2"}, leg1 {"w2"} → ElementsMatch {"w1","w2"}
}

func TestMergeLegResults_RejectsNonMatrix(t *testing.T) {
	// a promql.Vector leg → error
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pushdown -run TestMergeLegResults -count=1`
Expected: FAIL to compile (`undefined: mergeLegResults`).

- [ ] **Step 3: Implement `mergeLegResults` in `merge.go`**

Build the result with `make(promql.Matrix, 0, n)`, so an all-empty merge is non-nil.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pushdown -run TestMergeLegResults -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pushdown/merge.go internal/pushdown/merge_test.go
git commit -m "feat: merge time-leg matrices by series without re-aggregating"
```

---

### Task 3: Piece runner, request slots and connection budget

**Files:**
- Create: `internal/pushdown/pieces.go`
- Create: `internal/pushdown/main_test.go` (goleak, copying `internal/chstore/main_test.go`'s ignore list and comment, package `pushdown`)
- Test: `internal/pushdown/pieces_test.go`
- Modify: `go.mod` / `go.sum` (`golang.org/x/sync` becomes direct; `go mod tidy`)

**Interfaces:**
- Produces:
  - `func NewConnBudget(maxOpenConns int) *semaphore.Weighted` with capacity `max(1, maxOpenConns/2)`.
  - `type parallelismLimits interface{ MaxQueryParallelism(ctx context.Context, userID string) int }`
  - `func withRequestSlots(ctx context.Context, l parallelismLimits) context.Context` installs a `*semaphore.Weighted` sized `SmallestPositiveIntPerTenant(tenants, MaxQueryParallelism)`, clamped to ≥ 0, under an unexported context key. It is a no-op when one is already installed.
  - `func requestSlots(ctx context.Context) *semaphore.Weighted`, nil when absent.
  - `func runPieces(ctx context.Context, n int, need int64, slots, budget *semaphore.Weighted, run func(ctx context.Context, i int) error) error`. For each `i` in order, while the context is live: if `slots.TryAcquire(1)` and `budget.TryAcquire(need)` both succeed, it runs `run` on an errgroup goroutine that releases both on return; otherwise it releases whatever it took and calls `run` inline. A nil `slots` or `budget` means "always inline". The first error cancels the shared context. It returns only after every goroutine has finished, and returns the first error.

- [ ] **Step 1: Write the failing tests**

```go
func TestNewConnBudget(t *testing.T) {
	// maxOpenConns 100 → TryAcquire(50) true, then TryAcquire(1) false
	// maxOpenConns 1 → capacity 1; maxOpenConns 0 → capacity 1
}

func TestRunPieces_ManyPiecesBoundedConcurrency(t *testing.T) {
	// n=10000, slots=NewWeighted(4), budget=NewWeighted(100), need=2
	// run increments an atomic in-flight counter, records the max, decrements
	// want: every i ran exactly once; max in-flight ≤ 5 (4 goroutines + inline coordinator)
}

func TestRunPieces_ExhaustedBudgetRunsInlineWithoutWaiting(t *testing.T) {
	// budget := NewWeighted(4); budget.Acquire(ctx, 4) held by the test for the whole call
	// n=50, slots NewWeighted(32), need 2; run tracks max in-flight with an atomic counter
	// want: returns nil (it did not wait on the held budget), all n ran, max in-flight == 1
}

func TestRunPieces_ZeroSlotsRunsInline(t *testing.T) {
	// slots = NewWeighted(0), budget NewWeighted(100): all ran, max in-flight == 1
}

func TestRunPieces_NeedAboveBudgetRunsInline(t *testing.T) {
	// budget NewWeighted(2), need 3 → all inline
}

func TestRunPieces_FirstErrorCancelsRest(t *testing.T) {
	// piece 0 returns errBoom after the others have started and block on <-ctx.Done()
	// want: errors.Is(err, errBoom); every started piece observed ctx.Err() != nil
}

func TestRunPieces_CancelStopsAll(t *testing.T) {
	// parent ctx cancelled while pieces block on ctx.Done()
	// want: errors.Is(err, context.Canceled); runPieces has returned (goleak checks no leftovers)
}

func TestWithRequestSlots(t *testing.T) {
	// tenant "t1" with MaxQueryParallelism 3 → requestSlots(ctx).TryAcquire(3) true, (1) false
	// calling withRequestSlots again returns the same semaphore
	// MaxQueryParallelism 0 → TryAcquire(1) false
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pushdown -run 'TestNewConnBudget|TestRunPieces|TestWithRequestSlots' -count=1`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `pieces.go`, add `main_test.go`, run `go mod tidy`**

The doc comment on `runPieces` states the deadlock argument from the spec ("Why there is no cycle") in three sentences.

- [ ] **Step 4: Run tests (with race) to verify they pass**

Run: `go test -race ./internal/pushdown -count=1`
Expected: PASS, no goleak report.

- [ ] **Step 5: Commit**

```bash
git add internal/pushdown/pieces.go internal/pushdown/pieces_test.go internal/pushdown/main_test.go go.mod go.sum
git commit -m "feat: run query pieces in parallel only when a reservation is free"
```

---

### Task 4: The splitting Tier B

**Files:**
- Create: `internal/pushdown/split.go`
- Test: `internal/pushdown/split_test.go`, `internal/pushdown/split_property_test.go`

**Interfaces:**
- Consumes: `splitInterval`, `splitLegs`, `connNeed`, `legParams` (Task 1); `mergeLegResults` (Task 2); `runPieces`, `withRequestSlots`, `requestSlots`, `parallelismLimits` (Task 3).
- Produces:
  - `type SplitLimits interface { logql.Limits; parallelismLimits; QuerySplitDuration(userID string) time.Duration }` (`*validation.Overrides` satisfies it).
  - `type SplitMetrics struct{ partitions prometheus.Histogram }`; `func NewSplitMetrics(r prometheus.Registerer) *SplitMetrics` (definition copied from `pkg/querier/queryrange/split_by_interval.go:61-75` with header). Observing through a nil `*SplitMetrics` is a no-op.
  - `type Splitter struct { Engine *logql.QueryEngine; Limits SplitLimits; Budget *semaphore.Weighted; Metrics *SplitMetrics }`
  - `func (s *Splitter) Query(p logql.Params) logql.Query`

`Exec(ctx)` behaviour, in order:
1. If the expression is not a `syntax.SampleExpr`, `step == 0`, or `!start.Before(end)`, return `s.Engine.Query(p).Exec(ctx)`.
2. Compute the interval: tenants from `tenant.TenantIDs(ctx)`, `SmallestPositiveNonZeroDurationPerTenant(tenants, QuerySplitDuration)`, then `splitInterval`. If it is 0, return the unsplit result.
3. Compute the legs and observe `len(legs)`. One leg means unsplit.
4. `ctx = withRequestSlots(ctx, s.Limits)`, then apply the tenant `query_timeout` with `context.WithTimeout`.
5. Run `runPieces(ctx, len(legs), connNeed(expr), requestSlots(ctx), s.Budget, …)`. Each piece runs `s.Engine.Query(legParams{…}).Exec(ctx)` into `results[i]`. Return the first leg error as-is.
6. `maxSeries := SmallestPositiveIntPerTenant(tenants, s.Limits.MaxQuerySeries)`. If `httpreq.IsLogsDrilldownRequest(ctx)` and (any leg warning equals `seriesLimitPartialResultsWarning(maxSeries)`, copied from `pkg/logql/engine.go:68`, or the merged series count > `maxSeries`), return `s.Engine.Query(p).Exec(ctx)`. Otherwise, if `maxSeries > 0` and the merged count > `maxSeries`, return `logqlmodel.NewSeriesLimitError(maxSeries)`.
7. Return the merged result.

Test fixture (shared by the tests below; ClickHouse-backed via `chtest.New`): `seedSplitData(t, w) time.Time` writes tenant `t1`, apps `web`, `api`, `worker`, from `2026-01-01T00:00:00Z` for 6 h, one line every 7 s per app (`level=<info|error> dur=<0..999>` from `rand.New(rand.NewSource(11))`), plus app `early` only in hours 0–2 and app `late` only in hours 4–6. `newSplitter(t *testing.T, conn driver.Conn, budget *semaphore.Weighted, mod func(*validation.Limits)) (*Splitter, *logql.QueryEngine)` builds overrides from `validation.Limits` defaults (`RegisterFlags` on a throwaway FlagSet), sets `QuerySplitDuration = model.Duration(time.Hour)` and `MaxQuerySeries = 500`, applies `mod` (nil is allowed), and returns the splitter plus the raw engine it wraps.

- [ ] **Step 1: Write the failing tests**

```go
func TestSplitter_SplitsRangeQueryAndReportsSplits(t *testing.T) {
	// iv 1h, `quantile_over_time(0.9, {app="web"} | logfmt | unwrap dur [5m]) by (app)`,
	// start base+10m+3s, end base+3h40m, step 1m
	// want: requireDataEqual(raw.Exec, split.Exec); got.Statistics.Summary.Splits == 4
}

func TestSplitter_DoesNotSplit(t *testing.T) {
	// table: instant query (step 0, start==end); log query `{app="web"}` range;
	// interval 0; a range inside one bucket
	// want: Summary.Splits == 0 and data equals raw
}

func TestSplitter_SeriesLimit(t *testing.T) {
	// MaxQuerySeries = 1; `count_over_time({app=~"early|late"}[1m])`, base+1h..base+5h, step 1m, iv 1h:
	// every leg sees at most one series (early ends at 2h, late starts at 4h), the merge sees two.
	// normal ctx: errors.As(err, &limitErr) with *logqlmodel.LimitError, and
	//   err.Error() == logqlmodel.NewSeriesLimitError(1).Error()
	// Drilldown ctx (httpreq.InjectQueryTags(ctx, "source="+constants.LogsDrilldownAppName)):
	//   split result equals raw.Exec(drilldownCtx): Data and Warnings (the partial-results warning)
	// A leg that overflows on its own (MaxQuerySeries = 1, `count_over_time({app=~"web|api"}[1m])`):
	//   normal ctx → same LimitError as raw; Drilldown ctx → equals raw (leg-warning path)
}

func TestSplitter_ExhaustedBudgetRunsInline(t *testing.T) {
	// budget semaphore.NewWeighted(0); same query as the first test
	// want: equal data, Summary.Splits == 4
}

func TestSplitter_QueryTimeoutCoversAllLegs(t *testing.T) {
	// mod sets QueryTimeout = model.Duration(time.Nanosecond); split query → errors.Is(err, context.DeadlineExceeded)
	// (errors.Is), and Exec returns (goleak checks no leftovers)
}

func TestSplitter_ObservesPartitions(t *testing.T) {
	// Metrics from NewSplitMetrics(prometheus.NewRegistry()); after the first test's query
	// testutil.CollectAndCount / histogram sample count == 1, sum == 4
}
```

`split_property_test.go`:

```go
// TestProperty_SplitEqualsUnsplit: seeds 1..3 × 150 draws, rand.New(rand.NewSource(seed)).
// Draw: iv ∈ {10m, 30m, 1h}; step ∈ {15s, 1m, 7m, 30m, 1h, 90m};
// range ∈ {1m, 5m, 45m, 2h}; offset ∈ {0, 5m};
// start = base + rand ns in [0, 1h); end = start + rand ns in [step, 5h];
// query from templates (fill R, O):
//   quantile_over_time(0.9, {app="web"} | logfmt | unwrap dur [R] O) by (app)
//   sum by (app) (rate({app=~".+"} | logfmt | level="error" [R] O))
//   count_over_time({app="web"}[R] O) / count_over_time({app="api"} | logfmt [R] O)
//   topk(2, sum by (app) (count_over_time({app=~".+"} | logfmt [R] O)))
//   absent_over_time({app="nope"}[R] O)
//   max_over_time({app=~"web|late"} | logfmt | unwrap dur [R] O) by (app)
// Compare raw vs splitter: requireDataEqual (topk: sortedValues), errors equal by message.
// Assert ≥ 60 % of draws had Summary.Splits > 1 (non-vacuous).
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pushdown -run 'TestSplitter|TestProperty_SplitEqualsUnsplit' -count=1`
Expected: FAIL to compile (`undefined: Splitter`).

- [ ] **Step 3: Implement `split.go`**

- [ ] **Step 4: Run tests (with race) to verify they pass**

Run: `go test -race ./internal/pushdown -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pushdown/split.go internal/pushdown/split_test.go internal/pushdown/split_property_test.go
git commit -m "feat: split engine-run metric range queries by time and evaluate legs in parallel"
```

---

### Task 5: Pool-safety regression test

**Files:**
- Test: `internal/pushdown/split_pool_test.go`

**Interfaces:**
- Consumes: `Splitter`, `NewConnBudget`, `seedSplitData`, `newSplitter` (Task 4).

- [ ] **Step 1: Write the test**

```go
// TestSplitter_ConcurrentMultiLegQueriesNeverDeadlockPool: C = 5 concurrent
// requests, each `count_over_time({app="web"} | logfmt [5m]) / count_over_time({app="api"} | logfmt [5m])`
// over base..base+6h, step 1m, iv 10m (36 legs of need 3), max_query_parallelism 32.
// Pool: _, db := chtest.New(t); opts := chtest.Options(db); opts.MaxOpenConns = 24;
// opts.MaxIdleConns = 24; opts.DialTimeout = 5 * time.Second.
// Budget NewConnBudget(24) = 12: at most 4 reserved legs. The other 12 connections
// serve inline legs: C inline legs need at most C·(3−1)+1 = 11 ≤ 12, so inline
// work alone cannot hit the pre-existing N4 wait (REVIEW.md §000) and the test
// fails only if pieces exceed the budget.
// Three rounds; every request: require.NoError (no "acquire conn timeout") and
// data equal to a sequential reference computed once before the rounds.
```

- [ ] **Step 2: Check the test catches the failure it pins**

Temporarily build the splitter with `semaphore.NewWeighted(1000)` instead of `NewConnBudget(24)`.
Run: `go test ./internal/pushdown -run TestSplitter_ConcurrentMultiLegQueriesNeverDeadlockPool -count=3`
Expected: at least one FAIL with `acquire conn timeout`. Record the outcome in the commit message. If it never fails, raise C to 10 and the pool to 44 (keep `C·2+1 ≤ pool/2`) and repeat. Revert the temporary change.

- [ ] **Step 3: Run with the real budget, also on the cluster**

Run: `go test -race ./internal/pushdown -run TestSplitter_ConcurrentMultiLegQueriesNeverDeadlockPool -count=5` and `make test-cluster`
Expected: PASS every time.

- [ ] **Step 4: Commit**

```bash
git add internal/pushdown/split_pool_test.go
git commit -m "test: split legs never deadlock a small connection pool"
```

---

### Task 6: Route Tier B through the splitter in `pushdown.Engine`

**Files:**
- Modify: `internal/pushdown/engine.go` (struct `Engine`, `Query`, `execOne`)
- Test: `internal/pushdown/engine_test.go`

**Interfaces:**
- Consumes: `Splitter.Query`, `withRequestSlots` (Tasks 3–4).
- Produces: a new field `Engine.Split *Splitter`; nil keeps today's behaviour. `TierB` stays the raw `*logql.QueryEngine`. An unexported `func (e *Engine) tierB(p logql.Params) logql.Query` returns `e.Split.Query(p)` when `Split != nil`, else `e.TierB.Query(p)`. `Engine.Query` uses `tierB` everywhere except the multi-tenant guard, which keeps `e.TierB.Query(p)`. When `Split != nil`, it wraps its result in a `logql.Query` whose `Exec` first does `ctx = withRequestSlots(ctx, e.Split.Limits)`. `execOne`'s fallback uses `e.tierB(q.Params)`.

- [ ] **Step 1: Write the failing tests**

```go
func TestEngine_TierBRangeQuerySplits(t *testing.T) {
	// for mode in {off, on, downstream}: Engine{Split: splitter(iv 1h)}
	// `quantile_over_time(0.9, {app="web"} | logfmt | unwrap dur [5m]) by (app)` over 3h40m
	// want: equal to raw Tier B; Summary.Splits == 4 for off/on
	// (downstream: data equal; the leaf's legs ran, assert via SplitMetrics count == 1)
}

func TestEngine_MultiTenantDoesNotSplit(t *testing.T) {
	// ctx with "t1|t2"; same query → SplitMetrics sample count unchanged (0)
}

func TestEngine_BinOpLeavesRunConcurrently(t *testing.T) {
	// Pins Goal 2 (Loki's newBinOpStepEvaluator loads both legs in parallel).
	// Mode on, TierA = barrierLeaf: Eligible → true; Exec signals entered, then waits
	// until two calls have entered (or 10 s → returns an error), returns empty Matrix.
	// `count_over_time({a="1"}[1m]) / count_over_time({a="2"}[1m])`, range query
	// want: NoError (both leaves were in Exec at once)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pushdown -run 'TestEngine_TierBRangeQuerySplits|TestEngine_MultiTenantDoesNotSplit|TestEngine_BinOpLeavesRunConcurrently' -count=1`
Expected: the first two FAIL to compile (`unknown field Split`). `TestEngine_BinOpLeavesRunConcurrently` is expected to already pass once it compiles. It pins Loki behaviour; if it fails, stop and report, because Goal 2 then needs the downstreamer change after all.

- [ ] **Step 3: Implement the routing in `engine.go`**

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/pushdown -count=1`
Expected: PASS (existing engine and property tests unchanged)

- [ ] **Step 5: Commit**

```bash
git add internal/pushdown/engine.go internal/pushdown/engine_test.go
git commit -m "feat: route engine-run queries through the time splitter"
```

---

### Task 7: Server wiring, metric, docs and changelog

**Files:**
- Modify: `internal/server/server.go` (package var next to `deleteClientMetrics`; `newAPI` gets the splitter)
- Test: `internal/server/server_test.go` (`TestRun_Metrics`)
- Modify: `CHANGELOG.md`, `docs/divergences.md` (`## Query`), `docs/operations.md` (`### Query`)

**Interfaces:**
- Consumes: `pushdown.Splitter`, `pushdown.NewSplitMetrics`, `pushdown.NewConnBudget`.
- Produces: `var splitMetrics = pushdown.NewSplitMetrics(prometheus.DefaultRegisterer)` with a comment in the style of `deleteClientMetrics`'s. `newAPI` sets `Split: &pushdown.Splitter{Engine: tierB, Limits: limits, Budget: pushdown.NewConnBudget(cfg.ClickHouse.MaxOpenConns), Metrics: splitMetrics}`, with the same `tierB` value as `Engine.TierB`. The budget is built once per `Run`.

- [ ] **Step 1: Write the failing test**

Add `loki_query_frontend_partitions_bucket{le="1"}` to the `want` list in `TestRun_Metrics`. Before reading `/metrics`, issue one range query (`count_over_time({app="x"} | logfmt [1m])`, `start=now-2h`, `end=now`, `step=60`) for the test's tenant, so the histogram has a sample.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/server -run TestRun_Metrics -count=1`
Expected: FAIL (`does not contain "loki_query_frontend_partitions_bucket`).

- [ ] **Step 3: Wire it in `server.go`**

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/server -count=1`
Expected: PASS

- [ ] **Step 5: Docs and changelog**

- `CHANGELOG.md` under `## [Unreleased]`, add a `### Added` section: "Metric range queries that run in lokihouse's own engine (`| unwrap`, `quantile_over_time`, parsers) are split by `split_queries_by_interval` (default `1h`) and the pieces are evaluated in parallel, up to `max_query_parallelism` per request and half of `clickhouse.max_open_conns` across requests; `split_queries_by_interval: 0` turns it off. The histogram `loki_query_frontend_partitions` counts the pieces." Do not claim parallel leaves (already true, Loki's evaluator).
- `docs/divergences.md` `## Query`: the step-grid bullet. Loki's frontend aligns `start`/`end` to `step` before splitting (`alignStartEnd`), so split Loki returns shifted timestamps for unaligned `start`. lokihouse keeps the request's grid, so split and unsplit answers agree.
- `docs/operations.md` `### Query`: rows `split_queries_by_interval` (where: lokihouse engine; behaviour: legs at wall-clock multiples, never finer than the largest `[range]`, `0` disables, instant/log/multi-tenant never split) and `max_query_parallelism` (legs of one request at once; extra legs run on the request's own goroutine). Add a paragraph: pieces reserve connections from a budget of half of `max_open_conns` and never wait for it. Under load, queries degrade to sequential evaluation instead of queueing.

- [ ] **Step 6: `make check`, then commit**

Run: `make check`
Expected: PASS, coverage ≥ 85 %.

```bash
git add internal/server/server.go internal/server/server_test.go CHANGELOG.md docs/divergences.md docs/operations.md
git commit -m "feat: split engine-run range queries by split_queries_by_interval"
```

---

### Task 8: Oracle coverage for splitting

**Files:**
- Modify: `test/oracle/lokihouse.go` (add `StartLokihouseLimits(t *testing.T, extra, limits string) string`; `StartLokihouse(t, extra)` calls it with `""`; `limits` lines are appended inside `limits_config:`)
- Modify: `test/oracle/oracle_test.go` (`TestOracle` uses `StartLokihouseLimits(t, rulerYAML(...), "  split_queries_by_interval: 15m\n")`; new kind `unaligned`: range query with `start+17s`, `end`, `step=45s`)
- Create: `test/oracle/corpus/split.logql`

- [ ] **Step 1: Add the corpus**

```
# Splitting: run with split_queries_by_interval 15m in TestOracle, so each
# range query below crosses ≥ 6 boundaries; unaligned: shifts start by 17s, step 45s.
unaligned:quantile_over_time(0.95, {app="api"} | logfmt | unwrap bytes [5m]) by (app)
unaligned:sum by (app) (count_over_time({app=~".+"} | logfmt [5m]))
unaligned:count_over_time({app="web"} | logfmt [5m]) / count_over_time({app="api"} | logfmt [5m])
unaligned:topk(2, sum by (app) (rate({app=~".+"} | logfmt [1m])))
unaligned:absent_over_time({app="nope"}[5m])
range:max_over_time({app="api"} | logfmt | unwrap bytes [20m] offset 7m) by (app)
range:avg_over_time({app="web"} | logfmt | unwrap bytes [30m])
```

Before relying on these, check that `bytes` and the logfmt fields exist in `test/oracle/fixture.go`, and adjust the label/field names to the fixture's.

- [ ] **Step 2: Implement the `unaligned` kind and `StartLokihouseLimits`**

- [ ] **Step 3: Run the oracle in all modes and on the cluster**

Run: `make build && make oracle-all && make oracle-cluster`
Expected: PASS in all three modes and on the cluster. A failure here is an exactness bug: fix it in the code (Tasks 1–4), not in the corpus.

- [ ] **Step 4: Commit**

```bash
git add test/oracle/lokihouse.go test/oracle/oracle_test.go test/oracle/corpus/split.logql
git commit -m "test: oracle compares split range queries with unsplit Loki"
```

---

### Task 9: Benchmark span, acceptance and soak

**Files:**
- Modify: `test/oracle/bench_test.go` (`LOKIHOUSE_BENCH_SPAN`, parsed with `time.ParseDuration`, default `1h`; `benchData` takes `span time.Duration` and spreads lines over `[end−span, end)`; query `start = end − span`; the header line names the span)
- Modify: `docs/benchmark.md`

- [ ] **Step 1: Make Loki keep a long span queryable**

When `span > 2h`, append `ingester: {max_chunk_age: 24h, chunk_idle_period: 24h}` to both bench Lokis' config (`ruler()` extra). The oracle notes that flushed chunks drop out of Loki's results after about 2 h 40 min. The bench's `Compare` calls are the check: a 6 h run must pass them.

- [ ] **Step 2: Run the default and 6 h benchmarks**

Run: `make build && make bench` and `LOKIHOUSE_BENCH_SPAN=6h LOKIHOUSE_BENCH_OUT=/tmp/bench-6h.md make bench`
Expected: all `Compare` calls pass. At the default span, the `unwrap quantile` lokihouse median ≤ the `Loki` (sharded) median. If it is not, stop and report the numbers; do not tune the test.

- [ ] **Step 3: Record results in `docs/benchmark.md`**

Add the default-span and 6 h tables with date and host notes, following the file's existing sections, and one sentence on how the gain scales with range.

- [ ] **Step 4: Soak**

Run: `make soak`
Expected: PASS; goroutine and heap profiles flat (as the target reports).

- [ ] **Step 5: Commit**

```bash
git add test/oracle/bench_test.go docs/benchmark.md
git commit -m "test: bench over a configurable data span; record split-engine results"
```
