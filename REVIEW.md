# Review: `feat/ch-query-review` and project-wide pass (2026-10-23)

Reviewer: Mistral Vibe (second pair of eyes)
Branch: `feat/ch-query-review`, 19 commits ahead of `main` (18 real commits + 1 merge).
Loki pinned at `bc1962860a1607eccb7e0c8ee206970b292b9e80` (`Makefile` `LOKI_COMMIT`, unchanged).

## 1. Scope and method

Two passes:

1. **Branch vs. plans.** The branch implements the spec
   `docs/superpowers/specs/2026-10-23-clickhouse-query-review.md` (9 required
   changes) through four plans: `2026-10-23-ch-query-path-fixes.md`,
   `2026-10-23-ch-time-windowed-reads.md`, `2026-10-23-ch-tiera-aggregation.md`,
   `2026-10-23-ch-label-filter-pushdown.md`. Each plan's steps, its "Review
   Focus" list and its test list were checked against the commits and the code.
2. **General project review** with extra attention to security (AGENTS.md "Safe
   Go" and "Untrusted input" rules) and to Loki compatibility (AGENTS.md "Loki
   compatibility" rules).

Method: read the spec, the four plans and every commit diff; read the pinned
Loki source where behaviour could diverge (`logql` parser, `LabelsBuilder`,
sample extractor); then run every gate in AGENTS.md "Before you push".

## 2. Verification gates

All gates from AGENTS.md were run against a real local ClickHouse
(`clickhouse-server:26.9` on `localhost:9000`).

| Gate | Result |
|---|---|
| `go build ./...`, `go vet ./...` | pass |
| `gofmt` / `goimports` | pass (only vendored `test/oracle/.cache/loki-src` flagged, not project code) |
| `golangci-lint` v2.14.0 `run ./...` | 0 issues |
| `go test ./... -race` (full suite) | pass |
| `scripts/govulncheck.sh` | pass (`GO-2026-5115` allowlisted with documented reason + proving test `TestRules_PathTraversalRejected`) |
| `make cover` (total coverage) | **87.2 %** (≥ 85 % required; up from 86.3 % before this branch) |
| Oracle vs. real Loki, `LOKIHOUSE_ORACLE_PUSHDOWN=off` | pass (~47 s) |
| Oracle vs. real Loki, `=downstream` | pass |
| Oracle vs. real Loki, `=default on` | pass (~107 s), incl. new `test/oracle/corpus/pushdown.logql` entries |
| `make test-cluster` equivalent (2 shards × 2 replicas, `LOKIHOUSE_TEST_CH_CLUSTER=lokihouse`) | pass (chstore 247 s, pushdown 70 s, server/cmd/rest ok); cluster torn down afterwards |
| `make soak` (`-tags oracle,soak`, heap/goroutine profiles) | pass, 604 s, profiles flat |

Plan-specific tests all pass with `-race`: `TestPlanWindows`,
`TestPlanWindows_Invariants` (500 random cases), `TestManyStreams_AllPathsWork`,
`TestStreamSet_*`, `TestSelectSamples_SameResultForAnyWindowSize`,
`TestSelectLogs_SameResultForAnyWindowSize`, `TestSelectLogs_OpensWindowsLazily`,
`TestMigrateSchema_*`, `TestMigrate_RefusesV2`, `TestLineASCIIEscapeFlag`,
`TestMetadataFilter*`, `TestParsedFilter*`, `TestTierA_SumByNarrowsMetadata`,
`TestTierA_SumLeafFallback`, `TestTierA_MetadataFilter`, `TestMap_SumLeaves`.

**Verdict: every gate passes.** The branch is pushable per AGENTS.md.

## 3. Branch vs. plans: conformance

All 9 required changes of the spec are implemented, each by the expected
commits, with tests. Mapping:

| # | Spec requirement | Commits | Key tests | Verdict |
|---|---|---|---|---|
| 1 | Stream ID sets of any size in every stream-filtering query | `e651453` (+ `27dffde` refactor) | `TestManyStreams_AllPathsWork`, `TestStreamSet_*`; oracle `streams.logql` | conformant — all 5 stream-ID query paths use `streamIDsIn` with empty-set guards |
| 2 | Case-insensitive line filters push `line ILIKE` under the k/K/s/S fold rule | `71e4c50` | fold-case table tests; oracle | conformant (see F4: one plan-table row is imprecise, code is right) |
| 3 | Pushdown tests prove granule skipping (rows read), not only EXPLAIN | `fdbaf18` | rows-read assertions in pushdown tests | conformant |
| 4 | `RegexExact` regex stream matchers also evaluated in SQL | `91c9b25` | regex matcher tests; oracle | conformant — Go still re-checks |
| 5 | Tier B reads in time windows sized from `stream_stats_hourly` | `57869ed`, `0d1c271`, `3905ad5`, `27dffde` | `TestPlanWindows*`, `TestSelect*_SameResultForAnyWindowSize`, `TestSelectLogs_OpensWindowsLazily` | conformant; window coverage/contiguity verified by `TestPlanWindows_Invariants` |
| 6 | Tier A: `line` only for byte aggs; `do_not_merge_across_partitions_select_final = 1`; `sum by` groups only by visible metadata keys | `7547bea`, `fa0eb0d` | `TestTierA_SumByNarrowsMetadata`, `TestTierA_SumLeafFallback`, `TestMap_SumLeaves` | conformant, with one test-coverage gap (F1) |
| 7 | Schema v3: `sm_keys`, `sm_vals`, `line_ascii_escape`; offline `-schema.migrate` | `7ced5a6`, `5cb4964` | `TestMigrateSchema_*`, `TestMigrate_RefusesV2`, `TestLineASCIIEscapeFlag`; `testdata/schema_v2.sql` byte-identical to pre-branch `schema.sql` | conformant; DDL in `schema.sql` == `migrate_v3.go` (test-enforced shape equality) |
| 8 | Pre-parser label filters push `structured_metadata[k] = v`; exact keeps Tier A | `70e4b59` | `TestMetadataFilter*`, `TestTierA_MetadataFilter` | conformant — uses Loki's `bloomv1.ExtractTestableLabelMatchers` |
| 9 | Post-parser equality after `\| json` / `\| logfmt` pushes `LIKE` / `mapContainsKey` / `line_ascii_escape` OR | `b4a6874`, `3d3b22c` | `TestParsedFilter*`, `TestLineASCIIEscapeFlag` | conformant; three-way OR granule-skipping measured and test-proven |

Style/fix commits `9f1f0ab` (gofmt), `4e149ec` (gocritic), `8933bca` (do not
push label filters on names renamed by a stream-label clash — a real bug found
by this branch's own review focus) are consistent with AGENTS.md.

Deep checks against pinned Loki source (all verified, no divergence):

- `_extracted` semantics: Loki's `LabelsBuilder.Add` renames clashing added
  labels to `<name>_extracted`; the JSON parser renames a parsed key when the
  base has it or it is present in structured metadata. lokihouse stores
  metadata as pushed (no write-time rename), so `clashNames`/`mdKeysFor` are
  correct; the `8933bca` fix closes the remaining pushdown hole.
- `!=` or-chain AND-pushdown: Loki's parser nests `!=`/`!~` or-chains as AND
  (De Morgan); `Or` chains exist only for `|=`, `|~`, `|>`. lokihouse's
  AND-of-`position()=0` pushdown is therefore right.
- `tiera.go` `seriesLabels` mirrors Loki's sample extractor
  (`NewBaseLabelsBuilder().ForLabels(...)` + `Add(StructuredMetadataLabel, md)`);
  `eligibleRange` rejects grouped range aggregations, so grouping is always
  nil — equivalent to the extractor.
- `line_ascii_escape` regex verified byte-exact against `schema.sql`: matches
  exactly `\u00XX` of `0-9A-Za-z`; `\u003c` (`<`) and `\u0026` (`&`) correctly
  excluded (Go's common escapes do not set the flag).
- Migration: ClickHouse computes a missing MATERIALIZED column **on read** for
  old parts — `TestMigrateSchema_OldRowsHaveTheFlag` proves the flag is TRUE
  for pre-migration rows containing `\u0065rror`. The implementation is safe;
  see F3 for the plan text that mis-states this.

## 4. Findings

Severity: **medium** = real gap, fix before merge or immediately after;
**low** = harden or document; **info** = note for the record.

### F1 (medium, test-coverage gap): the plan's Review Focus clash case is not actually seeded

Plan `2026-10-23-ch-tiera-aggregation.md` (Review Focus) requires testing
`sum by (level)` where `level` is both a stream label and a structured-metadata
key, and `sum by (level_extracted)` fetching the base metadata key. In
`internal/pushdown/tiera_test.go:318` `seedSumByData` builds the
`{app="a", level="s"}` stream with `StructuredMetadata: md[1:]`
(`tiera_test.go:326`) — i.e. **without** a `level` metadata key (only
`trace_id`). The clash the plan asks to exercise (stream label `level` +
metadata key `level`) therefore never occurs in the seeded data, and
`sum by (level)` / `sum by (level_extracted)` in `sumByQueries` run against
data where the two names never collide.

I verified the implementation handles the real clash correctly with a
temporary probe test (stream `{app="a", level="s"}` **with** `level` metadata;
7 queries incl. `sum by (level)`, `sum by (level_extracted)`,
`sum by (app, level_extracted)` and bytes variants): Tier A == Tier B results,
`TierALeaves` incremented, series show
`{app="a", level="s", level_extracted="warn", …}`. The probe was deleted after
passing; it is not in the tree.

**Recommendation:** extend `seedSumByData` — drop the `md[1:]` so the clash
stream carries `level` metadata too (one-line change), making the existing
`sumByQueries` actually cover the Review Focus case.

### F2 (low): `planWindows` panics on `target == 0`

`internal/chstore/windows.go:54` computes `n := int64((count + target - 1) / target)`.
With `target == 0` this divides by zero and panics. The only caller
(`windows.go:111-113`) substitutes `defaultWindowRows` when
`q.WindowRows == 0`, so no request path can trigger it today, and
`TestPlanWindows_Invariants` covers 500 random cases. Still, the function's
safety depends on a guard one call site away.

**Recommendation:** either clamp inside `planWindows` (treat 0 as
`defaultWindowRows`) or document the precondition on the function.

### F3 (low, info): plan text mis-states pre-migration `line_ascii_escape` reads

The plan for schema v3 claims the flag "reads as false" for rows written
before the migration. That is wrong: ClickHouse evaluates a missing
MATERIALIZED column on read for old parts, and
`TestMigrateSchema_OldRowsHaveTheFlag` proves the flag is TRUE for
pre-migration rows containing `\u0065rror`. The implementation is safe (the
flag is conservative-correct for old rows); CHANGELOG's "Existing data is not
re-indexed" is accurate for the text indexes only.

**Recommendation:** correct the plan document; no code change.

### F4 (low, info): plan fold-case table row contradicts its own k/s rule

The plan's fold-case table lists `(?i)connection refused` → two ILIKEs, but
`refused` contains `s`, so by the plan's own rule ("no `k`, `K`, `s` or `S`")
the fold is unsafe and the correct pushdown is a single `ILIKE '%connection%'`
(and `(?i)connection failed` is the two-ILIKE case). The implementation and
its tests follow the rule; only the plan table is imprecise.

**Recommendation:** fix the plan table; no code change.

### F5 (info): one oracle corpus entry is vacuous

`test/oracle/corpus/pushdown.logql` contains
`range:count_over_time({app="web"} | level="error" [5m])`, but the corpus
fixture has no structured metadata, so both sides are empty (the entry's own
comment admits this). It adds no signal. Real coverage of metadata filters in
Tier A is `TestTierA_MetadataFilter` and the `TestMetadataFilter*` unit tests.

**Recommendation:** optional — give the fixture a `level` metadata key so the
entry is meaningful, or drop it.

### F6 (info): `TierALeaves` counts attempts, not successes

`internal/pushdown/engine.go:128` increments `TierALeaves` whenever
`TierA.Exec` does not return `ErrFallback` — including when it returns a real
error. It is a test-only counter (documented as such at `engine.go:27`), used
for coverage assertions, so the semantics are harmless but slightly misleading.

**Recommendation:** optional — increment only on `err == nil`, or rename to
`TierAAttempts`.

### F7 (info, pre-existing): mutable package state `logChunkSize`

`internal/chstore/keyset.go:11` declares `var logChunkSize = 1000`, mutated by
tests through `SetLogChunkSize` (`internal/chstore/export_test.go:6`).
AGENTS.md allows package-level mutable state only for Prometheus collectors
and values Loki forces on us, "document those where they are declared" — this
one is neither and is undocumented. Pre-existing, not introduced by this branch.

**Recommendation:** add a one-line comment at the declaration naming the test
mutation, or make the chunk size a field on the querier.

### F8 (info, pre-existing): client-side binding size limit and error leakage

- clickhouse-go binds `?` parameters client-side, so bound filter values are
  interpolated into the query text and count toward ClickHouse
  `max_query_size` (256 KiB, per the spec's own measurement). A single filter
  value ≳ 256 KiB fails the query where Loki would answer it. The branch's new
  fragments add more such sites, but the mechanism is pre-existing (it is why
  stream ID sets moved to the `lh_stream_ids` external table in `e651453`).
- `internal/ingest/handler.go:73` answers `http.Error(w, err.Error(), 500)`,
  leaking ClickHouse error text (host, user, internal paths) to clients.
  Several other handlers do the same for 4xx/5xx paths.

**Recommendation:** optional hardening — log the detailed error server-side and
return a generic message; consider server-side binding or chunking for
oversized values. Both pre-date this branch; neither is a regression.

### F9 (info): `MetadataFilterSQL` and `ParsedFilterSQL` duplicate the stream-label set construction

`MetadataFilterSQL` (`internal/chstore/labelfilter.go:23-26`) and
`ParsedFilterSQL` (`internal/chstore/labelfilter.go:113-116`) each build the
identical set of stream label names:

```go
streamLabels := map[string]struct{}{}
for _, s := range streams {
	s.Labels.Range(func(l labels.Label) { streamLabels[l.Name] = struct{}{} })
}
```

Both are called together at the single production call site
(`internal/chstore/querier.go:73`) with the same `streams` slice, so the set is
built twice per query resolution. The set is consumed by the already-shared
`clashNames`/`clashesWithStream` logic (`labelfilter.go:41-57`); only its
construction is duplicated. The other `Labels.Range` loops in the package are
not candidates for the same helper: `metadata.go:36` builds a name set or a
per-name value set depending on `req.Values`, `detected.go:162` builds a
name→values map, `stats.go:96` aggregates volumes.

**Recommendation:** extract an unexported
`streamLabelNames(streams []Stream) map[string]struct{}` helper in
`labelfilter.go` used by both functions — it keeps the exported signatures and
the tests (`labelfilter_test.go` calls both with explicit `streams`) unchanged.
Optionally, compute the set once at the `querier.go:73` call site and pass it
in, saving one pass per query, at the cost of changing both signatures and
their tests. No behaviour change either way.

## 5. Security review

**Clean.** Checked against AGENTS.md "Safe Go" and "Untrusted input":

- **No SQL injection.** All user-controlled values (tenant, label names and
  values, line-filter needles, time bounds) are `?`-bound parameters or sent
  via the `lh_stream_ids` external table; clickhouse-go binds client-side with
  proper escaping. Identifiers that reach SQL (database, table, cluster names)
  come from config, not requests, and go through `quoteIdent`/`sqlString`.
  Delete mutations keep batched literal lists per the plan, with the lists
  built from validated stream IDs.
- **No ReDoS.** User regexes run in RE2 (Go `regexp`), which has no
  catastrophic backtracking; the `RegexExact` gate is sound (verified against
  Loki's own use of the same predicate).
- **`escapedLike`** escapes `\`, `%` and `_` before building `LIKE` patterns.
- **Path traversal:** `GO-2026-5115` is allowlisted in govulncheck with a
  documented reason, and `TestRules_PathTraversalRejected` proves ruler rule
  paths are validated; rule files and query strings are validated before use.
- **Secrets:** `password_file` is supported for credentials; no secrets appear
  in the new log lines, error messages or metric labels.
- **Concurrency:** shared state is mutex/atomic-guarded; full suite passes
  with `-race`; soak profiles are flat.
- **Timeouts/contexts:** every I/O path takes `context.Context` first; the one
  new `//nolint:gosec` (`windows.go:54`, integer conversion) carries the linter
  name and reason as AGENTS.md requires.

## 6. Loki compatibility review

**Clean.**

- `docs/divergences.md` is correctly unchanged: the branch adds no observable
  behaviour difference, and the oracle (all 3 modes, whole corpus incl. new
  `pushdown.logql` entries) confirms lokihouse matches real Loki at the pinned
  commit.
- Copied-from-Loki code keeps its `// Copied from grafana/loki@<commit>
  <path>; modifications: <list>` headers (pre-existing copies). New code imports
  Loki (`bloomv1.ExtractTestableLabelMatchers`) instead of copying — preferred
  per AGENTS.md.
- No new Loki config keys, flags, metric names or error messages were
  introduced; where Loki has them, lokihouse uses Loki's exactly.
- `go.mod`/`go.sum` unchanged — no new third-party dependency.
- CHANGELOG `[Unreleased]` covers every user-visible change (Added/Changed/
  Fixed), including the breaking `-schema.migrate` requirement.
- Docs updated consistently: man page, `install/index.md`,
  `debian/README.Debian`, `reference/configuration.md` (grants for the new
  indexes and `-schema.migrate`).
- **Due at release:** the `debian/README.Debian` packaging change needs a
  `debian/changelog` entry. Repo convention is that entries appear in the
  release commit (no `Unreleased` section exists in `debian/changelog`), so
  this is a reminder for the 0.11 release commit, not a defect now.

## 7. General review

- **Structure:** `cmd/lokihouse` stays thin (flags/signals only); `internal/`
  packages keep single responsibilities; no package imports `cmd` or `test`.
- **Tests:** behaviour is tested through public APIs; deterministic (no
  sleeps; `require.Eventually` where polling is needed); nothing writes outside
  `t.TempDir()`.
- One `panic(err)` in the test helper `streamsOf` (`labelfilter_test.go`) —
  acceptable for fixed test strings, not in a request path.
- Untracked `.claude/` directory is pre-existing; left alone.
- Not verified: the mkdocs documentation build (no docs CI gate was run); worth
  a `mkdocs build` before release if one exists.

## 8. Recommendations (prioritized)

1. **F1 (medium):** extend `seedSumByData` in `internal/pushdown/tiera_test.go`
   (drop `md[1:]` at line 326) so the clash cases the plan's Review Focus asks
   for are actually seeded. One line; I verified the implementation passes the
   resulting tests.
2. **F2 (low):** guard `target == 0` inside `planWindows` or document the
   precondition (`internal/chstore/windows.go:54`).
3. **At the 0.11 release commit:** add the `debian/changelog` entry for the
   `README.Debian` packaging change (repo convention).
4. **F7 (info):** document the `logChunkSize` test-mutation exception at
   `internal/chstore/keyset.go:11`, or make it injectable.
5. **F8 (info, pre-existing, optional):** stop leaking ClickHouse error text in
   `internal/ingest/handler.go` (and the other handlers); consider a strategy
   for filter values approaching `max_query_size`.
6. **F3/F4 (info):** correct the two plan-document inaccuracies (docs only).
7. **F5/F6 (info, optional):** make the vacuous oracle corpus entry meaningful
   or drop it; make `TierALeaves` count successes only.
8. **F9 (info, optional):** extract a shared `streamLabelNames` helper in
   `internal/chstore/labelfilter.go` for the duplicated stream-label set
   construction in `MetadataFilterSQL`/`ParsedFilterSQL`.

## 9. Bottom line

The branch faithfully implements all 9 required changes of the
2026-10-23 spec through its four plans, passes every gate in AGENTS.md
(coverage 87.2 %, race, lint, govulncheck, oracle in 3 modes against real
Loki, 2×2 cluster, soak), introduces no security issue and no Loki-compatibility
divergence. The single substantive finding is a test-seeding gap (F1) that
leaves one of the plan's own Review Focus cases unexercised — the code is
verified correct, but the regression test should exist. Everything else is
low/info hardening, documentation and small DRY cleanups.
