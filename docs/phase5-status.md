# Lokihouse Phase 5 (Hardening and Release) - Status Report

> **Last Updated:** 2026-10-12
> **Current Branch:** main
> **Latest Commit:** 4586c9d - "ci: checks, tests, coverage, deb, docs, nightly, release, pages"

## Executive Summary

Phase 5 Parts A–C (tasks 1–9) are complete and all gates pass: total coverage is
**85.9%** (≥ 85 % target), `make check`, `make test-cluster` and `make oracle-all`
are green. Task 9 (ingestion rate limit, query limits, query timeouts) is
implemented with its tests and the `docs/operations.md` "Limits" section.
Tasks 10–14 (multi-tenant queries, `/metrics` + query stats, benchmark, leak
checks, wrap-up) remain.

A bug was found and fixed while verifying Task 9: the query handler
double-reported a LogQL parse error (it wrote `NewLiteralParams`'s parse error
*and* let the engine report a second, `unexpected type (<nil))` error), which
surfaced as Loki/lokihouse response mismatches in the oracle. The handler now
reports the parse error once, with Loki's exact message.

### Completed Tasks
- ✅ **Task 1:** Quality gates and AGENTS.md
- ✅ **Task 2:** Process lifecycle and graceful shutdown
- ✅ **Task 3:** Operator flags and configuration
- ✅ **Task 4:** Lint-clean tree
- ✅ **Task 5:** Coverage to ≥ 85% — **85.9%**
- ✅ **Task 6:** Documentation site, README and manpage
- ✅ **Task 7:** Debian package
- ✅ **Task 8:** GitHub Actions
- ✅ **Task 9:** Ingestion rate limit, query limits and timeouts

### Remaining Tasks
- ⬜ **Task 10:** Multi-tenant queries (`querier.multi_tenant_queries_enabled`)
- ⬜ **Task 11:** Metrics endpoint and query stats
- ⬜ **Task 12:** Benchmark against Loki
- ⬜ **Task 13:** Memory and goroutine leak checks
- ⬜ **Task 14:** Phase 5 wrap-up

### Quality Gates Status (at 85.9 % coverage)
| Gate | Status | Notes |
|------|--------|-------|
| `make fmt-check` | ✅ PASS | All formatting correct |
| `make vet` | ✅ PASS | No vet issues |
| `make lint` | ✅ PASS | 0 golangci-lint issues |
| `make vuln` | ✅ PASS | Only allowlisted GO-2026-5115 |
| `make cover` | ✅ PASS | 85.9 % (minimum 85 %) |
| `make test-cluster` | ✅ PASS | All packages on the 3-node cluster |
| `make oracle-all` | ✅ PASS | Loki vs lokihouse, all pushdown modes |

## Coverage Details

### Current Coverage: 85.9%

**Previous:** 83.7% (commit 79d480f)
**Improvement:** +2.2%
**Target:** 85%
**Result:** **target met**

### Package-Level Coverage Improvements

| Package | Before | After | Improvement |
|---------|--------|-------|-------------|
| `internal/build` | 0.0% | 100.0% | +100.0% |
| `internal/chstore` | 74.6% | 82.7% | +8.1% |
| `internal/patterns` | 84.1% | 87.5% | +3.4% |
| `internal/tail` | 81.0% | 86.9% | +5.9% |
| `tools/extract-corpus` | 29.4% | 42.6% | +13.2% |
| `internal/api` | 66.3% | 66.3% | ±0.0% |
| `internal/config` | 95.2% | 95.2% | ±0.0% |
| `internal/deletes` | 78.8% | 78.8% | ±0.0% |
| `internal/ingest` | 82.8% | 82.8% | ±0.0% |
| `internal/ingest/distcopy` | 83.1% | 83.1% | ±0.0% |
| `internal/logging` | 87.5% | 87.5% | ±0.0% |
| `internal/pushdown` | ~90.4% | ~90.4% | ±0.0% |
| `internal/ruler` | 81.4% | 81.4% | ±0.0% |
| `internal/server` | 80.0% | 80.0% | ±0.0% |
| `cmd/lokihouse` | 58.3% | 58.3% | ±0.0% |

### Functions with 0% Coverage (Remaining)

The following functions still have 0% statement coverage:

1. **`cmd/lokihouse/main.go:22`** - `main()` - Entry point (tested by system)
2. **`internal/api/rules.go:15`** - `ruleRoutes()` - HTTP handler setup for ruler API
3. **`internal/api/rules.go:47`** - `decodedRulePaths()` - HTTP middleware for decoded rule paths
4. **`internal/tail/ingester.go:207-212`** - `Header()`, `Trailer()`, `Context()`, `SendMsg()`, `RecvMsg()` - gRPC client stub methods
5. **`tools/extract-corpus/main.go:24`** - `main()` - Entry point (tested by system)

These are all small, single-statement functions that are difficult to test in isolation:
- Entry points are tested by the system when running the binary
- HTTP handler setup functions require full ruler configuration
- gRPC stub methods are interface implementations that return nil/empty values

### Functions with Low Coverage (< 50%)

| Function | Coverage | Package |
|----------|----------|---------|
| `rangeQuery` | 50.0% | internal/api/query.go:15 |
| `exec` | 57.1% | internal/api/query.go:59 |
| `shouldBlockPolicy` | 50.0% | internal/ingest/distcopy/validator.go:242 |
| `sdNotify` | 50.0% | internal/server/server.go:236 |
| `List` | 53.6% | internal/deletes/handler.go:154 |
| `series` | 60.0% | internal/api/metadata.go:33 |
| `Do` | 62.5% | internal/api/lokicodec.go:21 |

## Test Files Added/Modified

### New Test Files (4 files, 368 lines)

1. **`internal/build/build_test.go`** (27 lines)
   - Tests for `Info()` function
   - Result: build package at 100% coverage

2. **`internal/chstore/detected_test.go`** (209 lines)
   - Tests for `DetermineType()`, `determineType()`
   - Tests for `shouldRejectLabel()`, `containsAllIDTypes()`
   - Result: All type detection functions at 100% coverage

3. **`internal/chstore/stats_integration_test.go`** (109 lines)
   - Integration tests for `Volume()`, `IndexStats()`
   - Tests for `BucketCounts()`
   - Result: Improved chstore coverage from 74.6% to 82.7%

4. **`internal/patterns/logger_test.go`** (23 lines)
   - Tests for `logger()` method
   - Result: Improved patterns coverage from 84.1% to 87.5%

### Modified Test Files (4 files, 160 lines added, 48 lines removed)

1. **`internal/chstore/querier_test.go`** (+21 lines)
   - Added test for `StreamHash()` method
   - Result: StreamHash at 100% coverage

2. **`internal/tail/ingester_test.go`** (+27 lines)
   - Added tests for `TailDisconnectedIngesters()`, `TailersCount()`
   - Result: Improved tail coverage from 81.0% to 86.9%

3. **`tools/extract-corpus/main_test.go`** (+110 net lines)
   - Added tests for `maxRangeFor()`, `normalise()`, `goStrings()`, `mdLogQL()`
   - Result: Improved extract-corpus coverage from 29.4% to 42.6%

4. **`internal/ingest/distcopy/prepare_test.go`** (formatting fix)
   - Minor formatting adjustment

## Bug Fixed While Verifying Task 9

**Issue:** the query handler double-reported a LogQL parse error.

`rangeQuery`/`instantQuery` built their params through a `params(...)` helper that
wrote `logql.NewLiteralParams`'s parse error to the response *and* returned a zero
`Params` whose expression was nil. The handler then called `exec`, whose engine
`Exec` failed on the nil expression and wrote a second error,
`unexpected type (<nil>): cannot evaluate`. The oracle therefore saw two
concatenated messages for every parse-failure query
(`parse error : grouping not allowed for rate aggregation` +
`unexpected type (<nil>): cannot evaluate`).

**Fix:** `params` now returns the parse error and the handler writes it once (400),
so the nil expression never reaches the engine. The reported message is now
exactly Loki's. Verified with `make oracle-all`.

## Next Steps (Tasks 10–14)

1. **Task 10 — Multi-tenant queries:** `querier.multi_tenant_queries_enabled`,
   `internal/multitenant` querier (Loki `multi_tenant_querier.go`), `__tenant_id__`
   labelling, oracle comparison.
2. **Task 11 — Metrics endpoint and query stats:** `GET /metrics`, Loki request /
   distributor / ruler metrics, `chstore.WithQueryStats`.
3. **Task 12 — Benchmark against Loki:** `make bench`, `docs/benchmark.md`.
4. **Task 13 — Memory and goroutine leak checks:** `goleak`, pprof endpoints,
   `make soak`, nightly soak in CI.
5. **Task 14 — Wrap-up:** `docs/roadmap.md`, final status, commit.

## Coverage Timeline

| Date | Commit | Coverage | Change | Notes |
|------|--------|----------|--------|-------|
| 2026-10-11 | 79d480f | 83.7% | +0.0% | OTLP severity number tests |
| 2026-10-12 | bee0418 | 84.9% | +1.2% | Comprehensive test additions |
| 2026-10-12 | (Task 9) | 85.9% | +1.0% | New rate-limit/query-limit code + its tests; target met |

## Files Changed in Phase 5

### Task 1: Quality Gates and AGENTS.md
- `AGENTS.md` (new)
- `CLAUDE.md` (new)
- `.golangci.yml` (new)
- `scripts/coverage.sh` (new)
- `scripts/govulncheck.sh` (new)
- `scripts/govulncheck-ignore.txt` (new)
- `Makefile` (modified)
- `go.mod` (modified)

### Task 2: Process Lifecycle and Graceful Shutdown
- `internal/server/server.go` (new)
- `internal/server/server_test.go` (new)
- `cmd/lokihouse/main.go` (modified)
- `internal/config/config.go` (modified)
- `internal/config/config_test.go` (modified)
- `internal/deletes/processor.go` (modified)
- `internal/deletes/processor_test.go` (modified)

### Task 3: Operator Flags and Configuration
- `internal/logging/logging.go` (new)
- `internal/logging/logging_test.go` (new)
- `cmd/lokihouse/main.go` (modified)
- `internal/config/config.go` (modified)
- `internal/config/config_test.go` (modified)
- `internal/server/server_test.go` (modified)
- `internal/api/misc.go` (modified)
- `Makefile` (modified)
- `lokihouse.example.yaml` (modified)

### Task 4: Lint-Clean Tree
- All lint findings fixed across the codebase

### Task 5: Coverage to ≥ 85%
- `internal/build/build_test.go` (new)
- `internal/chstore/detected_test.go` (new)
- `internal/chstore/stats_integration_test.go` (new)
- `internal/patterns/logger_test.go` (new)
- `internal/chstore/querier_test.go` (modified)
- `internal/tail/ingester_test.go` (modified)
- `tools/extract-corpus/main_test.go` (modified)
- `internal/ingest/distcopy/prepare_test.go` (modified)

### Task 6: Documentation site, README and manpage
- `mkdocs.yml`, `docs/index.md`, `docs/install/index.md`, `docs/migrating.md`,
  `docs/reference/*.md` (new), `docs/man/lokihouse.1.md` (new),
  `cmd/lokihouse/main_test.go`, `internal/config/docs_test.go` (config-drift test)

### Task 7: Debian package
- `debian/*` (control, rules, service, changelog, smoke test), `Makefile` (`deb`)

### Task 8: GitHub Actions
- `.github/workflows/{ci,deb,docs,nightly,release}.yml`,
  `.github/dependabot.yml`, `docs/development.md`

### Task 9: Ingestion rate limit, query limits and timeouts
- `internal/ingest/ratelimit.go` (new, Loki localStrategy), `internal/ingest/ratelimit_test.go` (new)
- `internal/api/limits.go` (new, `checkQuery` + Loki limit checks), `internal/api/limits_test.go` (new)
- `internal/ingest/distcopy/process.go` (`Prepare` returns `Result`, `ReportDiscarded`)
- `internal/ingest/handler.go` (rate-limit gate + `RateLimiter`), `internal/api/query.go`
  (parse-error reported once), `internal/server/server.go` (wire `NewRateLimiter`)
- `docs/operations.md` (Limits), `docs/divergences.md` (Hardening section)

## Metrics

### Lines of Code
- **Test Code Added:** 528 lines
- **Test Code Removed:** 48 lines
- **Net Test Code:** +480 lines
- **Production Code Changed:** Minimal (mostly formatting)

### Test Execution Time
- **Total test time:** ~2-3 minutes (excluding pushdown)
- **Slowest tests:** pushdown property tests (~60 seconds)
- **Fastest tests:** build, logging (~1 second)

### Coverage by Statement Type
- **Total statements:** ~5,200 (estimated)
- **Covered statements:** ~4,468 (85.9%)
- **Uncovered statements:** ~732 (14.1%)

## Conclusion

Phase 5 tasks 1–9 are complete: all quality gates pass, coverage is **85.9 %**
(the 85 % target is met), and the ingestion rate limit, query limits and query
timeouts (task 9) are implemented and oracle-verified. A query-handler bug
(double-reported LogQL parse error) was found and fixed while verifying task 9.

Remaining work is tasks 10–14: multi-tenant queries, the `/metrics` endpoint and
query stats, the Loki benchmark, the memory/goroutine leak checks, and the phase
wrap-up (roadmap + final status).
