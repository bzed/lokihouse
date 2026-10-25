# Delete-Requests Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the per-query ClickHouse round trip for a tenant's delete requests by serving query-time delete filtering from Loki's own delete-requests cache, as Loki's querier does. This closes most of the `log filter` gap to Loki (8.7 ms against 6.7 ms in `make bench`).

**Architecture:** `internal/deletes` gains a constructor that wraps the ClickHouse-backed `deletes.Store` in Loki's `deletion.NewDeleteRequestsClient` (a per-tenant cache, refreshed every 5 minutes) and `deletion.NewPerTenantDeleteRequestsClient` (honours `deletion_mode`). This is exactly the stack Loki builds in `pkg/loki/modules.go` (`deleteRequestsClient`). `internal/server` builds the client once per `Run`, hands it to the querier, Tier A and tail in place of the raw store, and stops it on every exit path. The delete API and the delete processor keep using the store directly.

**Tech Stack:** Go; Loki `pkg/compactor/deletion` at the pinned commit (`bc1962860a16`); ClickHouse 26.9; testify; goleak.

**Spec:** `docs/benchmark.md` (Result: the `log filter` cause), `docs/divergences.md` (Delete requests: "Query-time delete visibility"), and the measured request timeline: per `log filter` request lokihouse runs `delete_requests` (≈2 ms to the next query), then stream resolution (≈2 ms), then the 3 ms read. Each runs only after the previous one returns.

**Not in this plan:** running the delete lookup and the stream resolution concurrently. After this change the lookup is an in-memory map read for every query except a tenant's first, so overlapping it with stream resolution saves nothing measurable. The remaining gap after this plan is stream resolution itself (≈2 ms on `streams`), which is a separate investigation.

## Global Constraints

- Reuse Loki's code: `deletion.NewDeleteRequestsClient`, `deletion.NewPerTenantDeleteRequestsClient`, `deletion.NewDeleteRequestClientMetrics`. Do not re-implement the cache.
- Metric names are Loki's: `loki_delete_request_lookups_total`, `loki_delete_request_lookups_failed_total`, registered once per process on `prometheus.DefaultRegisterer`, as in `pkg/loki/loki.go:508`.
- The cache duration is Loki's default, 5 minutes. No new configuration key (Loki has none).
- `clientType` passed to Loki is `"querier"`.
- No package-level mutable state except the Prometheus metrics value. Document it where it is declared.
- Every goroutine has an owner that stops it: `Stop()` runs on every `server.Run` exit path, before ClickHouse is closed.
- Tests: red first; no sleeps to wait for state; `require.Eventually` with a generous timeout where polling is needed; `make check` (coverage ≥ 85 %), `make test-cluster` and `make oracle-all` pass.
- User-visible change: one `CHANGELOG.md` line under `## [Unreleased]`, in the commit that changes behaviour.

## Review Focus

1. **`deletion_mode: disabled` for a tenant that still has received requests:** queries must not filter (Loki's per-tenant client returns no requests). Pinned in Task 1, `TestQueryClient_DisabledTenantFiltersNothing`.
2. **A failed first lookup** (ClickHouse unreachable): the query must fail with the error, not cache an empty list that silently shows deleted lines for 5 minutes. Pinned in Task 1, `TestQueryClient_ErrorIsNotCached`.
3. **Concurrent first queries of one tenant:** no data race and the same result. Pinned in Task 1, `TestQueryClient_ConcurrentLookups` (run with `-race`).
4. **Every `server.Run` exit path stops the refresh goroutine:** listen error, ruler start error, normal shutdown. Pinned by goleak in `internal/server` (`TestRun_ListenError`, `TestRun_PushQueryShutdown`), which already run under `goleak.VerifyTestMain`.
5. **`server.Run` twice in one process (tests):** the Loki metrics must not be registered twice (promauto panics). Pinned in Task 2 by the existing tests that start several servers per process, plus `TestRun_DeleteLookupsAreCached`.

---

### Task 1: `deletes.NewQueryClient`, Loki's cached per-tenant delete client over the store

**Files:**
- Create: `internal/deletes/client.go`
- Test: `internal/deletes/client_test.go`

**Interfaces:**
- Consumes: `(*deletes.Store).GetAllDeleteRequestsForUser(ctx, userID string, forQuerytimeFiltering bool, tr *deletion.TimeRange) ([]deletionproto.DeleteRequest, error)` (exists).
- Produces:
  - `func NewQueryClient(s *Store, l deletion.Limits, m *deletion.DeleteRequestClientMetrics, opts ...deletion.DeleteRequestsStoreOption) (deletion.DeleteRequestsClient, error)`: returns `deletion.NewPerTenantDeleteRequestsClient(cached, l)`, where `cached` is `deletion.NewDeleteRequestsClient(compactorClient{s}, m, "querier", opts...)`. The caller must call `Stop()`.
  - Unexported `type compactorClient struct{ *Store }` satisfying `deletion.CompactorClient`: `GetCacheGenerationNumber(ctx, userID) (string, error)` returns `"", nil` (lokihouse has no results cache to invalidate); `Name() string` returns `"lokihouse"`; `Stop()` does nothing (the store owns no goroutine). Doc comment on each method says why.

The tests use `newStore(t)` from `store_test.go` and a fake limits type in the test file:

```go
type modeLimits map[string]string // tenant -> deletion_mode; "" means filter-and-delete
func (m modeLimits) DeletionMode(u string) string { if v := m[u]; v != "" { return v }; return "filter-and-delete" }
func (modeLimits) RetentionPeriod(string) time.Duration { return 0 }
func (modeLimits) StreamRetention(string) []validation.StreamRetention { return nil }
```

Each test builds metrics with `deletion.NewDeleteRequestClientMetrics(prometheus.NewRegistry())` and calls `t.Cleanup(c.Stop)`.

- [ ] **Step 1: Write the failing tests**

```go
// The first lookup of a tenant reads the store; later ones are served from
// the cache until it refreshes, so a request added in between is not seen.
func TestQueryClient_CachesPerTenant(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, err := s.Add(ctx, "t1", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)
	c, err := deletes.NewQueryClient(s, modeLimits{}, deletion.NewDeleteRequestClientMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(c.Stop)

	got, err := c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	_, err = s.Add(ctx, "t1", `{app="y"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)
	got, err = c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1, "served from the cache")
}

// With a short refresh interval the cache picks up the new request.
func TestQueryClient_Refreshes(t *testing.T)
// as above, with opts deletion.WithRequestClientCacheDuration(100*time.Millisecond);
// after the second Add: require.Eventually(len(got) == 2, 10*time.Second, 50*time.Millisecond)

func TestQueryClient_DisabledTenantFiltersNothing(t *testing.T)
// Add a request for "t1"; limits modeLimits{"t1": "disabled"};
// GetAllDeleteRequestsForUser → require.NoError, require.Empty

func TestQueryClient_ErrorIsNotCached(t *testing.T)
// conn, db := chtest.New(t); s := &deletes.Store{Conn: conn, Now: ...}; Add one request for "t1";
// bad, _ := clickhouse.Open(chtest.Options(db)); require.NoError(t, bad.Close()); s.Conn = bad
// first GetAllDeleteRequestsForUser → require.Error
// s.Conn = conn; second call → require.NoError, require.Len(got, 1)

func TestQueryClient_ConcurrentLookups(t *testing.T)
// 16 goroutines (sync.WaitGroup) each call GetAllDeleteRequestsForUser(ctx, "t1", true, nil);
// every call returns 1 request and no error; the test runs under -race via make check.
```

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -count=1 -run TestQueryClient ./internal/deletes/`
Expected: build failure, `undefined: deletes.NewQueryClient`.

- [ ] **Step 3: Implement `NewQueryClient` and `compactorClient` in `internal/deletes/client.go`**

The package doc of `client.go` names the source: the stack mirrors `deleteRequestsClient` in `grafana/loki@bc1962860a16 pkg/loki/modules.go`. Nothing is copied, so no `// Copied from` header.

- [ ] **Step 4: Run the tests and see them pass, with the race detector**

Run: `go test -race -count=1 -run TestQueryClient ./internal/deletes/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/deletes/client.go internal/deletes/client_test.go
git commit -m "feat: deletes.NewQueryClient wraps the store in Loki's delete-requests cache"
```

### Task 2: Serve query-time delete filtering from the cached client

**Files:**
- Modify: `internal/server/server.go` (`Run` around lines 79–115, `newAPI` lines 213–258)
- Test: `internal/server/server_test.go`
- Modify: `CHANGELOG.md`, `docs/divergences.md:55`

**Interfaces:**
- Consumes: `deletes.NewQueryClient` (Task 1).
- Produces:
  - Package-level `var deleteClientMetrics = deletion.NewDeleteRequestClientMetrics(prometheus.DefaultRegisterer)` in `server.go`, commented as the Prometheus-collector exception in AGENTS.md. It also keeps a second `Run` in one process from registering the metrics twice.
  - `func newAPI(cfg *config.Config, conn driver.Conn, store *deletes.Store, dels deletion.DeleteRequestsClient, limits *validation.Overrides, logger log.Logger, rulerReg prometheus.Registerer) (*api.API, *chstore.Querier)`. `Run` creates `store` and `dels` (with `limits`) before calling it. `dels` replaces `store` as `chstore.Querier.Deletes`, `pushdown.TierA.Deletes` and the `deleteGetter` of `tail.NewQuerier`. `deletes.Handler` and `deletes.Processor` keep `store`.
  - `Run` calls `dels.Stop()` inside `stopLoops`, which every exit path already calls before `closeConn`.

- [ ] **Step 1: Write the failing test**

```go
// Two queries of one tenant look its delete requests up in ClickHouse once:
// the second is served from Loki's delete-requests cache.
func TestRun_DeleteLookupsAreCached(t *testing.T) {
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())
	tenant := uniqueTenant() // the pattern of TestRun_Metrics: counters are process-global
	require.Equal(t, http.StatusNoContent, push(t, r.addr, tenant, pushBody(time.Now(), "hello")))
	before := counterValue(t, "loki_delete_request_lookups_total")
	queryRange(t, r.addr, tenant, `{app="a"}`) // 200, any body
	queryRange(t, r.addr, tenant, `{app="a"}`)
	require.Equal(t, before+1, counterValue(t, "loki_delete_request_lookups_total"))
}
```

`counterValue` gathers `prometheus.DefaultGatherer` like `inflightPosts` and sums the named counter. `queryRange` issues a `GET /loki/api/v1/query_range` with `X-Scope-OrgID` over the last 5 minutes and requires status 200. `uniqueTenant` factors out the random-tenant lines of `TestRun_Metrics`, and that test uses it too.

- [ ] **Step 2: Run the test and see it fail**

Run: `go test -count=1 -run TestRun_DeleteLookupsAreCached ./internal/server/`
Expected: FAIL. The counter is not registered (0 → 0, `before+1` expected).

- [ ] **Step 3: Wire the client as described in Interfaces**

- [ ] **Step 4: Run the server package and the delete-filtering suites**

Run: `go test -race -count=1 ./internal/server/ ./internal/deletes/ ./internal/chstore/ ./internal/pushdown/`
Expected: `ok` for all four. goleak reports no `deleteRequestsClient.updateLoop` goroutine.

- [ ] **Step 5: Update the docs in the same commit**

- `CHANGELOG.md`, `## [Unreleased]` → `### Changed`: "Queries look up a tenant's pending delete requests through Loki's delete-requests cache, refreshed every 5 minutes, instead of querying ClickHouse every time: as in Loki, a new or cancelled delete request can take up to 5 minutes to change query results."
- `docs/divergences.md:55`: delete the "Query-time delete visibility" entry. lokihouse now behaves like Loki, so it is no longer a divergence. Keep the oracle's workaround described where `test/oracle/phase3_test.go:291-296` already documents it.

- [ ] **Step 6: Run the gates**

Run: `make check && make test-cluster && make oracle-all && make oracle-cluster`
Expected: all pass, coverage ≥ 85 %. `deletesOracle` still passes because it never queries a tenant before deleting.

- [ ] **Step 7: Commit**

```bash
git add internal/server/server.go internal/server/server_test.go CHANGELOG.md docs/divergences.md
git commit -m "feat: serve query-time delete filtering from Loki's delete-requests cache"
```

### Task 3: Measure and record the effect

**Files:**
- Modify: `docs/benchmark.md` (Result section)

- [ ] **Step 1: Run the benchmark twice**

Run: `make bench` twice, with `LOKIHOUSE_LOKI_BIN` pointing at the cached Loki binary.
Expected: `log filter` lokihouse median ≈ 6.5–7 ms (from 8.7 ms), ratio ≈ 1.0. No other query slower than the recorded table by more than its p90 spread. Both runs pass every `Compare`.

- [ ] **Step 2: Update `docs/benchmark.md`**

Replace the table with the second run. In the "slower on" list, rewrite the `log filter` bullet: the delete-request round trip is gone (served from Loki's cache), and what remains is stream resolution on `streams` (≈2 ms) before the read. If the ratio is ≤ 1.05, move `log filter` out of the "slower" list.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmark.md
git commit -m "docs: benchmark after caching delete requests"
```
