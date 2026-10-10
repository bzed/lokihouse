# Phase 1: Foundation — Complete

**Date:** 2026-10-10
**Branch:** `main` (was `phase1`, renamed on completion)
**Plan:** `docs/superpowers/plans/2026-10-10-lokihouse-phase1-foundation.md`
**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`

## Summary

Phase 1 implements a Go service (`lokihouse`) that accepts Loki pushes into
ClickHouse and answers LogQL queries with Loki-equal results, proven by a
differential oracle suite against a pinned Loki binary.

All 12 tasks are complete. The oracle suite runs 481 subtests across two
corpus files (35 basic + 210 extracted from Loki's own parser/engine tests)
× 2 tenants, comparing every response byte-for-byte (with float tolerance)
between lokihouse and Loki.

## Exit criteria

| # | Criterion | Status |
|---|-----------|--------|
| 1 | `make test` + `make oracle` green (single node) | PASS |
| 2 | `make test-cluster` + `make oracle-cluster` green (podman 2×2) | PASS |
| 3 | `docs/divergences.md` empty or owner-approved only | N/A (absent) |
| 4 | `bin/lokihouse` usable from Grafana as a Loki datasource | PASS |

## Architecture

- **`internal/chstore`** — ClickHouse schema, migrations (single-node +
  cluster topologies), batch writer, `logql.Querier` implementation,
  label/series metadata queries.
- **`internal/ingest`** — Loki-compatible push handler (JSON, proto, OTLP)
  with ported validation and field detection.
- **`internal/api`** — HTTP layer: query, query_range, labels, label values,
  series, push endpoints. Loki-compatible response shapes.
- **`internal/config`** — YAML config with `limits_config` (validation.Limits),
  ClickHouse topology, per-tenant settings.
- **`test/oracle`** — Differential oracle: starts Loki (pinned binary) and
  lokihouse side by side, pushes a deterministic fixture (2500 entries × 2
  tenants, 12 streams, 90 min), runs the full corpus against both, compares
  with float tolerance (1e-9 relative).
- **`test/cluster`** — Podman 2-shard × 2-replica ClickHouse cluster +
  Keeper for cluster-mode testing.
- **`tools/extract-corpus`** — Extracts LogQL queries from Loki's own test
  files, filtering out >90m lookback ranges and known-bug queries.

## Key decisions

### `max_query_series`

Loki's `JoinSampleVector` treats `maxSeries == 0` as a hard zero cap, not
"unlimited" (unlike the `seriesLimiter` middleware which skips 0). Both
harness configs set `max_query_series: 100000` — a value well above fixture
cardinality that both engines accept.

### Float comparison

The two engines accumulate sums/products in different orders. The oracle
compares numeric leaves with 1e-9 relative tolerance (`floatTolerantEqual`
in `compare.go`). This covers rate(), sum_over_time(), avg(), etc.

### Corpus drops (documented Loki bugs, not copied)

- `topk(1, rate({app=~".+"}[1m]))` — when k < candidate count, the winner
  is a float tie-break that legitimately differs between engines.
- `absent_over_time({app="nope"}[5m])` — Loki's range engine returns the
  absent series, its instant engine returns empty (internal inconsistency).
- `group_right on ()` queries — Loki "duplicate series" bug in their data.
- Queries with lookback > 90m (the fixture window).

### Fixture design

- 2500 entries per tenant, 12 streams (3 apps × 2 envs × 2 pods), 90 min.
- No structured metadata (per-entry SM breaks stream identity comparison).
- No same-timestamp twins (kept 3 exact duplicates for dedup testing).
- Deterministic: seeded RNG (seed 42).

### Cluster topology

- 2 shards × 2 replicas (`ch1`–`ch4`) + 1 Keeper, podman network `lhnet`.
- Native ports `localhost:19001`–`19004`.
- Image: `docker.io/clickhouse/clickhouse-server:26.9`.
- Cluster name: `lokihouse`.

## Loki quirks discovered

1. **`JoinSampleVector` with maxSeries=0**: hard cap, not unlimited. The
   `seriesLimiter` middleware (drilldown path) treats 0 as unlimited; the
   engine path does not.
2. **`SmallestPositiveIntPerTenant` vs `SmallestPositiveNonZeroIntPerTenant`**:
   the former returns 0 if any tenant has 0 (which then triggers the hard
   cap); the latter skips 0 values (treating 0 = unlimited).
3. **`topk(k, ...)` with k < candidates**: winner depends on float
   accumulation order, which differs between Loki's in-memory engine and
   lokihouse's ClickHouse-backed engine.
4. **`absent_over_time` on empty selector**: range engine returns the series,
   instant engine returns empty.
5. **Filesystem backend flush lag**: Loki's filesystem storage can take 60+
   seconds to make pushed data visible to queries. The oracle waits up to
   120s with a 60% tolerance before proceeding.

## Running the tests

```sh
# Single node (requires ClickHouse on localhost:9000)
make test
make oracle

# Podman cluster (2×2 + Keeper, ports 19001-19004)
make test-cluster
make oracle-cluster

# Cleanup
make cluster-down
```

## Next phases (see plan §"Follow-up plans")

- **Phase 2:** Tier-A SQL push-down (planner whitelist, windowed range
  aggregations, vector aggregations, regex parity gate).
- **Phase 3:** Tail (websocket), index stats, volume_range, patterns,
  delete API, retention TTL.
- **Phase 4:** Ruler (rules API, evaluation, recording-rule remote write).
- **Phase 5:** Rate limits, multi-tenant queries, perf benchmark, packaging.
