# Lokihouse Phase 6 (Operational Gaps) - Status Report

> **Last Updated:** 2026-10-13
> **Current Branch:** main
> **Latest Code Commit:** 1d553a2 - "feat(api): multi-tenant index, volume, detected_labels and patterns endpoints"

## Executive Summary

Phase 6 is **complete**. Total statement coverage is **86.3%** (≥ 85 % target).
`make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` and
`make soak` all passed on the code at 1d553a2 (recorded from real runs).

### Completed Tasks
- ✅ **Task 1** (00dc461): pushes over 60 `(day, retention)` partitions are rejected with 400 (`too_many_partitions`); pushes over 8 partitions are inserted synchronously.
- ✅ **Task 2** (d78fcc8): `clickhouse.dial_timeout`, `read_timeout`, `max_open_conns`, `max_idle_conns`.
- ✅ **Task 3** (b1fa30c): `/ready` returns 503 when ClickHouse is unreachable.
- ✅ **Task 4** (07985d1): processed delete requests run `APPLY DELETED MASK`, retried on failure.
- ✅ **Task 5** (c4cef77): `multitenant.Index` fan-out for index stats, volume, detected labels, patterns.
- ✅ **Task 6** (1d553a2): the endpoints are served through the fan-out.
- ✅ **Task 7**: ClickHouse outage behaviour documented in `docs/operations.md`.

## Deferred
- SQL push-down for multi-tenant queries (single-tenant by construction).
- `detected_metric_names` (not served by lokihouse at all).
