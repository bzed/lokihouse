# Phase 2 execution tracker

Plan: `2026-10-11-lokihouse-phase2-pushdown.md`. Inline execution, TDD per task,
one commit per task. Single-node ClickHouse on localhost:9000; 2x2 podman cluster
on 19001-19004 (keeper + ch1-ch4).

- [x] Recreate existing ClickHouse databases (post-review schema change)
- [x] Task 1: Stream Tier-B samples (samples.go) — commit
  - Note: plan's expected label order at equal ts was inverted; actual order is
    by stream_id (cityHash64) as the SQL guarantees. Test fixed, semantics unchanged.
- [x] Task 2: Keyset-chunked log reads (keyset.go) — commit
  - Found & fixed: empty base on no-streams path produced bare `LIMIT ?` SQL.
- [x] Task 3: Necessary pre-filters from regex/pattern filters — commit
  - Note: property test inlines the line as a SQL literal (clickhouse-go mis-binds a
    leading ?-as-alias subquery arg).
- [x] Task 4: Regex parity gate (regexgate.go) — commit
- [x] Task 5: Leaf mapper + Downstreamer, pushdown mode — commit
  - Note: root vector agg / root arith binop over a range-agg leaf diverges by
    ~1e-16 in float order; Map returned ok=false for those shapes (Tier B).
    Phase 2 review: Loki's own order is random (map iteration), so Map now maps
    them and the tests compare floats at 1e-9 relative, like the oracle.
- [x] Task 6: Tier A leaf (buckets.go, tiera.go) — commit
  - Note: series labels come from the stream's real labels (BaseLabels) merged
    with structured metadata, not from the selector matchers — so regex/inequality
    matchers don't drop labels. Empty windows emit no point (matches Loki).
- [x] Task 7: Property tests Tier A == Tier B — commit
  - Note: test infrastructure added (seeded data, 3 seeds × 300 draws, coverage
    assertion). It was skipped at first for a "1-count difference at boundary
    points". Phase 2 review: the cause was Tier B (clickhouse-go binds time.Time
    at second precision, truncating query bounds and keyset keys); fixed and
    re-enabled. See docs/phase2-status.md "Fixed during the Phase 2 review".
- [x] Task 8: Oracle coverage (pushdown corpus, modes, coverage) — commit
  - Note: added pushdown.logql corpus (40+ queries), debug endpoint for
    TierALeaves, coverage assertion, and oracle-all Makefile target. All
    modes (off, downstream, on) pass on single node and cluster.
- [x] Task 9: Benchmark + docs/phase2-status.md — commit
  - Note: the first benchmark's seed INSERTs failed silently (empty data);
    rewritten in the Phase 2 review. Tier A is 2.1-5.8x faster with ~97% less
    memory. Created phase2-status.md with exit criteria,
    benchmark results, decision record, and known issues.

Verification per task: `go test -count=1 ./internal/...` (single node),
`make test-cluster` (Tasks 5-8 per plan), `make oracle` / `make oracle-downstream`
where the plan requires. On failure: systematic-debugging, fix the code, never the
test's expectations.
