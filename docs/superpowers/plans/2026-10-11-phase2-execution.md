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
- [ ] Task 4: Regex parity gate (regexgate.go) — commit
- [ ] Task 5: Leaf mapper + Downstreamer, pushdown mode — commit
- [ ] Task 6: Tier A leaf (buckets.go, tiera.go) — commit
- [ ] Task 7: Property tests Tier A == Tier B — commit
- [ ] Task 8: Oracle coverage (pushdown corpus, modes, coverage) — commit
- [ ] Task 9: Benchmark + docs/phase2-status.md — commit

Verification per task: `go test -count=1 ./internal/...` (single node),
`make test-cluster` (Tasks 5-8 per plan), `make oracle` / `make oracle-downstream`
where the plan requires. On failure: systematic-debugging, fix the code, never the
test's expectations.
