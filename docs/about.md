# About / License

lokihouse is a single Go service that is wire-compatible with Grafana
Loki's HTTP API but stores and processes logs in ClickHouse.

## License

lokihouse is **AGPL-3.0-only**. The license is required because lokihouse
imports Loki's `pkg/logql/**` and `pkg/distributor` packages, which are
AGPL-3.0-only. See the
[full license text](https://github.com/bzed/lokihouse/blob/main/LICENSE).

## Source

- **Repository:** <https://github.com/bzed/lokihouse>
- **Design spec:** [`docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`](https://github.com/bzed/lokihouse/blob/main/docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md)
- **Per-phase status:** [`docs/phase1-status.md`](https://github.com/bzed/lokihouse/blob/main/docs/phase1-status.md) … [`docs/phase4-status.md`](https://github.com/bzed/lokihouse/blob/main/docs/phase4-status.md)
- **Canonical divergences list:** [`docs/divergences.md`](https://github.com/bzed/lokihouse/blob/main/docs/divergences.md)

## Status

lokihouse is **work in progress**. Phases 1–4 are complete (foundation,
SQL push-down, remaining APIs, ruler). Phase 5 (hardening: rate limits,
multi-tenant queries, query stats, `/metrics` endpoint, packaging) is in
progress. See the per-phase status docs in the repository for the detailed
exit criteria and benchmark results.
