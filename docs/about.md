---
description: About lokihouse: AGPL-3.0 license, source code, project status and links to the design document.
---

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
- **Canonical divergences list:** [`docs/divergences.md`](https://github.com/bzed/lokihouse/blob/main/docs/divergences.md)

## Status

lokihouse is a **release candidate**: it is feature complete and passes the
Loki compatibility oracle. What is missing before a stable release is
production experience. See the [roadmap](roadmap.md) for what is still open.
