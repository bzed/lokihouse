---
description: About Rúnakefli: AGPL-3.0 license, source code, project status and links to the design document.
---

# About / License

Rúnakefli is a single Go service that is wire-compatible with Grafana®
Loki®'s HTTP API but stores and processes logs in ClickHouse®.

## The name

Rúnakefli (ASCII: `runakefli`) is Icelandic for a stick carved with runes, an
old way of keeping records on wood. Until version 0.14.0 the project was called
lokihouse; it was renamed as a precaution to avoid trademark issues with
Grafana Labs' Loki mark. Rúnakefli is an independent project, not affiliated
with or endorsed by Grafana Labs.

## License

Rúnakefli is **AGPL-3.0-only**. The license is required because Rúnakefli
imports Loki's `pkg/logql/**` and `pkg/distributor` packages, which are
AGPL-3.0-only. See the
[full license text](https://github.com/bzed/runakefli/blob/main/LICENSE).

## Source

- **Repository:** <https://github.com/bzed/runakefli>
- **Design spec:** [`docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`](https://github.com/bzed/runakefli/blob/main/docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md)
- **Canonical divergences list:** [`docs/divergences.md`](https://github.com/bzed/runakefli/blob/main/docs/divergences.md)

## Status

Rúnakefli is a **release candidate**: it is feature complete and passes the
Loki compatibility oracle. What is missing before a stable release is
production experience. See the [roadmap](roadmap.md) for what is still open.
