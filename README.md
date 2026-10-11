# lokihouse

> **Work in progress.** lokihouse is under active development and not ready
> for production use yet. Packaging, documentation and the remaining hardening
> work are coming soon.

lokihouse is a single Go service that speaks Grafana Loki's HTTP API (push,
LogQL queries, labels, series, tail, index statistics, deletes and the ruler)
and stores the logs in ClickHouse. Feature equality with Loki is checked by a
differential test suite that runs the same data and queries against real Loki.

- Design: [`docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`](docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md)
- Known differences from Loki: [`docs/divergences.md`](docs/divergences.md)
- Status per phase: `docs/phase*-status.md`

## License

AGPL-3.0, see [`LICENSE`](LICENSE).
