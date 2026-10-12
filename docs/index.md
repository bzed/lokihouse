# lokihouse

> **Work in progress.** lokihouse is under active development and not ready
> for production use yet. Packaging (Debian repositories, Docker images) is
> work in progress. The [installation guide](/install/) is the manual
> path that works today: build the static Go binary, run it under systemd,
> store logs in ClickHouse.

lokihouse is a single Go service that speaks Grafana Loki's HTTP API (push,
LogQL queries, labels, series, tail, index statistics, deletes and the
ruler) and stores the logs in ClickHouse. Feature equality with Loki is
checked by a differential test suite that runs the same data and queries
against real Loki.

## Who is this for

- **[I want to install lokihouse](/install/)** — a sysadmin who has never
  run lokihouse and wants one running on a bare machine.
- **[I'm migrating from Loki](/migrating/)** — a sysadmin running stock Loki
  who wants to swap Loki for lokihouse. A full cutover runbook, the
  behavioural differences to know about, and a verification checklist.

## Reference

- [Configuration](/reference/configuration/) — every config key, with
  defaults.
- [HTTP API](/reference/http-api/) — every endpoint lokihouse serves.
- [Divergences](/reference/divergences/) — pointer to the canonical list of
  differences from Loki.
- [About / License](/about/) — AGPL-3.0, links to the design and status docs.

## Source

Source code and the canonical developer docs (per-phase status, the
detailed divergences list, the design spec) live in the
[GitHub repository](https://github.com/bzed/lokihouse).
