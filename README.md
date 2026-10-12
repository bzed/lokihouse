<div align="center">
  <img src="assets/logo.svg" alt="lokihouse logo" width="160" height="160">

  <h1>lokihouse</h1>
  <p><strong>Grafana Loki's HTTP API, with logs stored in ClickHouse.</strong></p>

  [![CI](https://github.com/bzed/lokihouse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/bzed/lokihouse/actions/workflows/ci.yml)
  [![Go Reference](https://pkg.go.dev/badge/github.com/bzed/lokihouse.svg)](https://pkg.go.dev/github.com/bzed/lokihouse)
  [![Docs](https://img.shields.io/badge/docs-bzed.github.io%2Flokihouse-blue)](https://bzed.github.io/lokihouse/)
  [![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
  [![Status: work in progress](https://img.shields.io/badge/status-work%20in%20progress-orange)](docs/roadmap.md)
</div>

---

> **Work in progress.** lokihouse is under active development and not ready
> for production use yet. Packaging (Debian repositories, container images) is
> still to come; the [installation guide](docs/install/index.md) is the manual
> path that works today.

lokihouse is a single static Go service that speaks
[Grafana Loki](https://grafana.com/oss/loki/)'s HTTP API (push, LogQL queries,
labels, series, tail, index statistics, deletes and the ruler) and stores the
logs in [ClickHouse](https://clickhouse.com/). Point Grafana, Alloy or
Promtail at it as if it were Loki; ClickHouse does the storage and the heavy
lifting.

**Built with**
[![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Grafana Loki](https://img.shields.io/badge/Grafana%20Loki-compatible-F46800?logo=grafana&logoColor=white)](https://github.com/grafana/loki)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-26.9-FFCC00?logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![Debian](assets/built-with-debian.svg)](https://www.debian.org/)
[![Claude](assets/built-with-claude.svg)](https://claude.com/claude-code)

## What you get

- **Loki's API, unchanged.** The same endpoints, the same LogQL, the same error
  messages and configuration keys wherever Loki has them. Loki's own parser,
  engine and ruler code is reused rather than re-implemented, at one pinned Loki
  commit.
- **Proven equality.** A differential test suite (the "oracle") runs the same
  data and queries against real Loki and compares the answers. Every accepted
  difference is listed, with its reason, in
  [`docs/divergences.md`](docs/divergences.md).
- **ClickHouse underneath.** Single server, or a cluster with replicated and
  distributed tables. Credentials can come from a `password_file`.
- **One binary.** Static (`CGO_ENABLED=0`), no runtime dependencies besides a
  ClickHouse server.

## Quick start

You need a ClickHouse server (26.9 or later) and a recent Go toolchain (see
`go.mod`):

```console
$ podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9
$ make build
$ cat > lokihouse.yaml <<'YAML'
server:
  http_listen_port: 3100
clickhouse:
  addr: ["localhost:9000"]
  database: lokihouse
  username: default
YAML
$ ./bin/lokihouse -config.file lokihouse.yaml
```

Then use it like Loki, for example as a Grafana Loki data source at
`http://localhost:3100`. [`lokihouse.example.yaml`](lokihouse.example.yaml) lists
the optional keys; the [installation guide](docs/install/index.md) covers
running it under systemd.

## Documentation

Full documentation: **<https://bzed.github.io/lokihouse/>**

- [Installation](docs/install/index.md) and [migrating from Loki](docs/migrating.md)
- [Configuration](docs/reference/configuration.md) and [HTTP API](docs/reference/http-api.md)
- [Operations](docs/operations.md) and [benchmarks](docs/benchmark.md)
- Design: [`docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`](docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md)
- Known differences from Loki: [`docs/divergences.md`](docs/divergences.md)
- What is left to do: [`docs/roadmap.md`](docs/roadmap.md); status per phase:
  `docs/phase*-status.md`

## Development

Read [`AGENTS.md`](AGENTS.md) first; it holds the rules for every change, by
humans and by coding agents. [`docs/development.md`](docs/development.md) has
the details.

```console
$ make check            # format, vet, lint, govulncheck, tests with -race, coverage >= 85 %
$ make test-cluster     # the same against a ClickHouse cluster
$ make oracle-all       # differential tests against real Loki
```

## Trademarks

Loki and Grafana are trademarks of Grafana Labs. ClickHouse is a trademark of
ClickHouse, Inc. Debian is a registered trademark of Software in the Public
Interest, Inc. Claude is a trademark of Anthropic. lokihouse is an independent
project, not affiliated with or endorsed by any of them; the names are used only
to say what it works with. The logo is original artwork and does not use their
logos.

## License

AGPL-3.0, see [`LICENSE`](LICENSE).
