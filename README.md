<div align="center">
  <img src="assets/logo.svg" alt="lokihouse logo" width="160" height="160">

  <h1>lokihouse</h1>
  <p><strong>Grafana Loki's HTTP API, with logs stored in ClickHouse.</strong></p>

  [![CI](https://github.com/bzed/lokihouse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/bzed/lokihouse/actions/workflows/ci.yml)
  [![codecov](https://codecov.io/gh/bzed/lokihouse/branch/main/graph/badge.svg)](https://codecov.io/gh/bzed/lokihouse)
  [![Go Reference](https://pkg.go.dev/badge/github.com/bzed/lokihouse.svg)](https://pkg.go.dev/github.com/bzed/lokihouse)
  [![Docs](https://img.shields.io/badge/docs-bzed.github.io%2Flokihouse-blue)](https://bzed.github.io/lokihouse/)
  [![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
  [![Status: release candidate](https://img.shields.io/badge/status-release%20candidate-yellowgreen)](docs/roadmap.md)
</div>

---

> **Release candidate.** lokihouse is feature complete and passes the Loki
> compatibility oracle, but has not yet been proven in production. Packages and
> images are built from every release. Back up the ClickHouse database before
> upgrading; schema changes ship with a migration (`-schema.migrate`).

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

## Speed

Query latency against Loki on identical data and queries (200 000 lines, 50
streams, one node each, p50, lokihouse 0.12.0; every answer is compared with
Loki's before it counts):

| query | Loki | lokihouse |
|---|---:|---:|
| `count_over_time` | 71 ms | **12 ms** |
| `topk` | 34 ms | **12 ms** |
| `bytes_over_time` | 33 ms | **14 ms** |
| `logfmt` filter | 56 ms | **27 ms** |
| `rate` with regex | 18 ms | **13 ms** |
| `unwrap` + `quantile_over_time` | 69 ms | 72 ms |
| plain log filter | 6 ms | 9 ms |

Method, environment and caveats: [`docs/benchmark.md`](docs/benchmark.md).

## Install

**Debian 13 "trixie"** (amd64, arm64), from the signed apt repository:

```console
$ sudo install -d -m 0755 /etc/apt/keyrings
$ sudo curl -fsSLo /etc/apt/keyrings/lokihouse-archive-keyring.asc https://bzed.github.io/lokihouse/apt/lokihouse-archive-keyring.asc
$ sudo tee /etc/apt/sources.list.d/lokihouse.sources <<'EOF'
Types: deb
URIs: https://bzed.github.io/lokihouse/apt
Suites: trixie
Components: main
Signed-By: /etc/apt/keyrings/lokihouse-archive-keyring.asc
EOF
$ sudo apt update && sudo apt install lokihouse
```

The key's fingerprint is `74E9 21AC B040 DCC7 2C02  A144 1193 7B6E A8AF C649`.
Edit `/etc/lokihouse/lokihouse.yaml` (the service is not started on install),
then `sudo systemctl enable --now lokihouse`.

**Container** (linux/amd64, linux/arm64):

```console
$ podman run -d -p 3100:3100 -e LOKIHOUSE_CLICKHOUSE_ADDR=clickhouse.example.org:9000 ghcr.io/bzed/lokihouse:latest
```

ClickHouse credentials: `-e LOKIHOUSE_CLICKHOUSE_USER=...` and either
`-e LOKIHOUSE_CLICKHOUSE_PASSWORD=...` or, preferably, a mounted file named by
`-e LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE=...`
([details](https://bzed.github.io/lokihouse/install/#clickhouse-password)).

**From source** (a ClickHouse server 26.9 or later and a recent Go toolchain, see `go.mod`):

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
the optional keys; the [installation guide](docs/install/index.md) has the
details, including systemd.

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
