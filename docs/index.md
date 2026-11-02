---
title: Grafana Loki-compatible log storage on ClickHouse
description: lokihouse is a single Go service that speaks Grafana Loki's HTTP API and LogQL and stores the logs in ClickHouse. Debian packages and container images for amd64 and arm64.
hide:
  - toc
---

<div class="lh-hero" markdown>
<div class="lh-hero__text" markdown>

# lokihouse

<p class="lh-tagline"><strong>Grafana Loki's HTTP API, with logs stored in ClickHouse.</strong></p>
<p class="lh-lede">Point Grafana, Alloy or Promtail at it as if it were Loki.</p>

[Install lokihouse](install/index.md){ .md-button .md-button--primary }
[Migrate from Loki](migrating.md){ .md-button }

</div>
<div class="lh-swap" markdown>

<p class="lh-swap__caption">Moving from Loki starts with one line in your Grafana data source:</p>

<pre class="lh-diff" aria-label="Change the data source URL from http://loki:3100 to http://lokihouse:3100"><code><span class="lh-del">- url: http://loki:3100</span><span class="lh-add">+ url: http://lokihouse:3100</span></code></pre>

<p class="lh-swap__note">The push and query endpoints and LogQL stay the same. Alloy and Promtail change their client URL in the same way.</p>

</div>
</div>

<p class="lh-badges">
<a href="https://github.com/bzed/lokihouse/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/bzed/lokihouse/actions/workflows/ci.yml/badge.svg?branch=main"></a>
<a href="https://codecov.io/gh/bzed/lokihouse"><img alt="Code coverage" src="https://codecov.io/gh/bzed/lokihouse/branch/main/graph/badge.svg"></a>
<a href="https://github.com/bzed/lokihouse/blob/main/LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-1e4d5c.svg"></a>
<a href="roadmap/"><img alt="Status: release candidate" src="https://img.shields.io/badge/status-release%20candidate-9c5a09"></a>
</p>

!!! note "Release candidate"

    lokihouse is feature complete and passes the Loki compatibility oracle, but
    has not yet been proven in production. Debian packages (amd64, arm64) and
    container images are built from every release. Back up the ClickHouse
    database before upgrading; schema changes ship with a migration
    (`-schema.migrate`). See the [installation guide](install/index.md).

## What is lokihouse?

lokihouse is a single static Go service that speaks
[Grafana Loki](https://grafana.com/oss/loki/)'s HTTP API (push, LogQL queries,
labels, series, tail, index statistics, deletes and the ruler) and stores the
logs in [ClickHouse](https://clickhouse.com/). Your log shippers and dashboards
keep talking to "Loki"; ClickHouse does the storage and the heavy lifting.

Use it when you already know and like Loki's API, LogQL and tooling, but want
your logs in a ClickHouse you can also query with SQL, replicate, and scale like
any other ClickHouse data.

## What you get

<div class="grid cards" markdown>

-   **Loki's API, unchanged**

    ---

    The same endpoints, the same LogQL, the same error messages and
    configuration keys wherever Loki has them. Loki's own parser, engine and
    ruler code is reused rather than re-implemented, at one pinned Loki commit.

-   **Proven equality**

    ---

    A differential test suite (the "oracle") runs the same data and queries
    against real Loki and compares the answers. Every accepted difference is
    listed, with its reason, in the [divergences](reference/divergences.md).

-   **ClickHouse underneath**

    ---

    A single server, or a cluster with replicated and distributed tables.
    ClickHouse credentials can come from a `password_file`.

-   **One binary**

    ---

    Static (`CGO_ENABLED=0`), with no runtime dependency besides a ClickHouse
    server. Signed Debian packages and container images for amd64 and arm64.

-   **Fast**

    ---

    1.7–4× faster than Loki on aggregations and `logfmt` filters on identical
    data, with every answer checked against Loki's. See [the numbers](#speed).

-   **Built for operations**

    ---

    Loki's `limits_config` is enforced per tenant, tail and the ruler work, and
    schema changes ship with an offline migration. See [operations](operations.md).

</div>

## How it works

<ol class="lh-flow">
  <li class="lh-flow__node"><strong>Your log clients</strong><span>Grafana, Alloy, Promtail, logcli</span></li>
  <li class="lh-flow__link">Loki HTTP API</li>
  <li class="lh-flow__node lh-flow__node--core"><strong>lokihouse</strong><span>one static Go binary</span></li>
  <li class="lh-flow__link">ClickHouse protocol</li>
  <li class="lh-flow__node"><strong>ClickHouse</strong><span>single server or cluster</span></li>
</ol>

## Speed

Query latency against Loki on identical data and queries (200 000 lines, 50
streams, one node each, p50; every answer is compared with Loki's before it
counts):

| query | Loki | lokihouse |
|---|---:|---:|
| `count_over_time` | 53 ms | **14 ms** |
| `topk` | 25 ms | **11 ms** |
| `bytes_over_time` | 25 ms | **15 ms** |
| `logfmt` filter | 46 ms | **19 ms** |
| `rate` with regex | 14 ms | 14 ms |
| plain log filter | 5.5 ms | 5.5 ms |
| `unwrap` + `quantile_over_time` | **49 ms** | 56 ms |

lokihouse is 1.7–4× faster on the aggregations and the `logfmt` filter, level
with Loki on the plain log filter and `rate` with regex, and 1.1–1.2× slower on
`unwrap` + `quantile_over_time`. Method, environment, the object-storage runs
and caveats are in the [benchmark](benchmark.md).

## Quick start

=== "Debian 13 (trixie)"

    From the signed apt repository (amd64, arm64):

    ```console
    $ sudo install -d -m 0755 /etc/apt/keyrings
    $ sudo curl -fsSLo /etc/apt/keyrings/lokihouse-archive-keyring.asc https://lokihouse.dev/apt/lokihouse-archive-keyring.asc
    $ sudo tee /etc/apt/sources.list.d/lokihouse.sources <<'EOF'
    Types: deb
    URIs: https://lokihouse.dev/apt
    Suites: trixie
    Components: main
    Signed-By: /etc/apt/keyrings/lokihouse-archive-keyring.asc
    EOF
    $ sudo apt update && sudo apt install lokihouse
    ```

    The key's fingerprint is `74E9 21AC B040 DCC7 2C02  A144 1193 7B6E A8AF C649`.
    Edit `/etc/lokihouse/lokihouse.yaml` (the service is not started on
    install), then `sudo systemctl enable --now lokihouse`.

=== "Container"

    For linux/amd64 and linux/arm64:

    ```console
    $ podman run -d -p 3100:3100 -e LOKIHOUSE_CLICKHOUSE_ADDR=clickhouse.example.org:9000 ghcr.io/bzed/lokihouse:latest
    ```

    ClickHouse credentials and the `password_file` option are described in the
    [installation guide](install/index.md#clickhouse-password).

=== "From source"

    Needs a ClickHouse server 26.9 or later and a recent Go toolchain:

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
`http://localhost:3100`. The [installation guide](install/index.md) has the
details, including systemd.

## Where to go next

<div class="grid cards" markdown>

-   **[Install lokihouse](install/index.md)**

    ---

    For a sysadmin who has never run lokihouse and wants one running on a bare
    machine.

-   **[Migrating from Loki](migrating.md)**

    ---

    A full cutover runbook, the behavioural differences to know about, and a
    verification checklist.

-   **[Configuration](reference/configuration.md)**

    ---

    Every config key, with defaults.

-   **[HTTP API](reference/http-api.md)**

    ---

    Every endpoint lokihouse serves.

-   **[Operations](operations.md)**

    ---

    Limits, retention, deletes and disk space, metrics, health checks and profiling.

-   **[Roadmap](roadmap.md)**

    ---

    What is deferred or still open.

-   **[Benchmark](benchmark.md)**

    ---

    Method, environment and results of the comparison with Loki.

-   **[Development](development.md)**

    ---

    CI, releases, the signed apt repository, the container image, local testing
    and leak checks.

</div>

## Frequently asked questions

### Is lokihouse a drop-in replacement for Loki?

At the HTTP layer, yes: push, the LogQL query endpoints, labels and series,
tail, the delete API and the ruler API use Loki's paths, JSON shapes and
headers, so a Grafana data source or log shipper only needs a new base URL.
Storage is different (ClickHouse instead of Loki's chunks and index), so
existing data is not read in place. The [migration guide](migrating.md) covers
the cutover and the [divergences](reference/divergences.md) list every
observable difference.

### Why store logs in ClickHouse?

ClickHouse is a column store built for fast scans and aggregations, can be
queried with plain SQL, and replicates and scales as a cluster. lokihouse keeps
the Loki interface people already use and moves the storage onto it.

### Which ClickHouse versions are supported?

ClickHouse 26.9 or later, as a single server or a cluster with replicated and
distributed tables.

### Is lokihouse production ready?

It is a release candidate: feature complete and passing the Loki compatibility
oracle, but not yet proven in production. Back up the ClickHouse database
before upgrading. See the [roadmap](roadmap.md).

### How do I know it behaves like Loki?

A differential test suite runs the same data and queries against real Loki and
compares the answers, and Loki's own LogQL parser, engine and ruler code is
reused rather than re-implemented.

### What is the license?

AGPL-3.0-only, because lokihouse imports Loki's AGPL-licensed LogQL and
distributor packages. See [About](about.md).

### Is lokihouse affiliated with Grafana Labs or ClickHouse, Inc.?

No. lokihouse is an independent project.

## Source and license

The source code, the design spec and the detailed divergences list live in the
[GitHub repository](https://github.com/bzed/lokihouse); how CI, releases and
the apt repository work is described in [Development](development.md). lokihouse is licensed
under AGPL-3.0, see [About](about.md).

Loki and Grafana are trademarks of Grafana Labs. ClickHouse is a trademark of
ClickHouse, Inc. Debian is a registered trademark of Software in the Public
Interest, Inc. Claude is a trademark of Anthropic. lokihouse is an independent
project, not affiliated with or endorsed by any of them; the names are used
only to say what it works with. The logo is original artwork and does not use
their logos.
