# Changelog

All notable changes to lokihouse are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The Debian package
changelog (`debian/changelog`) carries the same releases.

## [Unreleased]

## [0.8.0] - 2026-10-22

### Security

- Built with Go 1.27.3, which fixes GO-2026-6809 (`crypto/x509` intermediate certificate distrust).

### Added

- Container image: `LOKIHOUSE_HTTP_LISTEN_PORT` sets `server.http_listen_port`.
- Container image: `LOKIHOUSE_CLICKHOUSE_PASSWORD` and `LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE` set `clickhouse.password` and `clickhouse.password_file`; the install guide documents how to pass the ClickHouse password to the container.

## [0.7.0] - 2026-10-13

lokihouse is now a release candidate: feature complete, not yet proven in production.

### Added

- With `query.querier.multi_tenant_queries_enabled`, `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns` accept several tenants (`a|b`), as in Loki.
- `clickhouse.dial_timeout`, `clickhouse.read_timeout`, `clickhouse.max_open_conns` and `clickhouse.max_idle_conns`. The pool now defaults to 100 connections (was clickhouse-go's 10) and the dial timeout to 5s (was 30s).

### Changed

- Processed delete requests now physically remove the deleted rows from disk (`APPLY DELETED MASK`); before, they stayed on disk, hidden, until a merge or retention dropped their partition.
- `GET /ready` returns 503 when ClickHouse is unreachable (it always returned 200 before).
- A push spanning more than 60 day/retention partitions is rejected with 400 (discard reason `too_many_partitions`) instead of failing in ClickHouse with 500 forever; pushes over 8 partitions bypass the async-insert buffer, so a backfill can no longer fail other pushes that share its flush.
- Line filters (`|=`, and the literals of `|~`) now use the `line_text` full-text index, which no query used before: ClickHouse skips the granules without a match instead of reading every line of the selected streams.

### Fixed

- `delete_requests` is created as `ReplacingMergeTree(version)`. Before, a merge kept the last-inserted status of a delete request instead of the newest, so with skewed clocks between instances a cancelled request could reappear as processed. Existing tables are not changed; `docs/operations.md` shows how to rebuild them.

## [0.6.0] - 2026-10-13

### Added

- Signed apt repository (suites `trixie` and `trixie-snapshot`, amd64 and arm64)
  published with the documentation.
- Container image `ghcr.io/bzed/lokihouse` (linux/amd64 and linux/arm64), signed
  with cosign, with SBOM and provenance.
- `SECURITY.md`, `CONTRIBUTING.md` and this changelog.

### Changed

- Pinned Loki commit bumped from `fbc3f077` to `bc196286` (2026-10-12). The Loki
  code lokihouse copies is unchanged between the two; unit, cluster and oracle
  suites pass.

### Fixed

- Delete requests: a status change (processed, cancelled) written in the same
  clock tick as the previous write could lose to it; versions are now strictly
  increasing.
- Documentation links that pointed at the site root instead of `/lokihouse/`.
- The man page title (lintian `wrong-manual-section`).

## [0.5.0] - 2026-10-11

### Added

- Graceful shutdown, Debian packaging with a hardened systemd unit, user and
  reference documentation, CI, per-tenant limits, multi-tenant queries, metrics
  and query statistics, a Loki benchmark, and memory/goroutine leak checks
  (goleak, pprof, soak test).
