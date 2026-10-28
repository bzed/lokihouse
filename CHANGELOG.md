# Changelog

All notable changes to lokihouse are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The Debian package
changelog (`debian/changelog`) carries the same releases.

## [Unreleased]

### Added

- Metric range queries that run in lokihouse's own engine (`| unwrap`, `quantile_over_time`, parsers) are split by `split_queries_by_interval` (default `1h`) and the pieces are evaluated in parallel, up to `max_query_parallelism` per request and half of `clickhouse.max_open_conns` across requests; `split_queries_by_interval: 0` turns it off. The histogram `loki_query_frontend_partitions` counts the pieces.

### Changed

- Queries look up a tenant's pending delete requests through Loki's delete-requests cache, refreshed every 5 minutes, instead of querying ClickHouse every time: as in Loki, a new or cancelled delete request can take up to 5 minutes to change query results.
- Log queries whose selector matches a single stream read ClickHouse in the part's physical order instead of sorting the range by timestamp; their pagination continues from the last row with a keyset predicate the primary key can prune. On a cluster, a single-stream log query is routed to the shard that owns the stream.
- Pinned Loki commit bumped from `bc196286` to `39c256e5` (2026-10-27). Inherited from Loki: `sum`, `avg`, `sum_over_time`, `avg_over_time` and unwrapped `rate`/`bytes_rate` use Kahan summation (results can differ in the last digits); `|~ "a .*(bb|cc)"` no longer misses lines such as `a x bb`; `|~ ".*"` counts as no filter; stream-first metric queries stop at the first pipeline error; a parser after `| drop name` keeps `name` instead of `name_extracted`. Push metrics and responses are unchanged.

### Fixed

- Metric queries with a `|=` or chain that has an empty operand, such as `count_over_time({app="web"} |= "" or "POST" [5m])`, counted every line instead of only those matching the non-empty operands. Like Loki, an empty operand is now ignored.

## [0.12.0] - 2026-10-24

### Changed

- Metric queries that run in lokihouse's own engine (e.g. `| unwrap`, `quantile_over_time`) let ClickHouse remove duplicate entries (`FINAL`) instead of lokihouse; much faster when retried pushes left duplicates in unmerged parts.
- Metric queries that run in lokihouse's own engine parse and extract samples on up to GOMAXPROCS cores per query.

## [0.11.0] - 2026-10-23

### Added

- Schema version 3: text indexes on structured-metadata keys and values (`sm_keys`, `sm_vals`) and the `line_ascii_escape` column.

### Changed

- **Breaking:** after upgrading, run `lokihouse -schema.migrate` once with every instance stopped. From version 2 it only alters `logs` (no copy, no extra disk space); a normal start against a version 2 database exits 1 and names the command. Existing data is not re-indexed.
- Case-insensitive line filters (`|~ "(?i)…"`, Grafana Logs Drilldown's search) use the ClickHouse text index.
- Log and metric queries that run in lokihouse's own engine read ClickHouse in time windows of at most 250 000 rows: metric queries with parsers (`| json`, `| logfmt`) no longer sort the whole range in ClickHouse memory.
- Metric queries (`count_over_time`, `rate`) pushed down to ClickHouse no longer read log lines unless the query needs bytes (`bytes_over_time`, `bytes_rate`).
- `sum` and `sum by` over `count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` aggregate in ClickHouse by only the needed structured-metadata keys (large gains for logs with per-line metadata such as `trace_id`).
- Label filters on structured metadata before any parser (`{…} | trace_id="…"`) use the new metadata indexes.
- `count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` with structured-metadata equality filters (`| level="warn"`) run in ClickHouse.
- Label equality filters after `| json` and `| logfmt` (`| json | level="error"`) use the ClickHouse text index.

### Fixed

- Queries whose selector matches more than about 13 000 streams no longer fail with `Max query size exceeded`.

## [0.10.0] - 2026-10-23

### Documentation

- `-schema.migrate` with the ClickHouse password in a systemd credential: run it with `systemd-run -p ImportCredential='lokihouse.*'` (install guide, manual page, README.Debian).
- New "ClickHouse user and grants" section: the privileges lokihouse and `-schema.migrate` need, on a single server and on a cluster.

## [0.9.0] - 2026-10-23

### Added

- Retention sweeper (`compactor.retention_enabled`, default `true` where Loki's is `false`): `retention_period` and `retention_stream` now apply to stored data every `compactor.compaction_interval`. New divergence: a Loki config with `retention_period` but without `retention_enabled` expires data on lokihouse.

### Changed

- A changed `retention_period` or `retention_stream` now applies to data that is already stored (it only affected new pushes before).
- **Breaking:** after upgrading from a release with schema version 1, run `lokihouse -schema.migrate` once, with every instance stopped; it needs free disk space for a copy of the largest table. A normal start against a version 1 database exits 1 and names the command.
- Schema version 2: `logs`, `streams` and `stream_stats_hourly` no longer carry `retention_days`, a TTL or a retention partition key; they are partitioned by UTC day only. A push is limited to 60 UTC days (was 60 day/retention partitions).
- `lokihouse.example.yaml` now shows every option with its default, and the Debian package installs it as `/etc/lokihouse/lokihouse.yaml`.
- Debian package: built with Go 1.27.3 (0.8.0 still downloaded 1.27.2).

### Documentation

- New retention section (`retention_period`, `retention_stream`); corrected the limits tables (keys that do not exist, wrong defaults); documented `query.pushdown`, the `compactor` keys and that there are no per-tenant overrides (new divergence).

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
