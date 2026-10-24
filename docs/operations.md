# Operations

## Limits

lokihouse enforces Loki's `limits_config` keys at the points below. A tenant is
the value of the `X-Scope-OrgID` header.

### Ingestion

| Key | Where enforced | Behaviour |
| --- | --- | --- |
| `ingestion_rate_mb` | distributor (push) | Per-tenant tokens/second. A push over the limit is rejected with `429` and nothing is stored; the push consumes no tokens. |
| `ingestion_burst_size_mb` | distributor (push) | Burst for the bucket above. |
| `max_line_size`, `max_line_size_truncate` | push validation | Lines over the limit are truncated (with Loki's marker) or rejected. |
| `max_label_name_length`, `max_label_value_length`, `max_label_names_per_series` | push validation | Streams violating them are rejected. |
| `reject_old_samples`, `reject_old_samples_max_age`, `creation_grace_period` | push validation | Entries older than the maximum age, or further in the future than the grace period, are dropped. |
| `allow_structured_metadata`, `max_structured_metadata_size`, `max_structured_metadata_entries_count` | push validation | Entries violating them are rejected. |
| `increment_duplicate_timestamp` | push | Loki's duplicate-timestamp handling. |
| `enforced_labels` | push validation | Streams missing them are rejected. |
| `discover_service_name`, `discover_log_levels` | push | Loki's `service_name` and `detected_level` discovery. |
| `retention_period`, `retention_stream` | push | applied by the retention sweeper; see [Retention](reference/configuration.md#retention). |
| `deletion_mode` | delete API | Whether delete requests are accepted. |

The size of one push is bounded by fixed limits (100 MiB compressed, 5000 MiB
decompressed, Loki's defaults); they are not configurable.

### Query

| Key | Where enforced | Behaviour |
| --- | --- | --- |
| `max_query_length` | `checkQuery` (all query endpoints) | `400` if the range is longer than the limit. |
| `max_query_lookback` | `checkQuery` | Start clamped to `now - lookback`; a range entirely before that returns an empty result. |
| `max_entries_limit_per_query` | `checkQuery` | `400` if `limit` exceeds it (log queries only). |
| `max_streams_matchers_per_query` | `checkQuery` | `400` if a stream selector has more matchers than the limit. |
| `required_labels` | `checkQuery` | `400` if a stream selector lacks a required label. |
| `required_number_labels` | `checkQuery` | `400` if a stream selector has fewer matchers than required. |
| `query_timeout` | Loki engine `Eval` | A timed-out query is a `504`. |
| `max_query_series` | Loki engine | `400` if the result would exceed the cap (default `500`). |
| `max_query_range` | Loki engine | `400` if the range is wider than allowed. |

Metric queries that lokihouse evaluates itself (parsers, `unwrap`) parse lines on
`min(GOMAXPROCS, matching streams)` goroutines *per query*, so concurrent queries
multiply the goroutine count while `GOMAXPROCS` (Go follows the cgroup CPU quota)
bounds the CPU they use together. Set the `GOMAXPROCS` environment variable to
limit it.

A metric query on at most 16 streams *fans out*: one goroutine and one
ClickHouse query per stream, merged in Go. Such a query therefore adds up to 16
goroutines and holds up to 16 connections from the ClickHouse pool while it
reads, and buffers roughly 48 MiB of driver blocks (about 3 MiB per stream
against roughly 3 MiB for a single-query read). The fan-out drains its streams
before the query returns: a stream's goroutine waits on nothing, so the query
holds its connections only while its streams transfer — never while the engine
waits on a sibling leg of the same request (a binary operation, a multi-tenant
merge) and never while a stream's query queues for a connection. Requests with
several legs therefore cannot deadlock the pool, even when the pool is smaller
than one fan-out; concurrent fanned-out queries queue on the pool and succeed.
The drained result is capped at about a million samples (roughly 48 MiB); a
query over the cap reads all streams in one query. The connection reservation is
all-or-nothing and capped at half of `clickhouse.max_open_conns`; when it
cannot be taken in full the query falls back to one query over all streams (the
behaviour above 16 streams), so a busy pool degrades latency, never
correctness. On a cluster each per-stream query is routed to the shard owning
the stream (`optimize_skip_unused_shards`), so a fanned-out query costs one
shard query per stream, not one per shard per stream. Size
`clickhouse.max_open_conns` (default `100`) and ClickHouse
`max_concurrent_queries` with this multiplier in mind: dashboards and ruler
evaluations can run many fanned-out queries at once.

`limits_config` is the same for every tenant; there are no per-tenant
overrides. Where a request names several tenants (multi-tenant queries), the
smallest positive value is used, as in Loki.

See [Differences from Loki](divergences.md#hardening-phase-5) for the rate-limit
strategy difference.

## Health checks

`GET /ready` answers `200 ready` when lokihouse can reach ClickHouse (a ping with a 2 s deadline) and `503 ClickHouse not ready: <error>` otherwise. All lokihouse instances usually share one ClickHouse, so they become unready together; use `/ready` for load-balancer readiness, not for liveness or restarts.

## Deletes and disk space

A processed delete request removes rows with lightweight `DELETE`s and then runs `ALTER TABLE logs_local APPLY DELETED MASK` (ON CLUSTER on a cluster), which rewrites only the parts that hold deleted rows. Deleted lines therefore leave the disk at the end of the compactor pass that processed the request, as with Loki's compactor. The rewrite costs I/O proportional to the affected parts; it runs once per pass, not once per request.

### `delete_requests` created by lokihouse 0.6.0 or earlier

Up to 0.6.0, `delete_requests` was created as `ReplacingMergeTree` without its `version` column. A merge then keeps the last-inserted status of a request, not the newest one. When instances' clocks disagree, a cancelled request can come back as processed, or a processed one as received. lokihouse does not change existing tables. To rebuild the table on a single server, stop every lokihouse instance and run (database `lokihouse`):

```sql
CREATE TABLE lokihouse.delete_requests_new AS lokihouse.delete_requests
  ENGINE = ReplacingMergeTree(version) ORDER BY (tenant, request_id);
INSERT INTO lokihouse.delete_requests_new SELECT * FROM lokihouse.delete_requests;
EXCHANGE TABLES lokihouse.delete_requests AND lokihouse.delete_requests_new;
DROP TABLE lokihouse.delete_requests_new;
```

On a cluster, do the same for `delete_requests_local` with `ON CLUSTER` and `ReplicatedReplacingMergeTree(version)`, and run the `INSERT … SELECT` on one replica of each shard.

## Retention

Retention is applied by a sweeper that runs on every lokihouse instance every `compactor.compaction_interval` (see [Retention](reference/configuration.md#retention)). It applies the current configuration to all stored data and is idempotent, so several instances can sweep at the same time. Nothing expires while no lokihouse instance runs.

The retention is not stored with each row. ClickHouse cannot change the columns of a table's partition or sorting key (`ALTER ... UPDATE` refuses them), so a stored value could never follow a configuration change.

To free space at once, drop the day partitions you no longer need (database `lokihouse`; on a single server use the table names without `_local` and without `ON CLUSTER`):

```sql
ALTER TABLE lokihouse.logs_local ON CLUSTER <cluster> DROP PARTITION ID '20260801';
ALTER TABLE lokihouse.streams_local ON CLUSTER <cluster> DROP PARTITION ID '20260801';
ALTER TABLE lokihouse.stream_stats_hourly_local ON CLUSTER <cluster> DROP PARTITION ID '20260801';
```

To see which days are stored:

```sql
SELECT partition, sum(rows) FROM system.parts
WHERE database = 'lokihouse' AND table = 'logs_local' AND active
GROUP BY partition ORDER BY partition;
```

## Metrics

`GET /metrics` (no tenant) serves Prometheus text format from the default
registry plus the ruler's private registry (when the ruler is enabled).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `loki_request_duration_seconds` | histogram | `method`, `route`, `status_code`, `ws` | Time spent serving each HTTP request. |
| `loki_inflight_requests` | gauge | `method`, `route` | Current number of in-flight requests. |
| `loki_distributor_bytes_received_total` | counter | `tenant`, `retention_hours`, `is_internal_stream`, `policy`, `format` | Bytes received per push (Loki's shared distributor collectors). |
| `loki_distributor_lines_received_total` | counter | `tenant`, `is_internal_stream`, `policy`, `format` | Lines received per push (Loki's shared distributor collectors). |
| `loki_discarded_samples_total` | counter | `tenant`, `reason` | Samples discarded by push validation. |
| `go_goroutines`, `process_*`, etc. | — | — | Standard Go runtime metrics from the default registry. |
| `lokihouse_build_info` | gauge | `version` | Build version string. |

Ruler metrics (when enabled): `loki_ruler_*` series collected in a private
registry and exposed through the same `/metrics` endpoint.

## Profiling

When `server.register_instrumentation` is `true` (the default), the Go runtime
profiler is served on the HTTP port at `/debug/pprof/*`, with no tenant. Use
`go tool pprof` to fetch and diff profiles:

```console
$ go tool pprof http://lokihouse:3100/debug/pprof/heap
$ go tool pprof -base warm-heap.pb.gz end-heap.pb.gz   # diff two heap snapshots
```

`/debug/pprof/profile?seconds=30` and `/debug/pprof/trace` run for up to their
`seconds` parameter; `server.http_server_write_timeout` (30 s default) bounds
them, as in Loki.

Keep port 3100 off untrusted networks, or set `server.register_instrumentation:
false`, because pprof exposes allocation and call-stack internals.

## Multi-tenant queries

`query.querier.multi_tenant_queries_enabled` (default `false`) accepts a
comma-separated list of org IDs in `X-Scope-OrgID` (e.g. `tenant-a,tenant-b`)
on the query, series and label endpoints and returns the merged result over
the listed tenants, as Loki's multi-tenant querier does. The `__tenant_id__`
label is always set on returned streams and series so the sources can be
told apart. Index stats, volume, `detected_labels` and `detected_metric_names`
endpoints do not fan out and keep the single-tenant resolution (see
`divergences.md`). Multi-tenant queries run on the plain engine, not the
ClickHouse pushdown path.

## When ClickHouse is unavailable

lokihouse keeps no local buffer; ClickHouse is the only state.

- **At startup** lokihouse creates its tables, and exits non-zero if ClickHouse is unreachable. The Debian unit restarts it after 5 s (`Restart=on-failure`); under Kubernetes the pod restarts with back-off.
- **Pushes** fail with HTTP 500. Loki clients (Promtail, Alloy, the OpenTelemetry Collector) retry 5xx responses with back-off, so nothing is lost unless an outage outlasts the client's retry budget (for example Promtail's `backoff_config.max_retries`). Size the clients' retries for the longest ClickHouse maintenance window you expect.
- **Queries, tails and the ruler** fail while ClickHouse is down. Rule evaluations are counted as failed, as in Loki.
- **Delete processing** logs the error and retries on the next `compactor.compaction_interval`. Query-time filtering keeps pending deletes hidden.
- **`/ready`** answers 503 (see Health checks).
- Requests wait at most `clickhouse.dial_timeout` for a connection and `clickhouse.read_timeout` for each answer from ClickHouse.
