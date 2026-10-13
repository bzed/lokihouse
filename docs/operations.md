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
| `max_received_message_size` / `max_decompressed_message_size` | push parsing | Oversized pushes are rejected. |
| `enforced_labels`, `enforced_metric_name_labels` | push validation | Streams missing them are rejected. |
| `reject_new_samples_max_age`, `reject_new_samples_max_offset` | push validation | Out-of-window samples are dropped. |
| `stream_granularity` (retention) | push | `retention_days` written per stream. |

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
| `max_query_series` | Loki engine | `400` if the result would exceed the cap. |
| `max_query_range` | Loki engine | `400` if the range is wider than allowed. |

Per-tenant values are read with the smallest positive value across the
tenants in the request, as in Loki.

See [Differences from Loki](divergences.md#hardening-phase-5) for the rate-limit
strategy difference.

## Health checks

`GET /ready` answers `200 ready` when lokihouse can reach ClickHouse (a ping with a 2 s deadline) and `503 ClickHouse not ready: <error>` otherwise. All lokihouse instances usually share one ClickHouse, so they become unready together; use `/ready` for load-balancer readiness, not for liveness or restarts.

## Deletes and disk space

A processed delete request removes rows with lightweight `DELETE`s and then runs `ALTER TABLE logs_local APPLY DELETED MASK` (ON CLUSTER on a cluster), which rewrites only the parts that hold deleted rows. Deleted lines therefore leave the disk at the end of the compactor pass that processed the request, as with Loki's compactor. The rewrite costs I/O proportional to the affected parts; it runs once per pass, not once per request.

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
