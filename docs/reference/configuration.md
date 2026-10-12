# Configuration

lokihouse is configured with a single YAML file, passed with
`-config.file=<path>` (default `lokihouse.yaml`). Unknown keys are an error
(strict parsing, as in Loki). With `-config.expand-env`, `${VAR}` and
`${VAR:-default}` are expanded before parsing (Loki's
`-config.expand-env`).

Defaults below are the values lokihouse uses when a key is omitted, as
defined in [`internal/config/config.go`](https://github.com/bzed/lokihouse/blob/main/internal/config/config.go).

## `server`

HTTP server settings.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `server.http_listen_address` | string | `""` (all interfaces) | Address to bind. Empty binds all interfaces. |
| `server.http_listen_port` | int | `3100` | Port to listen on. |
| `server.http_server_read_timeout` | duration | `30s` | Read timeout for the HTTP server. |
| `server.http_server_read_header_timeout` | duration | `0` (use the read timeout) | Read-header timeout. |
| `server.http_server_write_timeout` | duration | `30s` | Write timeout. Long-running LogQL queries may need a larger value (the default `query_timeout` is 1m). |
| `server.http_server_idle_timeout` | duration | `120s` | Idle keep-alive timeout. |
| `server.graceful_shutdown_timeout` | duration | `30s` | How long to wait for in-flight requests on shutdown. |
| `server.log_level` | string | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `server.log_format` | string | `logfmt` | Log format: `logfmt` or `json`. |
| `server.register_instrumentation` | bool | `true` | When true, serves `GET /metrics` and `/debug/pprof/*` (no tenant). Set `false` to disable both, e.g. on untrusted networks. |

## `auth_enabled`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `auth_enabled` | bool | `true` | When `true` (the default), every API endpoint except `/ready` and `format_query` requires the `X-Scope-OrgID` header. When `false`, no tenancy is enforced and the header is ignored. |

> Note: the default is `true` (tenancy on), matching Loki's single-binary
> default. The [example config](https://github.com/bzed/lokihouse/blob/main/lokihouse.example.yaml)
> sets it to `false` for a quick local setup.

## `clickhouse`

Connection and topology settings for the ClickHouse storage.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `clickhouse.addr` | list of strings | `["localhost:9000"]` | ClickHouse server addresses (native protocol, port 9000). List all nodes for a cluster. |
| `clickhouse.database` | string | `lokihouse` | The ClickHouse database to use. Created on first start if it does not exist. |
| `clickhouse.username` | string | `default` | ClickHouse user. |
| `clickhouse.password` | string | `""` | ClickHouse password. Mutually exclusive with `clickhouse.password_file`. |
| `clickhouse.password_file` | string | `""` | Path to a file containing the password (trailing newline stripped). The file should be `0o600`. Mutually exclusive with `clickhouse.password`. |
| `clickhouse.cluster` | string | `""` (single server) | The ClickHouse `remote_servers` cluster name. Empty = single-server mode. When set, lokihouse creates `*_local` `Replicated*MergeTree` tables `ON CLUSTER` plus `Distributed` tables. |
| `clickhouse.replicated` | bool | `true` | When `clickhouse.cluster` is set: `true` = `Replicated*MergeTree` (replication), `false` = sharding without replication. Ignored in single-server mode. |
| `clickhouse.replica_path` | string | server's `default_replica_path` | Replicated table path. Optional; the server default is used when omitted. |
| `clickhouse.replica_name` | string | server's `default_replica_name` | Replica name template. Optional. |
| `clickhouse.settings` | map | `{}` | Extra ClickHouse session settings, applied to every connection. Overrides topology defaults (e.g. `insert_quorum`, `distributed_product_mode`). |

### Password handling

`clickhouse.password` and `clickhouse.password_file` are mutually exclusive; setting both is an
error. `clickhouse.password_file` is read at startup; if the file is missing or
unreadable, lokihouse refuses to start. The file's contents are trimmed of
trailing `\r\n`.

For production, prefer `clickhouse.password_file` (or systemd credentials) over a
plain `clickhouse.password:` in the config file, so the secret is not in a world-readable
file.

## `query`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `query.pushdown` | string | `on` | SQL push-down mode: `on` (push eligible queries into ClickHouse), `off` (always run Loki's engine over ClickHouse rows), or `downstream` (push down only when the client opts in). See the project's Phase 2 status doc for details. |

### `query.querier`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `query.querier.multi_tenant_queries_enabled` | bool | `false` | When `true`, a query whose `X-Scope-OrgID` names several tenants (`a|b`) is answered by querying each tenant and labelling results with `__tenant_id__` (Loki's multi-tenant queries). Multi-tenant queries always run on the plain engine (no SQL push-down). When `false` (the default), such a request is rejected with `multiple org IDs present`. |

## `compactor`

Settings for the background delete-processing loop.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `compactor.compaction_interval` | duration | `10m` | How often the delete processor runs. |
| `compactor.delete_request_cancel_period` | duration | `24h` | How long a delete request waits before it is processed. |
| `compactor.delete_max_interval` | duration | `24h` | Maximum interval between delete-processor runs. |

## `ruler`

Loki's ruler block, with Loki's keys and defaults. The ruler only runs when
`ruler_storage` is also configured (see below).

The full set of keys is documented in the
[Loki ruler configuration reference](https://grafana.com/docs/loki/latest/configuration/#ruler).
The most commonly set keys:

| Key | Description |
| --- | --- |
| `ruler.rule_path` | Directory for rule files. **Default differs from Loki**: `rules-temp` relative to the working directory (Loki's single-binary default is `<common.path_prefix>/rules-temp`; lokihouse has no `path_prefix`). Set this explicitly. |
| `ruler.alertmanager_url` | Alertmanager URL for alert notifications. |
| `ruler.external_url` | External URL of the ruler (used in alert annotations). |
| `ruler.remote_write` | Recording-rule remote-write configuration (e.g. to Prometheus). |
| `ruler.wal` | WAL directory for the ruler. **lokihouse refuses to start** if this directory is empty or is the working directory, the rule storage directory, or `ruler.rule_path` (a safety measure; Loki would `os.RemoveAll` it). |
| `ruler.enable_sharding` | **Rejected at startup.** lokihouse has no ruler ring; this must be `false`. |
| `ruler.evaluation.mode` | `remote` is **rejected at startup** (no ring). Must be `local` (the default). |

## `ruler_storage`

The ruler only starts when this block is set. Two backends:

| Key | Description |
| --- | --- |
| `ruler_storage.backend` | `filesystem` (read/write rule files) or `local` (read-only rule files). |
| `ruler_storage.filesystem.dir` | Directory holding the rule files (for `backend: filesystem`). |

The full set of keys is documented in the
[Loki ruler storage reference](https://grafana.com/docs/loki/latest/configuration/#ruler_storage).

> **Single-instance rule:** because lokihouse has no ruler ring, every
> instance with `ruler_storage` configured evaluates **all** rule groups.
> Configure the ruler on exactly one instance; otherwise alerts and
> recording-rule samples are duplicated.

## `limits_config`

Loki's `validation.Limits` block. Every key Loki accepts is accepted here,
with Loki's defaults. The full set is documented in the
[Loki limits reference](https://grafana.com/docs/loki/latest/configuration/#limits_config).

Keys an operator is most likely to tune:

| Key | Loki default | Description |
| --- | --- | --- |
| `limits_config.max_query_series` | `0` (unlimited) | Hard cap on the number of series a query may return. lokihouse's test configs set `100000`. |
| `limits_config.max_query_lookback` | `0` (no limit) | Maximum lookback for a query. |
| `limits_config.volume_enabled` | `false` | Whether `index/volume` is enabled. |
| `limits_config.max_cache_size` | `1GB` | Per-query result cache size. |
