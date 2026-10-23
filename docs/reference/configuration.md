# Configuration

lokihouse is configured with a single YAML file, passed with
`-config.file=<path>` (default `lokihouse.yaml`). Unknown keys are an error
(strict parsing, as in Loki). With `-config.expand-env`, `${VAR}` and
`${VAR:-default}` are expanded before parsing (Loki's
`-config.expand-env`).

Defaults below are the values lokihouse uses when a key is omitted, as
defined in [`internal/config/config.go`](https://github.com/bzed/lokihouse/blob/main/internal/config/config.go).

[`lokihouse.example.yaml`](https://github.com/bzed/lokihouse/blob/main/lokihouse.example.yaml)
shows every option with its default and a short comment. The Debian package
installs it as `/etc/lokihouse/lokihouse.yaml` (a copy stays in
`/usr/share/doc/lokihouse/examples/`), so the file you edit lists everything
you can set. A test loads it with every option uncommented, so it cannot drift
from the code.

The blocks are: [`server`](#server), [`auth_enabled`](#auth_enabled),
[`clickhouse`](#clickhouse), [`query`](#query), [`compactor`](#compactor),
[`limits_config`](#limits_config) (Loki's, including
[retention](#retention)), [`ruler`](#ruler) and [`ruler_storage`](#ruler_storage)
(Loki's).

The command line has three flags: `-config.file` (default `lokihouse.yaml`),
`-config.expand-env` and `-version`; see `man lokihouse`.

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
| `clickhouse.dial_timeout` | duration | `5s` | Timeout for opening a ClickHouse connection and for waiting for a free pooled connection. |
| `clickhouse.read_timeout` | duration | `5m` | Longest wait for a packet from ClickHouse during a query or insert. |
| `clickhouse.max_open_conns` | int | `100` | Connection pool size. Each in-flight push holds a connection until its async insert is flushed; pushes, queries, tails and delete processing share the pool. |
| `clickhouse.max_idle_conns` | int | `20` | Idle connections kept open (≤ `max_open_conns`). |

### Password handling

`clickhouse.password` and `clickhouse.password_file` are mutually exclusive; setting both is an
error. `clickhouse.password_file` is read at startup; if the file is missing or
unreadable, lokihouse refuses to start. The file's contents are trimmed of
trailing `\r\n`.

For production, prefer `clickhouse.password_file` (or systemd credentials) over a
plain `clickhouse.password:` in the config file, so the secret is not in a world-readable
file.

The container image's built-in config maps `LOKIHOUSE_CLICKHOUSE_PASSWORD` to
`clickhouse.password` and `LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE` to
`clickhouse.password_file`; see
[Container image](../install/index.md#container-image). Outside the image,
use `${VAR}` in your config together with `-config.expand-env`.

### ClickHouse user and grants

lokihouse does not need the `default` user. Create a dedicated user and grant it
only what it uses (tested with ClickHouse 26.9). Replace the database name if
`clickhouse.database` is not `lokihouse`, and use your own password:

```sql
CREATE USER lokihouse IDENTIFIED BY '...';

-- Everything lokihouse reads, writes and maintains lives in its own database.
GRANT SELECT, INSERT, ALTER DELETE,
      CREATE DATABASE, CREATE TABLE, CREATE VIEW
  ON lokihouse.* TO lokihouse;

-- Retention reads the parts of the tables to find the days to drop.
GRANT SELECT ON system.parts TO lokihouse;
```

| Grant | Used for |
| --- | --- |
| `SELECT`, `INSERT` on the database | Queries, pushes, the statistics view. |
| `ALTER DELETE` | The delete API, the retention sweeper (lightweight `DELETE`, `DROP PARTITION`, `APPLY DELETED MASK`). |
| `CREATE DATABASE`, `CREATE TABLE`, `CREATE VIEW` | Every start runs `CREATE ... IF NOT EXISTS` for the database, the tables and the statistics view. ClickHouse checks the grant even when the object exists, so it cannot be left out. |
| `SELECT ON system.parts` | The retention sweeper lists the day partitions (`compactor.retention_enabled`). Not needed with `retention_enabled: false`. |

The user must not be `readonly`: lokihouse sets session settings such as
`async_insert` and writes data. `system.tables` and `system.columns` need no
grant; ClickHouse shows a user the entries it has access to.

**On a cluster** (`clickhouse.cluster` set) add, and issue the `GRANT`s
`ON CLUSTER` (and `CREATE USER ... ON CLUSTER` unless users are replicated):

```sql
GRANT CLUSTER ON *.* TO lokihouse;               -- DDL and deletes are ON CLUSTER
GRANT TABLE ENGINE ON Distributed TO lokihouse;  -- the Distributed tables
GRANT REMOTE ON *.* TO lokihouse;                -- the sweeper reads system.parts of all replicas
GRANT SELECT ON system.clusters TO lokihouse;    -- checks that the cluster exists
```

**For `lokihouse -schema.migrate`** (a one-off, see
[Upgrading to 0.9](../install/index.md#upgrading-to-09)) use a user with the grants
above plus `DROP TABLE` on the database, `SELECT` on `system.disks`
(the free disk space check) and, from schema version 2, `ALTER ADD COLUMN` and `ALTER ADD INDEX`. A separate, temporary user is a good choice:

```sql
CREATE USER lokihouse_migrate IDENTIFIED BY '...';
GRANT SELECT, INSERT, ALTER DELETE, CREATE DATABASE, CREATE TABLE, CREATE VIEW, DROP TABLE,
  ALTER ADD COLUMN, ALTER ADD INDEX
  ON lokihouse.* TO lokihouse_migrate;
GRANT SELECT ON system.parts TO lokihouse_migrate;
GRANT SELECT ON system.disks TO lokihouse_migrate;
-- on a cluster also the four cluster grants above, ON CLUSTER
```

Point `clickhouse.username` / `clickhouse.password_file` of the configuration used for the
migration at this user, then drop it with `DROP USER lokihouse_migrate`.

## `query`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `query.pushdown` | string | `on` | SQL push-down mode. `on`: range aggregations such as `count_over_time`, `rate` and `bytes_rate` run as one SQL query in ClickHouse, which returns only the series and steps; anything else (parsers, label filters after a parser, vector aggregation) runs in Loki's engine over the rows ClickHouse returns, and a leaf that cannot be pushed down falls back to it. `off`: always run Loki's engine over ClickHouse rows. `downstream`: map queries as `on` does but run every leaf in Loki's engine; for debugging. All three return the same results; `on` is much cheaper for aggregations over many lines. Any other value is a startup error. |

### `query.querier`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `query.querier.multi_tenant_queries_enabled` | bool | `false` | When `true`, a query whose `X-Scope-OrgID` names several tenants (`a|b`) is answered by querying each tenant and labelling results with `__tenant_id__` (Loki's multi-tenant queries). Multi-tenant queries always run on the plain engine (no SQL push-down). When `false` (the default), such a request is rejected with `multiple org IDs present`. This also applies to `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns`. |

## `compactor`

Settings for the background delete-processing and retention loops.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `compactor.compaction_interval` | duration | `10m` | How often the delete processor and the retention sweeper run. A failed run is logged and retried at the next interval. |
| `compactor.retention_enabled` | bool | `true` | Run the retention sweeper (Loki's default is `false`). See [Retention](#retention). |
| `compactor.delete_request_cancel_period` | duration | `24h` | How long a delete request waits before it is processed; it can be cancelled until then. |
| `compactor.delete_max_interval` | duration | `24h` | Default and upper bound of a delete request's `max_interval` parameter (a larger value is rejected with 400). |

Whether delete requests are accepted at all is set by `limits_config.deletion_mode`
(`filter-and-delete` by default; `filter-only` hides rows at query time without
removing them; `disabled` rejects delete requests). Retention is applied
by the sweeper; see [Retention](#retention).

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
| `ruler.evaluation_interval`, `ruler.poll_interval` | How often rules are evaluated (`1m`) and re-read from storage (`1m`). |
| `ruler.remote_write.enabled`, `ruler.remote_write.clients` | Send recording-rule samples with remote write, to one or more named clients (`clients: {prom: {url: ...}}`). |
| `ruler.alertmanager_client`, `ruler.notification_queue_capacity`, `ruler.notification_timeout` | TLS and authentication towards Alertmanager, and the notification queue. |
| `ruler.enabled_tenants`, `ruler.disabled_tenants` | Comma-separated tenant filters; empty means all tenants. |

## `ruler_storage`

The ruler only starts when this block is set. Two backends:

| Key | Description |
| --- | --- |
| `ruler_storage.backend` | `filesystem` (read/write rule files) or `local` (read-only rule files). |
| `ruler_storage.filesystem.dir` | Directory holding the rule files (for `backend: filesystem`). |
| `ruler_storage.local.directory` | Directory holding the rule files (for `backend: local`); rules are laid out as `<directory>/<tenant>/<file>`. |

The full set of keys is documented in the
[Loki ruler storage reference](https://grafana.com/docs/loki/latest/configuration/#ruler_storage).

> **Single-instance rule:** because lokihouse has no ruler ring, every
> instance with `ruler_storage` configured evaluates **all** rule groups.
> Configure the ruler on exactly one instance; otherwise alerts and
> recording-rule samples are duplicated.

## `limits_config`

Loki's `validation.Limits` block. Every key Loki accepts is accepted here,
with Loki's defaults and Loki's validation. The full set is documented in the
[Loki limits reference](https://grafana.com/docs/loki/latest/configure/#limits_config).

**Limits apply to every tenant.** lokihouse has no per-tenant overrides
(`runtime_config`, `-runtime-config.file`): a tenant cannot have its own
retention or rate limit. See [Differences from Loki](https://github.com/bzed/lokihouse/blob/main/docs/divergences.md).

lokihouse acts on the keys listed under [Retention](#retention) and in
[Operations → Limits](../operations.md#limits). The remaining keys are accepted
and validated, but have no effect: lokihouse has no ingesters, index gateway,
bloom components or query frontend for them to configure.

### Retention

Loki's default is to keep data forever (`retention_period: 0s`). To expire
data after a fixed time, set Loki's own retention keys. Retention is on by
default (`compactor.retention_enabled: true`, unlike Loki, where it defaults to
`false`); `false` turns the sweeper off and nothing expires.

| Key | Default | Description |
| --- | --- | --- |
| `compactor.retention_enabled` | `true` | Run the retention sweeper. Loki's default is `false`; `false` here keeps all data. |
| `limits_config.retention_period` | `0s` (forever) | How long to keep logs, for example `744h` for 31 days. |
| `limits_config.retention_stream` | none | Per-stream retention: a list of `{selector, priority, period}`. Of the rules whose selector matches a stream the highest `priority` wins, the shorter `period` on a tie; a stream matching no rule uses `retention_period`. |

```yaml
limits_config:
  retention_period: 744h            # 31 days for everything ...
  retention_stream:
    - selector: '{namespace="dev"}' # ... except dev, which is kept for a day
      priority: 1
      period: 24h
    - selector: '{app="audit"}'     # and audit logs, kept for a year
      priority: 2
      period: 8760h
```

How it works, and what differs from Loki:

- A sweeper runs on every lokihouse instance every `compactor.compaction_interval`
  and applies the *current* `retention_period` and `retention_stream`, so a
  change reaches data that is already stored. Nothing is stored per row. The
  tables are partitioned by UTC day. Nothing expires while no lokihouse
  instance runs, as with Loki's compactor.
- Expiry is on UTC midnight and counted from the log line's timestamp, not from
  the time of the push: a row stamped on UTC day D with a retention of N days
  is removed once D+N is today or earlier (00:00 UTC on day D+N). It is kept for
  between N-1 and N days after its timestamp (a 7-day retention keeps a row for
  6 to 7 days), and a line that is already older than that when it is pushed is
  removed by the next sweep.
- Days older than the longest configured period (`retention_period` and every
  `retention_stream` period; none if `retention_period` is `0s`) are dropped as
  whole day partitions. That is the only step when there are no
  `retention_stream` rules. Streams with a shorter period get lightweight
  deletes followed by `APPLY DELETED MASK`. Removed rows leave the disk at the
  end of the sweep.
- To free space at once, see the runbook in [Operations](../operations.md#retention).
- The longest period is 65535 days; longer values are capped.
- A push that spans many UTC days creates one partition per day, and at most
  60 are accepted per push (see Differences).

### Other keys lokihouse acts on

| Key | Default | Description |
| --- | --- | --- |
| `limits_config.deletion_mode` | `filter-and-delete` | `disabled`, `filter-only` or `filter-and-delete`; see [`compactor`](#compactor). |
| `limits_config.ingestion_rate_mb`, `ingestion_burst_size_mb` | `4`, `6` | Push rate limit, enforced per lokihouse instance. |
| `limits_config.max_query_series` | `500` | Hard cap on the number of series a metric query may return. `0` removes the cap. |
| `limits_config.max_query_lookback` | `0s` (no limit) | Maximum lookback for a query. |
| `limits_config.max_query_length` | `30d1h` | Longest time range of a query. |
| `limits_config.max_entries_limit_per_query` | `5000` | Largest accepted `limit` of a log query. |
| `limits_config.volume_enabled` | `true` | Whether `index/volume` is enabled. |

The push and query limits, and where each is enforced, are listed in
[Operations → Limits](../operations.md#limits).
