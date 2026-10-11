-- Retention: TTL is whole UTC days and retention_days is part of the partition key, so every
-- row of a partition expires at the same midnight and ClickHouse drops whole partitions
-- (ttl_only_drop_parts) instead of rewriting parts. retention_days = 0 keeps rows forever
-- (Loki's default). The expiry is computed in Date32 and capped at 2106-01-01 because the
-- TTL is a 32-bit DateTime that would otherwise wrap around for long retentions.
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "logs"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant              LowCardinality(String),
  stream_id           UInt64,
  ts                  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  line                String CODEC(ZSTD(3)),
  structured_metadata Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  entry_hash          UInt64 MATERIALIZED cityHash64(line, mapSort(structured_metadata)), -- Loki dedup identity
  retention_days      UInt16 DEFAULT 0,
  INDEX line_text line TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY (toDate(ts), retention_days)
ORDER BY (tenant, stream_id, ts, entry_hash)
TTL {{ttl "toDate32(ts)"}}
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "streams"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant         LowCardinality(String),
  day            Date,
  stream_id      UInt64,
  labels         Map(LowCardinality(String), String),
  labels_str     String,
  retention_days UInt16 DEFAULT 0
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY (day, retention_days)
ORDER BY (tenant, day, stream_id)
TTL {{ttl "toDate32(day)"}}
SETTINGS ttl_only_drop_parts = 1;

-- stream_stats_hourly: per (tenant, stream, hour) entry count, bytes and first/last entry
-- timestamp, fed at insert time by the MV below. It serves index/stats and index/volume
-- with Loki's chunk pro-rating rule applied per hourly bucket. Bytes follow Loki's head
-- block accounting: len(line) + 8 per structured-metadata pair (no symbol table).
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "stream_stats_hourly"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant         LowCardinality(String),
  stream_id      UInt64,
  hour           DateTime('UTC'),
  retention_days UInt16,
  entries        SimpleAggregateFunction(sum, UInt64),
  bytes          SimpleAggregateFunction(sum, UInt64),
  min_ts         SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
  max_ts         SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = {{.Engine "AggregatingMergeTree"}}
PARTITION BY (toDate(hour), retention_days)
ORDER BY (tenant, stream_id, hour)
TTL {{ttl "toDate32(hour)"}}
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DBName}}.{{.Local "stream_stats_hourly"}}_mv{{with .OnCluster}} {{.}}{{end}}
TO {{.DBName}}.{{.Local "stream_stats_hourly"}} AS
SELECT tenant, stream_id, toStartOfHour(toDateTime(ts, 'UTC')) AS hour, retention_days,
       toUInt64(count()) AS entries,
       toUInt64(sum(length(line) + 8 * length(mapKeys(structured_metadata)))) AS bytes,
       min(ts) AS min_ts, max(ts) AS max_ts
FROM {{.DBName}}.{{.Local "logs"}}
GROUP BY tenant, stream_id, hour, retention_days;

-- delete_requests: Loki delete API requests. Rows are never updated in place: a status
-- change inserts a newer version and readers take argMax(…, version).
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "delete_requests"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant     LowCardinality(String),
  request_id String,
  query      String,
  start_ms   Int64,
  end_ms     Int64,
  created_ms Int64,
  status     LowCardinality(String), -- received | processed | cancelled
  version    UInt64
) ENGINE = {{.Engine "ReplacingMergeTree"}}
ORDER BY (tenant, request_id);
{{if .Cluster}}
CREATE TABLE IF NOT EXISTS {{.DBName}}.stream_stats_hourly{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.stream_stats_hourly_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'stream_stats_hourly_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.logs{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.logs_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'logs_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.streams{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.streams_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'streams_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.delete_requests{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.delete_requests_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'delete_requests_local', cityHash64(tenant, request_id));
{{end}}
