-- Retention is not stored with the data: the sweeper (internal/retention) drops whole UTC day
-- partitions and deletes expired days of single streams, following the current configuration.
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Table "logs"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant              LowCardinality(String),
  stream_id           UInt64,
  ts                  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  line                String CODEC(ZSTD(3)),
  structured_metadata Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  entry_hash          UInt64 MATERIALIZED cityHash64(line, mapSort(structured_metadata)), -- Loki dedup identity
  INDEX line_text line TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
  -- True when the line hides an ASCII letter or digit behind a \u escape (see ParsedFilterSQL).
  line_ascii_escape   Bool MATERIALIZED match(line, '\\\\u00(3[0-9]|4[1-9a-fA-F]|5[0-9aA]|6[1-9a-fA-F]|7[0-9aA])'),
  INDEX sm_keys mapKeys(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1,
  INDEX sm_vals mapValues(structured_metadata) TYPE text(tokenizer = 'array') GRANULARITY 1,
  INDEX line_ascii_escape_idx line_ascii_escape TYPE minmax GRANULARITY 1
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY toDate(ts)
ORDER BY (tenant, stream_id, ts, entry_hash);

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Table "streams"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant         LowCardinality(String),
  day            Date,
  stream_id      UInt64,
  labels         Map(LowCardinality(String), String),
  labels_str     String
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY day
ORDER BY (tenant, day, stream_id);

-- stream_stats_hourly: per (tenant, stream, hour) entry count, bytes and first/last entry
-- timestamp, fed at insert time by the MV below. It serves index/stats and index/volume
-- with Loki's chunk pro-rating rule applied per hourly bucket. Bytes follow Loki's head
-- block accounting: len(line) + 8 per structured-metadata pair (no symbol table).
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Table "stream_stats_hourly"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant         LowCardinality(String),
  stream_id      UInt64,
  hour           DateTime('UTC'),
  entries        SimpleAggregateFunction(sum, UInt64),
  bytes          SimpleAggregateFunction(sum, UInt64),
  min_ts         SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
  max_ts         SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = {{.Engine "AggregatingMergeTree"}}
PARTITION BY toDate(hour)
ORDER BY (tenant, stream_id, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DBName}}.{{.Table "stream_stats_hourly"}}_mv{{with .OnCluster}} {{.}}{{end}}
TO {{.DBName}}.{{.Table "stream_stats_hourly"}} AS
SELECT tenant, stream_id, toStartOfHour(toDateTime(ts, 'UTC')) AS hour,
       toUInt64(count()) AS entries,
       toUInt64(sum(length(line) + 8 * length(mapKeys(structured_metadata)))) AS bytes,
       min(ts) AS min_ts, max(ts) AS max_ts
FROM {{.DBName}}.{{.Table "logs"}}
GROUP BY tenant, stream_id, hour;

-- delete_requests: Loki delete API requests. Rows are never updated in place: a status
-- change inserts a newer version and readers take argMax(…, version). Merges must keep
-- the same row, so version is the engine's version column: without it they keep the
-- last-inserted row, which with skewed clocks across instances is not the highest version.
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Table "delete_requests"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant     LowCardinality(String),
  request_id String,
  query      String,
  start_ms   Int64,
  end_ms     Int64,
  created_ms Int64,
  status     LowCardinality(String), -- received | processed | cancelled
  version    UInt64
) ENGINE = {{.Engine "ReplacingMergeTree" "version"}}
ORDER BY (tenant, request_id);
{{if .Cluster}}
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Dist "stream_stats_hourly"}}{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.{{.Table "stream_stats_hourly"}}
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, {{lit (.Table "stream_stats_hourly")}}, cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Dist "logs"}}{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.{{.Table "logs"}}
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, {{lit (.Table "logs")}}, cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Dist "streams"}}{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.{{.Table "streams"}}
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, {{lit (.Table "streams")}}, cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Dist "delete_requests"}}{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.{{.Table "delete_requests"}}
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, {{lit (.Table "delete_requests")}}, cityHash64(tenant, request_id));
{{end}}
