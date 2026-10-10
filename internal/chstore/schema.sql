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
{{if .Cluster}}
CREATE TABLE IF NOT EXISTS {{.DBName}}.logs{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.logs_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'logs_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.streams{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.streams_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'streams_local', cityHash64(tenant, stream_id));
{{end}}
