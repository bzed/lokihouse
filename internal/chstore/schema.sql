CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "logs"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant              LowCardinality(String),
  stream_id           UInt64,
  ts                  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  line                String CODEC(ZSTD(3)),
  structured_metadata Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  line_hash           UInt64 MATERIALIZED cityHash64(line),
  retention_days      UInt16 DEFAULT 31,
  INDEX line_text line TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY toDate(ts)
ORDER BY (tenant, stream_id, ts, line_hash)
TTL toDateTime(ts) + toIntervalDay(retention_days);

CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "streams"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant     LowCardinality(String),
  day        Date,
  stream_id  UInt64,
  labels     Map(LowCardinality(String), String),
  labels_str String
) ENGINE = {{.Engine "ReplacingMergeTree"}}
PARTITION BY toYYYYMM(day)
ORDER BY (tenant, day, stream_id);
{{if .Cluster}}
CREATE TABLE IF NOT EXISTS {{.DBName}}.logs{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.logs_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'logs_local', cityHash64(tenant, stream_id));

CREATE TABLE IF NOT EXISTS {{.DBName}}.streams{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.streams_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'streams_local', cityHash64(tenant, stream_id));
{{end}}
