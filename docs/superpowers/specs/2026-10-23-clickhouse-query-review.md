# ClickHouse schema and query review (2026-10-23)

Findings of a review of `internal/chstore/schema.sql`, the query builders in
`internal/chstore` and Tier A in `internal/pushdown`, measured on
clickhouse-server 26.9.14 with 2 M synthetic JSON lines in 50 streams, the
production `logs` DDL, rows read taken from `system.query_log`.

## Measured facts

- `line_text` (`text(tokenizer = 'splitByNonAlpha')`) answers `line LIKE '%x%'`
  by dictionary scan (8 192 of 2 M rows read) when the alphanumeric needle has
  ≥ 4 characters (`text_index_like_min_pattern_length = 4`). Shorter needles
  and `position()` read every row.
- Single-token `line ILIKE '%x%'` uses the index; a multi-word ILIKE does not.
- OR of predicates on the same text index skips granules. OR across different
  indexes is unreliable: `line LIKE … OR mapContainsKey(structured_metadata, k)`
  skipped, `hasToken(line, …) OR structured_metadata[k] = v` did not.
- `INDEX … mapValues(structured_metadata) TYPE text(tokenizer = 'array')`
  answers `structured_metadata[k] = v` (8 192 of 2 M rows); the `mapKeys`
  equivalent is ~1 KiB and answers `mapContainsKey`. The values index is about
  the size of the column for unique-per-line values (worst case).
- An `IN` list of 15 000 stream IDs (306 KB of SQL) fails with
  `Code: 62 … Max query size exceeded`; clickhouse-go binds `?` client side.
- Tier B `SelectSamples` (`ORDER BY ts`, no LIMIT) used 750 MiB for 2.2 M rows;
  `max_bytes_before_external_sort` did not change it. Each keyset chunk of
  `SelectLogs` re-runs a top-N over the whole remaining range.
- `BucketCounts`: `sum(length(line))` raised bytes read from 49 MiB to 125 MiB;
  `GROUP BY structured_metadata` with a per-line `trace_id` produced one group
  per row (2 M groups, 420 MiB); grouping by `structured_metadata['pod']` gave
  133 k groups and 70 MiB.

## Required changes

1. **Stream ID sets** of any size work in every query that filters `logs` or
   `stream_stats_hourly` by stream (no `max_query_size` failure).
2. **Case-insensitive line filters** `|~ "(?i)…"` push a necessary
   `line ILIKE` condition when the literal is ASCII and contains no `k`, `K`,
   `s` or `S` (Go folds those to U+212A and U+017F).
3. **Pushdown tests prove skipping** (rows read), not only that EXPLAIN names
   the index.
4. **Regex stream matchers** that pass `RegexExact` are also evaluated in SQL
   over `streams` (Go keeps re-checking them).
5. **Tier B reads in time windows** sized from `stream_stats_hourly`, so sample
   queries hold one window in memory and log-query keyset chunks re-scan only
   their window.
6. **Tier A** reads `line` only for `bytes_over_time`/`bytes_rate`, uses
   `do_not_merge_across_partitions_select_final = 1`, and for
   `sum [by (…)] (<eligible range aggregation>)` groups only by the
   structured-metadata keys the grouping can see.
7. **Structured-metadata indexes** `sm_keys`, `sm_vals` and the `line_ascii_escape`
   flag on `logs` (schema version 3). New databases get them from `schema.sql`;
   version 2 databases are migrated offline by `lokihouse -schema.migrate`
   with `ALTER TABLE` (no copy). Stored rows are not backfilled: they hold no
   `\u`-escaped data, and new parts are indexed as they are written or merged.
8. **Pre-parser label filters** (`{…} | trace_id="x"`) push
   `structured_metadata[k] = v` (Loki's `ExtractTestableLabelMatchers`) and,
   when exact, keep the query in Tier A.
9. **Post-parser string equality filters** after `| json` / `| logfmt` push a
   necessary `line LIKE` condition built from the value's verbatim runs, OR
   `mapContainsKey(structured_metadata, k)`, OR `line_ascii_escape`: a
   materialized `Bool` column (with a `minmax` index) that is true when the
   line contains a `\u` escape of an ASCII letter or digit, which would hide
   the value's text from `LIKE`. Measured: the three-way OR still skips
   granules (16 384 of 2 M rows read); the column is ~100 bytes for 2 M rows,
   and Go's common `\u003c` (`<`) does not set it. No divergence remains.

Every pushed condition is necessary (Go re-applies the pipeline) unless it is
used by Tier A, where it must be exact.
