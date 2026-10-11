# Phase 3 status

Phase 3 of the lokihouse plan serves the rest of Loki's read and admin API from
ClickHouse: per-stream retention, `index/stats`, `index/volume`,
`index/volume_range`, `detected_labels`, `detected_fields`,
`detected_field/{name}/values`, `patterns`, the delete API (with query-time
filtering and physical deletion), `tail` over WebSocket, and `format_query`.
Every endpoint is compared against real Loki (commit `fbc3f077`) in the oracle.

## Exit criteria

Run on 2026-10-11, on `main` after task 9.

| Gate | Command | Result |
| --- | --- | --- |
| Unit tests | `make test` | ok (all packages) |
| Cluster unit tests | `make test-cluster` | ok (4-node ClickHouse cluster) |
| Oracle (all push-down modes) | `make oracle-all` | ok (off / downstream / on) |
| Cluster oracle | `make oracle-cluster` | ok (Loki vs 4-node lokihouse) |

Note: `detected_labels` oracle cases are intermittently flaky (Loki TSDB index
readiness race — see `docs/divergences.md`) and are retried until clean.

## Endpoints

| Endpoint | Implementation | Oracle coverage |
| --- | --- | --- |
| `POST /loki/api/v1/push` | retained (Phase 1) + per-stream retention (Task 1) | `Compare` on the fixture |
| `GET /loki/api/v1/index/stats` | new `chstore` query over `stream_stats_hourly` (Task 2) | `p3/stats` |
| `GET /loki/api/v1/index/volume` | new: `VolumeFromHours` (querier path) + hourly stats (Task 3) | `p3/volume` (shape exact, total within 30%) |
| `GET /loki/api/v1/index/volume_range` | new (Task 3) | `p3/volume_range` |
| `GET /loki/api/v1/detected_labels` | new `chstore` query (Task 4) | `p3/detected_labels` |
| `GET /loki/api/v1/detected_fields` | new (Task 4) | `p3/detected_fields` |
| `GET /loki/api/v1/detected_field/{name}/values` | new (Task 4) | `p3/detected_field_values` |
| `GET/POST /loki/api/v1/patterns` | new `internal/patterns` (one Drain per stream/level, query-time) (Task 5) | `p3/patterns` |
| `PUT/POST /loki/api/v1/delete` | copied `pkg/compactor/deletion` request handlers; store in `delete_requests` (Task 6) | `deletesOracle` (add/list) |
| `GET /loki/api/v1/delete` | copied (Task 6) | `deletesOracle` (list) |
| `DELETE /loki/api/v1/delete` | copied (Task 6) | `deletesOracle` (cancel) |
| `GET/POST /loki/api/v1/tail`, `/api/prom/tail` | Loki `tail.Querier`/`Tailer`/`TailHandler` reused; one in-process polling `Ingester` (Task 8) | `tailOracle` (WebSocket) |
| `GET/POST /loki/api/v1/format_query` | copied `pkg/loki/format_query_handler.go` (Task 9) | `formatQueryOracle` (whole corpus) |

Query-time delete filtering and physical deletion are internal: the querier
hides rows matching a pending delete (Loki's `util/deletion` pipeline), and a
processor rewrites the physical rows after `delete_request_cancel_period`
(Tasks 6–7). Covered by `deletesOracle` (count drops after delete) and
`internal/deletes` unit tests.

## Operations

- **Delete processing loop** — `deletes.Processor.Run` wakes on
  `compactor.compaction_interval` and, after `delete_request_cancel_period`,
  rewrites the rows each pending delete request covers (lightweight DELETE,
  `lightweight_deletes_sync=2`; `ON CLUSTER` on a cluster). Requests are
  marked `processed` afterwards.
- **Tail** — the in-process ingester's tail client polls ClickHouse every
  1 s (`Ingester.Poll`, default) for rows with `ts` newer than the previous
  poll minus a 10 s lookback (`Ingester.Lookback`, default) for late
  arrivals, deduplicated by a seen-set. Each poll re-scans the lookback
  window (bounded by the highest ts already sent), so a busy stream pays a
  10 s re-read per second per tail; the seen-set makes this idempotent.
  Entries older than the window, or before the tail start, are not sent
  live (range queries reach them).
- **Patterns** — `patterns.Querier.MaxLines` caps the rows read per pattern
  query; 0 means the built-in default of 1,000,000 rows. When the cap
  truncates a query the querier logs a warning (no response field signals
  truncation). The per-stream Drain training is rebuilt at query time, so
  there is no persistent training state to operate.

## Backfilling `stream_stats_hourly`

Databases created before Phase 3 have rows in `logs` but no `stream_stats_hourly`
data, so `index/stats`, `index/volume` and retention are empty until the stats
are computed. Run this once per database (on one replica of every shard on a
cluster, against the `_local` tables):

```sql
INSERT INTO <db>.stream_stats_hourly
SELECT tenant, stream_id, toStartOfHour(toDateTime(ts, 'UTC')) AS hour, retention_days,
       count(), sum(length(line) + 8 * length(mapKeys(structured_metadata))), min(ts), max(ts)
FROM <db>.logs WHERE ts < <migration time> GROUP BY tenant, stream_id, hour, retention_days
```

After the migration time, new rows fill the table through the normal write path.

## Known issues

See `docs/divergences.md`: delete request IDs; stats/volume ignoring deletes;
the 5-minute Loki delete-list cache; and tail delivery (poll vs push).
