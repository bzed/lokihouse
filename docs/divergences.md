# Known divergences from Loki

Every accepted observable difference between lokihouse and Loki, with its
reason. Entries are grouped by topic.

## Ingestion

*Implemented in [phase 5](https://github.com/bzed/lokihouse/blob/main/docs/phase5-status.md); see the status file for background.*

- **`ingestion_rate_strategy: global`:** Loki's `global` strategy divides the limit over the healthy distributors in the ring, so the ring as a whole enforces the limit. lokihouse has no distributor ring; `ingestion_rate_strategy` is not honoured and the limit is always enforced *per lokihouse instance* (the `local` behaviour). With several lokihouse instances behind a load balancer, each enforces the full `ingestion_rate_mb`, so the cluster total can be N times the configured limit.
- **Per-policy rate limits:** Loki supports `policy_stream_mapping` to give a policy its own rate limit and burst. lokihouse does not support policies; only the tenant-wide `ingestion_rate_mb` / `ingestion_burst_size_mb` are enforced.

## Query

*Implemented in [phase 1](https://github.com/bzed/lokihouse/blob/main/docs/phase1-status.md), [phase 5](https://github.com/bzed/lokihouse/blob/main/docs/phase5-status.md); see the status file for background.*

- **Tie order inside one stream and timestamp:** Loki keeps insertion order; lokihouse cannot. The oracle compares such groups as multisets.
- **`/loki/api/v1/series` `match` parameter:** Loki's own docs show the single-matcher form `?match={...}`, which `loghttp.ParseSeriesQuery` reads from `r.Form["match"]` — populated only after `ParseForm`. Loki's handler relies on the net/http server (or a prior parse) to fill `r.Form`; lokihouse calls `r.ParseForm()` explicitly in its `series` handler so the `match` key is honoured. The `match[]` form works in both stores without `ParseForm` (Loki reads that key straight from the URL query), so existing clients are unaffected.
- **Query stats (`totalBytesProcessed`, `totalLinesProcessed`):** Loki's `totalBytesProcessed` sums the *decompressed* chunk bytes from the ingester's in-memory chunks; lokihouse's ClickHouse `WithProgress` reports the bytes the server read over the wire (typically smaller). `totalLinesProcessed` likewise counts the rows ClickHouse read (Loki counts the lines it decompressed); ClickHouse may skip granules Loki would have decompressed, so it is typically smaller too. `totalPostFilterLines` matches Loki: one per log line or sample that passes the query pipeline (plain engine; Tier A push-down aggregates in SQL and reports no post-filter lines).
- **Multi-tenant queries** (`query.querier.multi_tenant_queries_enabled`): the fan-out querier reuses Loki's `pkg/querier/multi_tenant_querier.go` logic and covers the log and sample select paths plus `label/values*` and `series`. Loki's querier also fans out `index/stats`, `index/volume*`, `detected_labels` and `detected_metric_names`; lokihouse does not — those endpoints keep resolving a single `X-Scope-OrgID` tenant (the `tenant` package turns a comma-separated header into `ErrTooManyOrgIDs` on them). With the flag on, multi-tenant queries always run on the plain engine (no ClickHouse push-down, which is single-tenant by construction).

## Tail

*Implemented in [phase 3](https://github.com/bzed/lokihouse/blob/main/docs/phase3-status.md); see the status file for background.*

- **Delivery:** Loki streams entries to a `/loki/api/v1/tail` WebSocket client as its ingesters receive them (push). lokihouse has no ingester to push from, so its tail client *polls* ClickHouse (every second by default) for rows with `ts` newer than now minus a 10 s lookback for late arrivals, deduplicated by a seen-set. Consequently an entry pushed later with a timestamp older than the current poll window, or with a timestamp before the tail started, is not sent live (it is reachable by a normal range query instead). History (rows before the tail start) is served by Loki's `Tailer` history path in both stores, not by the live client.

## Index statistics and volume (`index/stats`, `index/volume`, `index/volume_range`)

*Implemented in [phase 3](https://github.com/bzed/lokihouse/blob/main/docs/phase3-status.md); see the status file for background.*

- **Granularity:** Loki pro-rates per chunk (bounds = first/last entry of the chunk); lokihouse pro-rates per hourly bucket (bounds = first/last entry in that hour). Ranges that cover all data in a bucket/chunk agree; ranges cutting through a chunk can differ.
- **Duplicates:** Loki's head block drops exact duplicates before counting; the materialized view counts every inserted row, so client retries of the same push count twice.
- **Bytes (`index/stats`):** Loki adds each chunk's structured-metadata symbol table; lokihouse counts `len(line) + 8 × pairs` only (≈1.5% for the oracle fixture with repeated JSON metadata).
- **`chunks`:** the number of overlapping hourly buckets, not Loki chunks.
- **Volume values:** these measure *different* Loki code paths. A live volume query goes through Loki's **querier** → `TSDBIndex.Volume`, which sums `KB << 10` per **flushed chunk**, where `KB = round(UncompressedSize / 1024)` — i.e. the on-disk block size of each 10m chunk, rounded to the nearest KiB. lokihouse instead copies the **ingester**'s `getVolume` accounting: raw head-block bytes `len(line) + 8 × pairs`, aggregated per **hourly** bucket and pro-rated per Loki's factor rule. The two therefore differ by ~15–30% (Loki's KiB rounding per sub-hour chunk + on-disk encoding packs these short lines tighter than `length(line)`); the difference is structural, not a bug. The oracle verifies the response *shape* exactly (resultType, metric set, timestamps, point count) and the per-metric **volume total** within 30%.
- **After a delete:** `index/stats`, `index/volume`, patterns and `detected_labels` ignore deletes (Loki's index stats also count unprocessed deletes; after processing, Loki's rewritten chunks shrink while lokihouse's materialized view does not).

## Metadata

*Implemented in [phase 3](https://github.com/bzed/lokihouse/blob/main/docs/phase3-status.md); see the status file for background.*

- **`detected_labels` after a Loki flush:** Loki answers `detected_labels` from ingester memory and, for flushed data, from the TSDB index, which only becomes readable once it is built and synced; for minutes after a flush Loki returns an empty set. lokihouse reads the stored rows and has no such gap. The oracle therefore compares `detected_labels` before it forces Loki's flush (`detectedLabelsCases`).

## Delete requests

*Implemented in [phase 3](https://github.com/bzed/lokihouse/blob/main/docs/phase3-status.md); see the status file for background.*

- **Request IDs:** lokihouse uses 8 random hex characters; the format and the IDs differ from Loki.
- **Query-time delete visibility (identical to Loki, but timing-sensitive):** both stores fetch a tenant's *pending* delete requests at query time and hide matching lines via Loki's `util/deletion` filter pipeline. Loki caches a tenant's delete list for 5 minutes from its first lookup, so a delete created after a query on that tenant is not visible for up to 5 minutes (Loki); lokihouse's store is authoritative per query (no such cache), so lokihouse can appear "fresher". The oracle works around the cache by using a fresh tenant that is never queried before the delete is created.
- **Rows written between delete submission and processing:** the processor re-resolves the request's selector at processing time, so rows that land inside the request's `[start, end]` window *after* the request was created are also deleted. They were not hidden by the query-time filter before they existed, so this is a (narrow, intentional) difference from "exactly the rows query-time filtering hid"; it matches Loki's compactor, which also re-reads the index when processing.

## Ruler

*Implemented in [phase 4](https://github.com/bzed/lokihouse/blob/main/docs/phase4-status.md), [phase 5](https://github.com/bzed/lokihouse/blob/main/docs/phase5-status.md); see the status file for background.*

- **No ruler ring (sharding):** Loki distributes rule groups over a ring of rulers; lokihouse has no ring. `ruler.enable_sharding: true` and `ruler.evaluation.mode: remote` are rejected at startup, and `/ruler/ring` is not served. Every lokihouse instance with `ruler_storage` configured evaluates *all* rule groups, so configure it on exactly one instance; otherwise alerts and recording-rule samples are duplicated.
- **`ruler.rule_path` default:** `rules-temp` relative to the working directory instead of Loki's `/rules` (Loki's single binary uses `<common.path_prefix>/rules-temp`; lokihouse has no `path_prefix`).
- **Ruler metrics:** collected in a private registry and exposed through `/metrics` alongside the default registry.
- **WAL directory guard:** with remote write enabled, Loki wipes `ruler.wal.dir` (`os.RemoveAll`) at every start. lokihouse refuses to start when that directory is empty or contains the working directory, the rule storage directory or `rule_path`; Loki would delete them.

## Observability

*Implemented in [phase 5](https://github.com/bzed/lokihouse/blob/main/docs/phase5-status.md); see the status file for background.*

- **HTTP request metrics `method` label:** dskit labels `loki_request_duration_seconds` and `loki_inflight_requests` with the raw request method, so a client can create one series per invented method (unbounded cardinality from untrusted input). lokihouse keeps the standard methods (`GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `CONNECT`, `OPTIONS`, `TRACE`) and labels every other method `other`. Dashboards that only use standard methods are unaffected.
