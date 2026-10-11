# Known divergences from Loki

Spec §7.4: every accepted difference, with its reason. Target: empty apart from these.

## Tie order inside one stream and timestamp (Phase 1)
Loki keeps insertion order; lokihouse cannot. The oracle compares such groups as multisets.

## index/stats and index/volume (Phase 3)
- **Granularity:** Loki pro-rates per chunk (bounds = first/last entry of the chunk); lokihouse pro-rates per hourly bucket (bounds = first/last entry in that hour). Ranges that cover all data in a bucket/chunk agree; ranges cutting through a chunk can differ.
- **Duplicates:** Loki's head block drops exact duplicates before counting; the materialized view counts every inserted row, so client retries of the same push count twice.
- **Bytes (index/stats):** Loki adds each chunk's structured-metadata symbol table; lokihouse counts `len(line) + 8 × pairs` only (≈1.5% for the oracle fixture with repeated JSON metadata).
- **chunks:** the number of overlapping hourly buckets, not Loki chunks.
- **Volume values (index/volume, index/volume_range):** these measure *different* Loki code paths. A live volume query goes through Loki's **querier** → `TSDBIndex.Volume`, which sums `KB << 10` per **flushed chunk**, where `KB = round(UncompressedSize / 1024)` — i.e. the on-disk block size of each 10m chunk, rounded to the nearest KiB. Lokihouse's plan (spec §5) instead copies the **ingester**'s `getVolume` accounting: raw head-block bytes `len(line) + 8 × pairs`, aggregated per **hourly** bucket and pro-rated per Loki's factor rule. The two therefore differ by ~15–30% (Loki's KiB rounding per sub-hour chunk + on-disk encoding packs these short lines tighter than `length(line)`); the difference is structural, not a bug. The oracle verifies the response *shape* exactly (resultType, metric set, timestamps, point count) and the per-metric **volume total** within 30%.

## Delete requests (Phase 3)
- **Request IDs:** lokihouse uses 8 random hex characters; the format and the IDs differ from Loki.
- **Stats and volume after a delete:** `index/stats`, `index/volume`, patterns and `detected_labels` ignore deletes (Loki's index stats also count unprocessed deletes; after processing, Loki's rewritten chunks shrink while lokihouse's MV does not).
- **Query-time delete visibility (identical to Loki, but timing-sensitive):** both stores fetch a tenant's *pending* delete requests at query time and hide matching lines via Loki's `util/deletion` filter pipeline. Loki caches a tenant's delete list for 5 minutes from its first lookup, so a delete created after a query on that tenant is not visible for up to 5 minutes (Loki); lokihouse's store is authoritative per query (no such cache), so lokihouse can appear "fresher". The oracle works around the cache by using a fresh tenant that is never queried before the delete is created.
- **Rows written between delete submission and processing:** the processor re-resolves the request's selector at processing time, so rows that land inside the request's `[start, end]` window *after* the request was created are also deleted. They were not hidden by the query-time filter before they existed, so this is a (narrow, intentional) difference from "exactly the rows query-time filtering hid"; it matches Loki's compactor, which also re-reads the index when processing.
- **detected_labels oracle flakiness:** Loki's `detected_labels` reads from the TSDB index, which is built asynchronously after the ingester flushes; right after a flush the store can briefly return an empty set. The oracle waits until the endpoint is non-empty (`waitDetectedLabelsReady`) and the full oracle is retried (up to 3x) on this known race; lokihouse is not affected (it reads the rows directly).

## Tail (Phase 3)
- **Tail delivery:** Loki streams entries to a `/loki/api/v1/tail` WebSocket client as its ingesters receive them (push). lokihouse has no ingester to push from, so its tail client *polls* ClickHouse (every second by default) for rows with `ts` newer than now minus a 10 s lookback for late arrivals, deduplicated by a seen-set. Consequently an entry pushed later with a timestamp older than the current poll window, or with a timestamp before the tail started, is not sent live (it is reachable by a normal range query instead). History (rows before the tail start) is served by Loki's `Tailer` history path in both stores, not by the live client.

## Ruler (Phase 4)
- **No ruler ring (sharding):** Loki distributes rule groups over a ring of rulers; lokihouse has no ring. `ruler.enable_sharding: true` and `ruler.evaluation.mode: remote` are rejected at startup, and `/ruler/ring` is not served. Every lokihouse instance with `ruler_storage` configured evaluates *all* rule groups, so configure it on exactly one instance; otherwise alerts and recording-rule samples are duplicated.
- **`ruler.rule_path` default:** `rules-temp` relative to the working directory instead of Loki's `/rules` (Loki's single binary uses `<common.path_prefix>/rules-temp`; lokihouse has no `path_prefix`).
- **Namespaces containing `/`:** Loki's router (gorilla mux on the decoded path) splits `/loki/api/v1/rules/a%2Fb` into namespace `a` and group `b`; lokihouse's `ServeMux` keeps `a/b` as one namespace. Other escaped characters (spaces, `%`, unicode) behave the same in both.
- **Ruler metrics:** collected in a private registry and not exposed (lokihouse has no `/metrics` endpoint before Phase 5).
