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
