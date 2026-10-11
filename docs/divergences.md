# Known divergences from Loki

Spec §7.4: every accepted difference, with its reason. Target: empty apart from these.

## Tie order inside one stream and timestamp (Phase 1)
Loki keeps insertion order; lokihouse cannot. The oracle compares such groups as multisets.

## index/stats and index/volume (Phase 3)
- **Granularity:** Loki pro-rates per chunk (bounds = first/last entry of the chunk); lokihouse pro-rates per hourly bucket (bounds = first/last entry in that hour). Ranges that cover all data in a bucket/chunk agree; ranges cutting through a chunk can differ.
- **Duplicates:** Loki's head block drops exact duplicates before counting; the materialized view counts every inserted row, so client retries of the same push count twice.
- **Bytes:** Loki adds each chunk's structured-metadata symbol table; lokihouse counts `len(line) + 8 × pairs` only (≈1.5% for the oracle fixture with repeated JSON metadata).
- **chunks:** the number of overlapping hourly buckets, not Loki chunks.
