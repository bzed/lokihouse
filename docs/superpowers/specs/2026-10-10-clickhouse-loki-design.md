# Lokihouse — Loki API on ClickHouse: Analysis & Design

Date: 2026-10-10 · Loki analysed at commit `fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1` (main, 2026-10-10) · ClickHouse 26.9

## 1. Goal

A single service ("lokihouse") that is wire-compatible with Grafana Loki for everything a client
can observe (push APIs, LogQL query APIs, metadata APIs, tail, ruler, deletes, retention), but
stores and processes logs in ClickHouse. Wherever ClickHouse can do the work natively (indexing,
filtering, aggregation, retention, deduplication, pre-aggregation), it does. Feature equality is
proven by tests, not asserted.

**Non-goals:** Kubernetes operator, Helm/jsonnet, Loki's storage layer (chunks, TSDB index, WAL,
ingesters, compactor service, bloom filters, dataobj, Kafka ingest path, engine v2), rings and
memberlist, query-frontend sharding/splitting/results-cache (ClickHouse parallelises
internally), querytee, canary, Loki UI.

## 2. What Loki actually consists of (measured)

Non-test Go LoC at the analysed commit, excluding generated protobuf:

| Package | LoC | Needed? |
|---|---:|---|
| `pkg/logql/syntax` (lexer, goyacc grammar 525 lines, AST, prettier) | 8 784 | **Yes**, verbatim |
| `pkg/logql/log` (+ json/logfmt/pattern sub-pkgs: pipeline stages, parsers, `line_format`/`label_format` with Go `text/template` + sprig, `ip()`, label filters, unwrap/extraction) | 11 286 | **Yes**, verbatim |
| `pkg/logql` engine/evaluators (range vectors, vector aggs, binops, sketches, `first/last_over_time`, step evaluator) | ~11 000 | **Yes** |
| `pkg/logql` sharding/downstream/rangemapper/shardmapper | ~8 000 | No: ClickHouse runs queries in parallel itself |
| `pkg/loghttp` + `pkg/loghttp/push` (request parsing, Loki-JSON/proto/OTLP push decoding) | 3 862 + ~2 500 | **Yes** |
| `pkg/util/marshal` (response JSON, v1 & legacy) | 891 | **Yes** |
| `pkg/distributor` (validation, rate limits; the rest is ring/ingester fan-out) | 5 099 | ~1 000 relevant |
| `pkg/querier` (+queryrange 12.8k) | 22 449 | ~2 000 relevant (detected fields/labels, API glue) |
| `pkg/ruler` | 9 425 | Mostly reusable (Prometheus rule manager + LogQL query func) |
| `pkg/pattern` (Drain clustering) | 5 014 | Drain algorithm reusable for on-read patterns |
| `pkg/engine` (v2), `pkg/dataobj`, `pkg/storage`, `pkg/ingester`, `pkg/bloom*`, `pkg/compactor`, `pkg/kafka*` | >150 000 | No (storage layer) |

**The LogQL + receiving layer is not small.** The parts a re-implementation must replicate
exactly come to about 32k Go LoC: syntax, pipeline, evaluator, HTTP parsing and push decoding.
About 30k Go LoC of tests back them. The hardest parts to replicate exactly are:

* `line_format` / `label_format`: Go `text/template` semantics plus ~50 Loki/sprig functions.
* Parser edge cases: JSON flattening (`a_b_c`, sanitisation, `_extracted` suffix on conflicts,
  `__error__` labels), logfmt quoting and escaping, the pattern parser, `unpack`, `decolorize`.
* The Go regexp dialect: Loki uses `grafana/regexp`, a Go regexp fork. Its corner cases differ
  from both Rust `regex` and C++ RE2.
* Number formatting in responses: Go `strconv` / Prometheus formatting of floats and durations.
* Iterator tie-breaking rules, deduplication, and step/range boundary arithmetic (left-open
  windows `(t-range, t]`, lookback).

## 3. Decision: Go, importing Loki's packages as a library (no Rust rewrite)

| Criterion | Go, reuse Loki packages | Rust from scratch |
|---|---|---|
| New code to write for parity | ~12–15k LoC (adapter + SQL translator + API glue + tests) | ~45–60k LoC (parser, pipeline, template engine + sprig subset, evaluator, API, ruler, tests) |
| Semantic equality | **By construction**: the same parser, pipeline, and evaluator code runs. Only the storage adapter and SQL push-down can diverge, and those are tested differentially. | Must be re-earned for every function, template helper, regex corner, float format. Long tail of divergences. |
| Oracle for tests | In-process: the Loki engine fed from an in-memory querier, plus a real Loki binary built from the pinned commit | Real Loki only (slower, coarser) |
| Upstream LogQL changes | `go get` a new commit, re-run the differential suite | Port each change by hand |
| Performance | The heavy lifting runs in ClickHouse. Go only post-processes rows that are not pushed down. | Faster post-processing, but that path is rare after push-down |
| Sonnet 5.5 cost (est.) | **~$250–450** | ~$1 500–3 000 plus a long equality bug tail |

**Verdict:** write the service in Go and import `github.com/grafana/loki/v3` at a pinned commit.
Rust only pays off in the Go post-processing path. That path shrinks as push-down grows, and a
rewrite would trade a measurable speed gain for equality risk that cannot be bounded. Revisit
Rust only if profiling shows the Go fallback pipeline is the bottleneck. Even then, a Rust
*sidecar for specific stages* beats a rewrite.

**License:** `pkg/logql/**` and `pkg/distributor` are AGPL-3.0-only, so lokihouse is
**AGPL-3.0-only**. The project owner accepted this on 2026-10-10. It also lets us copy
unexported Loki code verbatim (distributor validation and field detection) with attribution.

Verified on 2026-10-10: a probe module importing `logql`, `syntax`, `loghttp/push` and
`util/marshal` at the pinned commit, plus `clickhouse-go/v2 v2.49.0`, builds with Go 1.27.1.
That needs Loki's `replace` directives and takes ~100 CPU-s cold.

Importing Loki costs a heavy dependency tree, which is acceptable. Loki's `go.mod` `replace`
directives must be copied into ours. Loki internals churn, so the commit is pinned and bumps are
deliberate, each gated by the differential suite.

## 4. Architecture

```
            ┌──────────────────────── lokihouse (one Go binary) ────────────────────────┐
 push ────► │ ingest: Loki push.ParseRequest (proto/JSON/OTLP) → validate → canonical    │
 (HTTP)     │ labels → stream_id = labels.StableHash → async_insert into ClickHouse       │
            │                                                                             │
 query ───► │ api: loghttp.Parse* → planner ─┬─ Tier A: full SQL push-down (metric/log)  │
 (HTTP/WS)  │                                └─ Tier B: Loki engine + chstore.Querier    │
            │   (SQL pre-filter: selector, time, line filters → rows → Loki pipeline)     │
            │ marshal: Loki util/marshal (byte-identical JSON)                            │
            │ ruler: Loki ruler/Prometheus rules manager with the same engine            │
            └─────────────────────────────────────────────────────────────────────────────┘
                                         │ native protocol (clickhouse-go v2)
            ┌──────────────────────────── ClickHouse ────────────────────────────────────┐
            │ logs (ReplacingMergeTree, text index, TTL)  streams (ReplacingMergeTree)    │
            │ MV → stream_stats_hourly (AggregatingMergeTree: volume/index stats/labels)  │
            └─────────────────────────────────────────────────────────────────────────────┘
```

**Tier B** (Phase 1) gives full feature equality immediately. ClickHouse resolves the stream
selector, time range, tenant and leading line filters. That uses the primary key, the
partitions and the text index. Loki's own pipeline and evaluator then run over the remaining
rows. SQL filters only need to be *necessary* conditions, because Loki re-applies the exact
filters, so a bug here can only cost speed, never correctness. The one exception is filters that
make the SQL result a strict subset (see 7.1).

**Tier A** (Phase 2) moves whole queries into ClickHouse when every AST node in them belongs to
a whitelist with proven-equal SQL. Each whitelisted node has differential property tests
against Tier B. Anything not on the whitelist silently falls back to Tier B.

## 5. ClickHouse schema (canonical)

```sql
CREATE TABLE logs (
  tenant              LowCardinality(String),
  stream_id           UInt64,                       -- labels.StableHash(stream labels) == Loki stream hash
  ts                  DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  line                String CODEC(ZSTD(3)),
  structured_metadata Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  entry_hash          UInt64 MATERIALIZED cityHash64(line, mapSort(structured_metadata)), -- Loki dedup identity
  retention_days      UInt16 DEFAULT 0,             -- 0 = keep forever (Loki default)
  INDEX line_text line TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = ReplacingMergeTree
PARTITION BY (toDate(ts), retention_days)
ORDER BY (tenant, stream_id, ts, entry_hash)
TTL toDateTime(least(toDate32(ts) + toIntervalDay(if(retention_days = 0, 65535, retention_days)), toDate32('2106-01-01')), 'UTC')
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE streams (
  tenant         LowCardinality(String),
  day            Date,
  stream_id      UInt64,
  labels         Map(LowCardinality(String), String),
  labels_str     String,                            -- canonical {a="b", c="d"} as Loki prints it
  retention_days UInt16 DEFAULT 0
) ENGINE = ReplacingMergeTree
PARTITION BY (day, retention_days)
ORDER BY (tenant, day, stream_id)
TTL <same expression over toDate32(day)>
SETTINGS ttl_only_drop_parts = 1;
```

Retention is whole UTC days, and `retention_days` is part of the partition key. Every row of a
partition therefore expires at the same UTC midnight, and ClickHouse drops whole partitions
(`ttl_only_drop_parts`) instead of rewriting parts. A `TTL … WHERE` clause would defeat this
because it is a rows-where TTL. The expiry is computed in `Date32` and capped below the 32-bit
`DateTime` limit; without the cap, long retentions wrap around and expire immediately.
`entry_hash` covers line and sorted metadata, because Loki dedups on (ts, line, metadata):
background merges must not collapse entries that differ only in metadata.

### 5.1 Single server or ClickHouse cluster

The topology is set in configuration (`clickhouse.cluster`, `clickhouse.replicated`). The query and insert code is the same for every topology.

| Topology | Tables | Notes |
|---|---|---|
| Single server (default) | `logs`, `streams` as above | — |
| Replicated and/or sharded cluster | `logs_local`, `streams_local` (`ReplicatedReplacingMergeTree`, created `ON CLUSTER`), plus `Distributed` tables `logs` and `streams` over them, sharded by `cityHash64(tenant, stream_id)` | Needs `remote_servers` and Keeper. `replicated: false` gives sharding without replication. |
| ClickHouse Cloud | Single-server DDL (Cloud maps it to SharedMergeTree) | Deferred, see `docs/roadmap.md` |

Rules that make the cluster mode correct:

- **One sharding key for both tables.** Every row of a (tenant, stream), in `logs` and in `streams`, sits on one shard. That keeps `ReplacingMergeTree` dedup and `FINAL` correct per shard, and lets a stream sub-select use `streams_local` without a cross-shard `GLOBAL IN`. A plain `IN (SELECT … FROM <Distributed>)` is rejected by ClickHouse's default `distributed_product_mode = 'deny'`.
- **Read-your-writes.** Connections carry `distributed_foreground_insert = 1` and, when replicated, `insert_quorum = 'auto'` with `insert_quorum_parallel = 1`. A push is acked only when all shards and a majority of replicas hold the rows. This matches Loki, which serves data from ingesters right after the ack. In the prototype, a replica read straight after a non-quorum insert was missing rows. With more than 2 replicas per shard, operators either set `insert_quorum` to the replica count or accept majority-lag on reads; both are possible via `clickhouse.settings`.
- **No silent partial results.** `skip_unavailable_shards` is never set, so a missing shard is an error (Loki 5xx).
- **DDL and mutations** use `ON CLUSTER` and the `_local` tables. That covers migrations, `DELETE FROM logs_local ON CLUSTER …` for the delete API, and the Phase 3 MV, which is created on `logs_local` and targets a `_local` `ReplicatedAggregatingMergeTree` with its own `Distributed` table. TTL, the text index and async inserts work per replica unchanged.
- **Replica paths.** By default the server's `default_replica_path` (`/clickhouse/tables/{uuid}/{shard}` on 26.9) is used. `ON CLUSTER` propagates the table UUID, so no extra Keeper configuration is needed; `replica_path`/`replica_name` can override it.

All of this was prototyped on ClickHouse 26.9 with 2 shards × 2 replicas plus Keeper in podman, and checked to work:

- replication, and co-location by (tenant, stream)
- `FINAL` and materialized columns through `Distributed`
- the text index
- async inserts with quorum through `Distributed`
- `DELETE … ON CLUSTER`
- an MV on `logs_local` read through `Distributed`

The project's test suites run on that same cluster (`make test-cluster`, `make oracle-cluster`).

Phase 3 adds `stream_stats_hourly`, an AggregatingMergeTree fed by a materialized view from
`logs`. It holds `count`, `sum(length(line))`, and `uniqState` of structured-metadata keys per
(tenant, stream, hour), and serves `index/stats`, `index/volume`, `volume_range` and
`detected_labels`.

## 6. Feature → ClickHouse mapping

| Loki feature | ClickHouse mechanism | Phase |
|---|---|---|
| Stream selector `{a="b", c=~"x.*"}` | `stream_id IN (SELECT stream_id FROM streams WHERE tenant=? AND day BETWEEN ? AND ? AND labels['a']='b' AND match(labels['c'],'^(?:x.*)$'))`, primary-key pruning. Regex matchers are evaluated in Go when CH regex parity is unproven (§7.2). | 1 |
| Time range | `ts >= ? AND ts < ?` on partition + ORDER BY key | 1 |
| Horizontal scale and HA (Loki's ring, replication factor, ingesters) | `Distributed` + `Replicated*MergeTree` over a ClickHouse cluster, quorum inserts (§5.1) | 1 |
| Multi-tenancy (`X-Scope-OrgID`, `a\|b` multi-tenant queries) | `tenant` first in ORDER BY; `tenant IN (...)` + `__tenant_id__` label injection | 1 / 5 |
| Line filters `\|=` `!=` (incl. `or` chains) | `position(line, ?) > 0` / `= 0`; the text index accelerates via dictionary scan | 1 |
| Regex line filters `\|~` `!~` | `match(line, ?)` (RE2), push-down gated by a regex parity check | 2 |
| Pattern filters `\|>` `!>` | `LIKE` with `<_>` → `%` as a necessary pre-filter | 2 |
| Structured metadata filters before parsers | `structured_metadata['k'] = ?` | 2 |
| `json` / `logfmt` / `regexp` / `pattern` / `unpack` parsers, `line_format`, `label_format`, `decolorize`, `ip()` | Loki pipeline in Go (Tier B). Push-down only for proven-equal subsets (e.g. `\| json k="a.b"` → `JSONExtractRaw`) | 1 (B) / 2+ |
| `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time` | Step buckets `ceil((ts-start)/g)`; `-State` aggregates; window `-Merge` over `RANGE BETWEEN k PRECEDING AND CURRENT ROW`; `WITH FILL` for empty buckets (prototype verified on CH 26.9) | 2 |
| `sum/avg/min/max/stddev/stdvar/quantile/first/last_over_time` with `unwrap` | Same windowing with `sumState`, `quantileExactState`, `argMaxState`… Unwrap conversions `duration()`, `bytes()` stay in Go unless parity-proven | 2 |
| `sum/avg/min/max/count/topk/bottomk/sort … by/without` | `GROUP BY` / window `row_number()` | 2 |
| Binary ops, `on/ignoring/group_left/right`, `label_replace`, `vector()` | Loki evaluator in Go over Tier-A sub-results | 2 |
| Log query `limit` + `direction` | `ORDER BY ts DESC, stream_id LIMIT n` when the pipeline is filter-only; else streamed cursor | 1 |
| Ingest deduplication | ReplacingMergeTree on `(tenant, stream_id, ts, entry_hash)` + Loki's merge iterator dedup at read time | 1 |
| Retention (global / per tenant / per stream selector) | Partition-aligned day TTL on `retention_days` computed at ingest (tenant `retention_period` since Phase 1; `retention_stream` in Phase 3) | 1 / 3 |
| Delete API (`/loki/api/v1/delete`) | Lightweight `DELETE FROM logs WHERE …` + `delete_requests` table + Loki delete-filter on read until the mutation finishes | 3 |
| Labels / label values / series | `streams` table (day-granular, like Loki's TSDB) | 1 |
| `index/stats`, `index/volume`, `volume_range` | MV → AggregatingMergeTree `stream_stats_hourly` | 3 |
| `detected_labels` / `detected_fields` | `uniq` over `streams` / sampled `logs` + Loki's field detection code | 3 |
| `patterns` | Loki's Drain over a `SAMPLE`d row set at query time | 3 |
| `tail` (websocket) | Polling cursor on `ts > last_seen` (`ORDER BY ts`) at 1s + Loki tail response marshal | 3 |
| Ruler (alerts + recording rules, rules API) | Loki ruler / Prometheus rule manager, query func = our engine; rule storage local dir | 4 |
| Ingest rate/size limits, validation | Loki `validation` limits + our own token bucket | 1 (validation) / 5 (rate) |
| OTLP ingest | Loki `loghttp/push` OTLP parser, unchanged | 1 |
| Query stats in responses | Map CH `query_log`/progress (rows, bytes read) onto Loki `stats.Result` fields | 5 |

## 7. Correctness strategy ("tested feature equality")

1. **Inherited unit tests**: the syntax/pipeline/evaluator packages are imported unchanged, so
   Loki's own tests already cover them.
2. **Differential suite against real Loki** (`test/oracle`): build `cmd/loki` from the pinned
   commit (no container needed). Run it single-binary with filesystem storage next to
   lokihouse. Push the same deterministic fixture to both, run a corpus of LogQL queries and
   API calls against both, and compare normalised JSON. The corpus is seeded from Loki's docs
   and `parser_test.go`/`engine_test.go` query strings, and every bug found becomes a corpus
   entry.
3. **Push-down property tests** (Phase 2): for each whitelisted AST shape, generate random
   queries and random data and assert Tier A == Tier B (in-process, fast).
4. **Known divergence register** (`docs/divergences.md`): any accepted difference is written
   down with its reason. Target: empty, apart from the tie-order note below.

7.1 *Tie order*: Loki orders entries with equal timestamps by stream hash. We use the same hash
(`labels.StableHash`), so ordering matches. Within one stream and one timestamp Loki keeps
insertion order and we cannot. The suite compares such groups as multisets.

7.2 *Regex*: Loki uses Go regexp semantics, ClickHouse uses RE2. Phase 1 never pushes regex
into SQL. Label regex matchers are resolved in Go over the candidate `streams` rows, which are
few. Phase 2 pushes a regex down only if it round-trips through a parity check (§ plan Phase 2).

## 8. Roadmap & estimates (Sonnet 5.5 implementing, subagent-driven)

Cost model. Sonnet 5.5 costs $2/MTok input, $10/MTok output and $0.20/MTok cache read. A typical
implement-plus-review task cycle runs at about 100k-token context over 50–80 turns. That is
roughly 5–8M cache-read tokens, 0.3–0.6M fresh or cache-write input and 40–80k output, which
comes to **$2.5–5 per normal task** and **$8–20 per hard task** (fix loops against the oracle).
Compiling against Loki's dependency tree is slow (first build several minutes), which costs
wall-clock time rather than tokens.

| Phase | Content | Tasks | New LoC (impl+tests) | Complexity | Est. cost | Agent wall-clock |
|---|---|---:|---:|---|---:|---:|
| 1 Foundation | Module + Loki import, schema/migrations incl. cluster topology, ingest (Loki/JSON/OTLP), CH querier (Tier B), query/query_range/labels/series API, differential harness + corpus, cluster test env + cluster runs | 12 | ~5.5k | Medium | $50–90 | 9–13 h |
| 2 Push-down | Tier-A planner, matcher/regex parity, range-agg SQL windowing, vector aggs, unwrap aggs, log-query limit push-down, property tests | 10 | ~6k | **High** | $90–160 | 12–20 h |
| 3 Remaining APIs | tail (WS), index stats/volume/volume_range (MV), detected_labels/fields, patterns, format_query, delete API, retention TTL | 9 | ~4k | Medium | $40–80 | 8–12 h |
| 4 Ruler | Rules API + evaluation + recording-rule remote write, rule storage | 4 | ~1.5k | Medium | $20–40 | 4–6 h |
| 5 Hardening | limits/rate limiting, multi-tenant queries, query stats, timeouts, perf bench vs Loki, packaging (podman compose, single static binary) | 5 | ~2k | Medium | $25–50 | 5–8 h |
| — | Equality bug-tail budget (oracle failures across phases) | — | — | — | $30–80 | — |
| **Total** | | **40** | **~18.5k** | | **$255–500** | **~41–61 h** |

For comparison, the Rust route comes to ~100+ tasks, ~50k LoC and $1.5–3k, plus a divergence
tail with no upper bound.

Each phase gets its own detailed implementation plan when it starts. Phase 1 is in
`docs/superpowers/plans/2026-10-10-lokihouse-phase1-foundation.md`.

## 9. Risks

| Risk | Mitigation |
|---|---|
| Loki internal API churn on bumps | Pin the commit and keep adapter surface small (`logql.Querier`, `loghttp`, `marshal`, `push`) |
| Huge transitive deps / build time | Accept, and cache `GOMODCACHE` in CI |
| Tier B pulls too many rows for parser-heavy metric queries | Phase 2 push-down; conservative necessary-condition pre-filters derived from label filters (e.g. `\| json \| status="500"` ⇒ `position(line,'500')>0`) |
| ClickHouse text-index syntax still evolving | Index is optional in migrations (skip on older servers); correctness does not depend on it |
| Replica lag after the push ack (reads on a replica outside the quorum) | `insert_quorum='auto'` by default; `TestCluster_ReadAfterPush`; operators can raise the quorum via `clickhouse.settings` |
| `ON CLUSTER` DDL hangs or fails when a node is down at startup | Migrations are idempotent `IF NOT EXISTS`, and `distributed_ddl_output_mode='throw'` turns the hang into a clear error; retry at the next start |
| Cluster-only bugs (SQL naming `_local` tables, distributed sub-selects) | Whole unit and oracle suites rerun on a 2×2 cluster (`make test-cluster`, `make oracle-cluster`) |
| Day-granular label/series results differ at range edges from Loki's TSDB | Same day granularity as Loki's index; oracle queries cover partial-day ranges |
