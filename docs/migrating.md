# Migrating from Loki

lokihouse is a **drop-in at the HTTP layer**. The push endpoints, the LogQL
query endpoints, the label/series metadata endpoints, the tail endpoint, the
delete API and the ruler API all use the same paths, the same JSON shapes and
the same headers as stock Loki. Point a Grafana datasource (or any other
client) at lokihouse's base URL instead of Loki's and it works.

This page is a full cutover runbook. It covers:

1. What you keep and what you stop running.
2. The behavioural differences from Loki, in plain language (the same
   differences the project tracks in `docs/divergences.md`, written for a
   migrator rather than a team member).
3. The cutover steps.
4. A verification checklist to tick off before you call the migration done.

## TL;DR

- Build and install lokihouse **next to** Loki (Loki stays up).
- **Shadow push**: send a copy of your push traffic to lokihouse while Loki
  keeps its own.
- **Verify** with the LogQL queries you already use.
- **Flip the Grafana datasource** from Loki's base URL to lokihouse's.
- **Stop pushing to Loki** (or keep it as a read-only rollback path).
- **Decommission Loki** after a soak period.

## What you keep, what you stop running

**Keep:**

- Your Grafana (datasource URL changes, nothing else).
- Your Alertmanager (the ruler talks to the same URL).
- Your push clients (same push JSON, same headers, same `X-Scope-OrgID` if
  you use multi-tenancy).
- Your LogQL queries (same syntax, same engine semantics — lokihouse runs
  Loki's own LogQL engine).
- Your rule files (the ruler reads the same file format; the storage dir is
  configurable).

**Stop running (Loki components lokihouse replaces):**

- Ingester, distributor, compactor, query-frontend, ruler (if you move the
  ruler too), index-gateway / TSDB index.
- Object-store access for chunks (lokihouse stores in ClickHouse, not in
  S3/GCS/Azure).
- The ring / memberlist (lokihouse has no ring; see
  [Ruler sharding](#ruler-sharding-no-ring) below).

## What is different (plain-language summary)

These are the same differences the project records in
[`docs/divergences.md`](https://github.com/bzed/lokihouse/blob/main/docs/divergences.md),
restated for a migrator. For each: what you will notice, why, and what to do
about it.

### Tie order inside one stream and timestamp

**What you'll notice:** entries that share a timestamp *within the same
stream* may come back in a different order than Loki.

**Why:** Loki preserves insertion order for equal timestamps inside one
stream; lokihouse cannot (ClickHouse does not keep insertion order for
equal-timestamp rows).

**What to do:** if a UI or downstream process relies on "Loki's insertion
order" for same-timestamp entries in one stream, you will see a difference.
Most workloads do not rely on this; if you do, bucket your processing by
timestamp rather than by arrival order.

### `index/stats` and `index/volume` granularity

**What you'll notice:** volume and index-statistics numbers for time ranges
that cut through an hour can differ from Loki's by roughly 15–30%.

**Why:** Loki pro-rates per *chunk* (10-minute blocks); lokihouse pro-rates
per *hourly* bucket. Ranges that cover whole hours agree exactly; ranges that
cut through an hour can differ.

**What to do:** if you build dashboards off `index/volume` or `index/stats`,
expect a modest shift for sub-hour ranges. The project's oracle verifies the
response *shape* exactly and the per-metric total within 30%, which is the
standard this page uses for "they agree".

**Bytes in `index/stats`:** Loki adds each chunk's structured-metadata symbol
table to the byte count; lokihouse counts `len(line) + 8 × metadata pairs`
only. For typical JSON metadata this is ≈1.5% — usually below the noise of
the chart.

**Chunks:** the "chunks" count in `index/stats` is the number of overlapping
hourly buckets in lokihouse, not Loki chunks. If you have a dashboard on
"number of chunks", expect a different number.

### `index/volume` matches Loki's *compactor* path, not its *querier* path

**What you'll notice:** if your volume dashboards were built against Loki's
live (querier) volume path, the numbers will shift when you move to
lokihouse.

**Why:** Loki has two code paths that both answer "how much volume is this":
the querier path (used by a live `index/volume` query) sums the on-disk size
of each flushed chunk, rounded to the nearest KiB; the ingester/compactor
path counts raw head-block bytes. lokihouse implements the ingester/compactor
accounting. The two Loki paths disagree with each other by ~15–30%;
lokihouse matches the compactor side.

**What to do:** treat any existing "Loki volume" dashboard numbers as
approximate; re-baseline them after the cutover.

### Delete requests

**Request IDs:** lokihouse generates 8 random hex characters; Loki's format
differs. If you have tooling that parses Loki's delete-request IDs, it will
need updating.

**Stats and volume after a delete:** `index/stats`, `index/volume`,
`patterns` and `detected_labels` ignore deletes in both systems. (Loki's own
index stats also count unprocessed deletes; after processing, Loki's
rewritten chunks shrink while lokihouse's materialized view does not.)

**Query-time delete visibility (subtle timing difference):** both systems
hide matching rows via a query-time filter. Loki caches a tenant's delete
list for 5 minutes from its first lookup, so a delete created *after* a query
on that tenant is not visible for up to 5 minutes. lokihouse is authoritative
per query (no cache), so a delete is visible immediately. If you have
monitoring that assumes Loki's 5-minute lag, it will fire differently.

**Rows written between delete submission and processing:** the processor
re-resolves the request's selector at processing time, so rows that land
inside the request's `[start, end]` window *after* the request was created
are also deleted. This matches Loki's compactor (which also re-reads the
index when processing) and is a narrow, intentional difference from "exactly
the rows the query-time filter hid".

### Tail delivery

**What you'll notice:** a Grafana Live view (or any `/tail` client) may miss
entries that Loki would have delivered live.

**Why:** Loki streams tail entries to the WebSocket client as its ingesters
receive them (push). lokihouse has no ingester to push from, so its tail
client *polls* ClickHouse every second for rows with `ts` newer than
"now minus a 10-second lookback" for late arrivals, deduplicated by a
seen-set. Consequently:

- An entry pushed with a timestamp **older than the current poll window** is
  not sent live (it is reachable by a normal range query).
- An entry with a timestamp **before the tail started** is not sent live
  (same: reachable by a range query).
- History (rows before the tail start) is served by Loki's `Tailer` history
  path in both systems, so a fresh tail client still gets the "past N
  minutes" context correctly.

**What to do:** if you rely on tail for real-time alerting or a live feed,
test with your actual push patterns. For most dashboards the 1-second poll
plus 10-second lookback is indistinguishable from push.

### Ruler sharding — no ring

**What you'll notice:** if you run more than one lokihouse instance, alerts
and recording-rule samples are **duplicated**.

**Why:** Loki distributes rule groups over a ring of rulers; lokihouse has no
ring. `ruler.enable_sharding: true` and `ruler.evaluation.mode: remote` are
rejected at startup, and `/ruler/ring` is not served. Every lokihouse
instance with `ruler_storage` configured evaluates *all* rule groups.

**What to do:** configure the ruler on **exactly one** lokihouse instance.
The other instance(s) should not set `ruler_storage`. If you need multiple
rulers for load, that is not yet supported.

### Ruler `rule_path` default

**What you'll notice:** if you were relying on Loki's default `rule_path`,
your rule files will not be where lokihouse looks.

**Why:** Loki's single-binary default is `/rules` (more precisely
`<common.path_prefix>/rules-temp`); lokihouse has no `path_prefix`, so its
default is `rules-temp` **relative to the working directory**.

**What to do:** set `ruler.rule_path` explicitly in your config, pointing at
the directory that holds your rule files (or the one you will mount / copy
them to).

### Ruler WAL directory guard

**What you'll notice:** lokihouse may **refuse to start** where Loki would
have started (and deleted your rules).

**Why:** with remote write enabled, Loki wipes `ruler.wal.dir`
(`os.RemoveAll`) at every start. lokihouse refuses to start when that
directory is empty or is the working directory, the rule storage directory,
or `rule_path` — because deleting those would be catastrophic.

**What to do:** set `ruler.wal.dir` to a dedicated, empty-on-startup directory
(e.g. `/var/lib/lokihouse/ruler-wal`) that is distinct from `rule_path` and
the rule storage dir.

### No `/metrics` endpoint (yet)

**What you'll notice:** any dashboards or alerts built on Loki's Prometheus
`/metrics` endpoint will not work as-is.

**Why:** lokihouse's ruler metrics are collected in a private registry and
not exposed (the `/metrics` endpoint is Phase 5 work).

**What to do:** if you have Prometheus scraping Loki's `/metrics`, plan to
re-point it once Phase 5 lands, or build interim dashboards off
ClickHouse's own `system.query_log` / `system.metrics` for the storage side.

## Cutover runbook

The steps below assume Loki and lokihouse are running side by side, with the
same push clients able to reach both. Adjust for your topology (load
balancer, push gateway, etc.).

### 1. Build and install lokihouse next to Loki

Follow the [Installation](install/index.md) page. Loki stays up and serving
traffic. lokihouse starts empty (its ClickHouse database is created on
first start).

### 2. Shadow push

Point a **copy** of your push traffic at lokihouse's push endpoint while
Loki keeps its own. The push JSON, headers and `X-Scope-OrgID` (if you use
multi-tenancy) are identical, so a load balancer or push gateway that
fan-out to both upstreams is the simplest option.

- If you have a push gateway in front of Loki, add lokihouse as a second
  upstream with the same weight.
- If you push directly from applications, run a short dual-write from the
  push client (or a small proxy) for the verification window.

The goal of this step is to get a representative sample of your real data
into lokihouse so the verification in step 3 is meaningful.

### 3. Verify with the same queries

Pick the LogQL queries you actually use in Grafana. Run each against **both**
Loki and lokihouse (two Grafana datasources, or two `curl` calls) and compare
results. The project's own differential oracle (`make oracle`) runs the same
corpus against both and compares byte-for-byte with float tolerance (1e-9
relative) — that is the bar "they agree" means here.

At minimum, verify:

- A few instant queries (`/loki/api/v1/query`).
- A few range queries (`/loki/api/v1/query_range`) with the same
  `start`/`end`/`step`.
- `labels` and `label/{name}/values` return the same sets.
- A `series` query returns the same series.
- If you use structured metadata: a query with a `| json` or `| logfmt`
  parser returns the same extracted fields.

Note the expected differences from the [plain-language summary above](#what-is-different-plain-language-summary)
— volume numbers within 30%, tail delivery timing, delete-visibility timing.
Anything outside those is a bug and should be reported.

### 4. Flip the Grafana datasource

In Grafana, edit the Loki datasource and change the **URL** from
`http://loki:3100` (or whatever) to `http://lokihouse:3100`. Everything else
— the path prefix, the auth headers, the `X-Scope-OrgID` mapping — stays the
same. Save and reload a dashboard.

### 5. Stop pushing to Loki

Remove Loki from your push load balancer / push gateway, or stop the
dual-write from step 2. Loki can stay up **read-only** for a transition
period if you want a rollback path. New data goes only to lokihouse from
this point.

### 6. Rollback (if needed)

Point the Grafana datasource back at Loki's URL. Loki's data is untouched by
the cutover (you were pushing to both in step 2), so any query that worked
against Loki before the cutover still works. If you have already deleted data
from Loki, that is not reversible — test on a copy of your data before you
delete.

### 7. Decommission Loki (optional, after a soak period)

After a soak period (a week to a month, depending on how confident you are),
remove the Loki deployment: stop the systemd units / pods, cancel the
object-store billing, and archive the rule files if you have not already
copied them to lokihouse's ruler storage.

## Verification checklist

Tick these off before you call the migration done:

- [ ] `GET /ready` on lokihouse returns `200` with body `ready\n`.
- [ ] `GET /loki/api/v1/status/buildinfo` returns the expected version
      (matches the binary you installed).
- [ ] A push to `/loki/api/v1/push` succeeds (`204` or `200`) and the entry
      is queryable via `/loki/api/v1/query`.
- [ ] Your N favourite LogQL queries return the same result on Loki and on
      lokihouse (within float tolerance for metrics; identical for log
      streams).
- [ ] `GET /loki/api/v1/labels` and
      `GET /loki/api/v1/label/{name}/values` return the same label/value
      sets on both systems.
- [ ] If you use the ruler: rule groups are visible via
      `GET /loki/api/v1/rules`, and a test alert rule fires to Alertmanager.
- [ ] If you use tail: a Grafana Live view works (allowing for the
      polling-vs-push delivery difference described above).
- [ ] If you use deletes: a delete request is accepted, the query-time
      filter hides the matching rows, and the request eventually completes
      (`GET /loki/api/v1/delete` shows it as processed).
- [ ] If you use multi-tenancy (`X-Scope-OrgID`): the same tenant isolation
      holds — a query for tenant A does not return tenant B's data.
- [ ] If you set `retention_period` per tenant: old data expires on the
      expected UTC-day boundary (lokihouse retention is whole UTC days).
- [ ] Grafana dashboards render correctly against the lokihouse datasource
      (no missing panels, no "no data" where there should be data).
- [ ] Alerting: any Loki alert rules you moved to lokihouse's ruler fire
      and resolve as expected.
