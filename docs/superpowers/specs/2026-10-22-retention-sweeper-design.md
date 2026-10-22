# Retention that follows the configuration (design)

## Problem

lokihouse stores each row's retention (`retention_days`) at push time and lets a
table `TTL` on `retention_days` expire it. `retention_days` is part of the
partition key, so ClickHouse cannot change it with `ALTER … UPDATE` (key columns
cannot be mutated). Changing `limits_config.retention_period` or
`retention_stream` therefore only affects rows pushed afterwards. Operators expect
Loki's behaviour: the compactor applies the *current* retention to all data.

## Decision

Retention is applied by a sweeper at run time, not stored with the data.

1. **Schema version 2.** `logs`, `streams` and `stream_stats_hourly` lose the
   `retention_days` column, their `TTL` and `ttl_only_drop_parts`. They are
   partitioned by UTC day only (`toDate(ts)`, `day`, `toDate(hour)`). The
   materialized view no longer carries `retention_days`. Nothing else in the
   schema changes. The query side never read `retention_days`.
2. **Sweeper.** When `compactor.retention_enabled` is true (Loki's key;
   lokihouse's default is `true`, Loki's is `false`), a loop runs every `compactor.compaction_interval` and
   applies the current `retention_period` and `retention_stream`, using Loki's
   rule (`retention.TenantRetentionSnapshot.RetentionPeriodFor`: highest
   priority wins, the shorter period on a tie, else `retention_period`). Periods
   are rounded up to whole days. A row stamped on UTC day D with an effective
   retention of N days is removed once `D + N <= today` (UTC), the same
   semantics the TTL had.
   - `maxDays` is `0` (keep forever) when `retention_period` is `0`, otherwise
     the largest of `retention_period` and every `retention_stream` period.
   - **Whole days:** if `maxDays > 0`, every day partition with
     `D + maxDays <= today` is dropped (`DROP PARTITION ID`) from the three
     tables. This is the only step when there are no `retention_stream` rules.
   - **Per stream:** for every stream whose effective retention `d` is
     `0 < d < maxDays` (or `d > 0` when `maxDays` is `0`), the days
     `D + d <= today` that still hold rows of the stream are deleted with
     lightweight `DELETE`s from the three tables. A read-only existence query
     comes first, so a sweep that has nothing to do issues no mutation.
   - After deletes, `APPLY DELETED MASK` runs on each table that had a delete.
   - Every step is idempotent. Several instances may sweep concurrently.
3. **Migration.** A database with schema version 1 (a `retention_days` column
   on the storage `logs` table) is migrated by `lokihouse -schema.migrate`,
   run once by the operator with every other lokihouse instance stopped. A
   normal start against a version 1 database exits 1 with an error naming
   the command. Steps, per table, resumable after a crash:
   drop the stats materialized view; create `<table>_migrate` (v2 DDL, plus a
   Distributed table on a cluster); `INSERT … SELECT` the rows without
   `retention_days` (`parallel_distributed_insert_select = 2` on a cluster, so each
   shard copies locally); `EXCHANGE TABLES`; drop the old data. The regular
   schema creation then recreates the view. Before copying, the migration
   refuses to start if any disk holding the database has less free space than
   the bytes of the table about to be copied.
4. **Docs.** The configuration reference and operations guide describe the
   sweeper, `compactor.retention_enabled`, why retention is not stored per row,
   and a manual runbook (drop a day partition to free space at once). The
   statements that the schema may change between releases without a migration
   are removed. The upgrade note describes `-schema.migrate`.

## Not in scope

- A ClickHouse table `TTL` as a safety net. It would have to be rewritten with
  `ALTER … MODIFY TTL` on every configuration change, and instances with
  different configs would fight over it. One mechanism, the sweeper, is enough.
  Loki also expires nothing while its compactor is down.
- Per-tenant overrides (still not supported).
- An automatic migration at startup: ClickHouse has no lock that would keep two
  starting instances from migrating at once.

## Compatibility

- `compactor.retention_enabled` defaults to `true`, unlike Loki (`false`), so a
  configured `retention_period` keeps working after the upgrade as it did with the
  TTL. The default is recorded in `docs/divergences.md` and in `CHANGELOG.md`.
  Someone moving a Loki config over with `retention_period` set but
  `retention_enabled` unset gets expiry on lokihouse where Loki kept the data;
  the divergence entry and the migration guide say so.
- The push partition limit (60) now counts UTC days only.
