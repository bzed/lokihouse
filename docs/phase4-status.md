# Phase 4 status

Phase 4 runs Loki's ruler inside lokihouse: rule groups in local filesystem
storage, Loki's rules API, alerting and recording rules evaluated with
lokihouse's engine (Tier A push-down, Tier B fallback, delete filtering),
Alertmanager notification and recording-rule remote write. Everything is
compared against real Loki (commit `fbc3f077`) in the oracle.

## Exit criteria

Run on 2026-10-11, on `phase4-ruler` after task 4.

| Gate | Command | Result |
| --- | --- | --- |
| Unit tests | `make test` | ok (all packages) |
| Cluster unit tests | `make test-cluster` | ok (4-node ClickHouse cluster) |
| Oracle (all push-down modes) | `make oracle-all` | ok (off / downstream / on) |
| Cluster oracle | `make oracle-cluster` | ok (Loki vs 4-node lokihouse) |

## Endpoints

All served by Loki's handlers (`pkg/ruler/base` `API`), registered as Loki's
`initRuler` does, when `ruler_storage` is configured and `ruler.enable_api`
is true (the default).

| Endpoint | Oracle coverage |
| --- | --- |
| `GET /prometheus/api/v1/rules` | `ruler/prometheus/api/v1/rules` (evaluation timestamps dropped) |
| `GET /prometheus/api/v1/alerts` | `ruler/prometheus/api/v1/alerts` (`activeAt` dropped) |
| `GET /loki/api/v1/rules`, `GET /api/prom/rules` | `rules_list`, `rules_legacy_list`, `rules_other_tenant` |
| `GET /loki/api/v1/rules/{namespace}` | `rules_list_ns`, `rules_tmp_list_deleted` |
| `POST /loki/api/v1/rules/{namespace}` | `rules_create*`, `rules_bad_*`, `rules_tmp_*create` |
| `DELETE /loki/api/v1/rules/{namespace}` | `rules_tmp_delete_ns` |
| `GET /loki/api/v1/rules/{namespace}/{groupName}` | `rules_get_group`, `rules_get_missing`, `rules_tmp_get_deleted` |
| `DELETE /loki/api/v1/rules/{namespace}/{groupName}` | `rules_tmp_delete_group` |
| `/api/prom/rules/…` (legacy) | same handlers; `rules_legacy_list` |

Outputs: recording-rule samples by Prometheus remote write
(`remote_write_samples`, `remote_write_tenants`) and alerts to Alertmanager
(`alertmanager_alerts`).

## Operations

- **Enable** by setting `ruler_storage` (backend `filesystem` with a writable
  `filesystem.dir`, or `local` for a read-only directory of rule files). The
  `ruler:` block takes Loki's keys; see `lokihouse.example.yaml`.
- **One ruler instance.** There is no ruler ring: configure `ruler_storage` on
  exactly one lokihouse instance, otherwise every instance evaluates every rule
  (duplicate alerts and samples). `ruler.enable_sharding` and
  `ruler.evaluation.mode: remote` are rejected at startup.
- **WAL.** With remote write enabled, Loki's ruler wipes `ruler.wal.dir` at
  every start; lokihouse refuses a WAL directory that contains the working
  directory, the rule storage or `rule_path`.
- **Paths.** `ruler.rule_path` (temporary rule files, default `rules-temp`) and
  `ruler.wal.dir` (default `ruler-wal`) are relative to the working directory
  unless set absolute.

## Known issues

See `docs/divergences.md`, section "Ruler (Phase 4)".
