# HTTP API

Every endpoint lokihouse serves, enumerated from
[`internal/api/api.go`](https://github.com/bzed/lokihouse/blob/main/internal/api/api.go)
and the rule/route registration in
[`internal/api/rules.go`](https://github.com/bzed/lokihouse/blob/main/internal/api/rules.go)
and
[`internal/api/lokicodec.go`](https://github.com/bzed/lokihouse/blob/main/internal/api/lokicodec.go).

All `/loki/api/v1/...` paths are Loki-identical. The `/api/prom/...` paths
are the legacy Prometheus-compatible paths Loki also serves.

## Auth

When `auth_enabled` is `true` (the default), every endpoint **except**
`/ready` and `/loki/api/v1/format_query` requires the `X-Scope-OrgID`
header. When `auth_enabled` is `false`, no tenancy is enforced.

## Push

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `POST` | `/loki/api/v1/push` | yes | Loki JSON push (proto/JSON/OTLP). |
| `POST` | `/api/prom/push` | yes | Legacy Prometheus-compatible push path. |
| `POST` | `/otlp/v1/logs` | yes | OTLP logs push. |

## Query

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET`, `POST` | `/loki/api/v1/query` | yes | Instant LogQL query. |
| `GET`, `POST` | `/loki/api/v1/query_range` | yes | Range LogQL query. |
| `GET`, `POST` | `/loki/api/v1/series` | yes | Series metadata for a selector + time range. |

## Metadata

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET` | `/loki/api/v1/labels` | yes | All label names. |
| `GET` | `/loki/api/v1/label/{name}/values` | yes | Values for a label name. |
| `GET`, `POST` | `/loki/api/v1/index/stats` | yes | Per-stream index statistics. |
| `GET`, `POST` | `/loki/api/v1/index/volume` | yes | Per-stream volume. |
| `GET`, `POST` | `/loki/api/v1/index/volume_range` | yes | Volume over a time range. |
| `GET`, `POST` | `/loki/api/v1/detected_labels` | yes | Labels detected from line content. |
| `GET`, `POST` | `/loki/api/v1/detected_fields` | yes | Fields detected from line content. |
| `GET`, `POST` | `/loki/api/v1/patterns` | yes | Log patterns (Drain clustering). |
| `GET`, `POST` | `/loki/api/v1/detected_field/{name}/values` | yes | Values for a detected field. |

## Deletes

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `PUT`, `POST` | `/loki/api/v1/delete` | yes | Submit a delete request. |
| `GET` | `/loki/api/v1/delete` | yes | List delete requests. |
| `DELETE` | `/loki/api/v1/delete` | yes | Cancel a delete request. |

## Tail

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET`, `POST` | `/loki/api/v1/tail` | yes | WebSocket tail. |
| `GET`, `POST` | `/api/prom/tail` | yes | Legacy tail path. |

## Ruler

Present only when `ruler_storage` is configured.

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET` | `/loki/api/v1/rules` | yes | List all rule groups. |
| `GET` | `/loki/api/v1/rules/{namespace}` | yes | List rule groups in a namespace. |
| `POST` | `/loki/api/v1/rules/{namespace}` | yes | Create a rule group. |
| `DELETE` | `/loki/api/v1/rules/{namespace}` | yes | Delete a namespace. |
| `GET` | `/loki/api/v1/rules/{namespace}/{groupName}` | yes | Get a rule group. |
| `DELETE` | `/loki/api/v1/rules/{namespace}/{groupName}` | yes | Delete a rule group. |
| `GET` | `/api/prom/rules` | yes | Legacy: list all rule groups. |
| `GET` | `/api/prom/rules/{namespace}` | yes | Legacy: list rule groups in a namespace. |
| `POST` | `/api/prom/rules/{namespace}` | yes | Legacy: create a rule group. |
| `DELETE` | `/api/prom/rules/{namespace}` | yes | Legacy: delete a namespace. |
| `GET` | `/api/prom/rules/{namespace}/{groupName}` | yes | Legacy: get a rule group. |
| `DELETE` | `/api/prom/rules/{namespace}/{groupName}` | yes | Legacy: delete a rule group. |
| `GET` | `/prometheus/api/v1/rules` | yes | Prometheus-compatible rules list. |
| `GET` | `/prometheus/api/v1/alerts` | yes | Prometheus-compatible alerts list. |

## Admin

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET` | `/ready` | **no** | Health check. Returns `ready\n` with HTTP 200. |
| `GET` | `/loki/api/v1/status/buildinfo` | yes | Build information (version, revision, branch, build date, Go version). |
| `GET`, `POST` | `/loki/api/v1/format_query` | **no** | Validate / format a LogQL query. Registered without the tenant wrapper, so no `X-Scope-OrgID` is required. |
| `GET` | `/debug/lokihouse/tiera_leaves` | no | Debug: list Tier A push-down leaves. Development-only; not a stable API. |
