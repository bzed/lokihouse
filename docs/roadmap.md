---
description: What is deferred or still open in lokihouse, the Grafana Loki-compatible log server backed by ClickHouse.
---

# Roadmap

## Open items

- **ClickHouse Cloud verification:** run the unit suite and the oracle against
  a ClickHouse Cloud service (single-server DDL, which Cloud maps to
  SharedMergeTree) and record the result here. Needs a Cloud account;
  `LOKIHOUSE_TEST_CH_ADDR`, `LOKIHOUSE_TEST_CH_USER`, `LOKIHOUSE_TEST_CH_PASSWORD`
  already select the server, but the native protocol over TLS (port 9440) must
  be wired into `chtest.Options` and `clickhouse.*` config (a `tls` key).
- **podman compose example:** a ready-to-run lokihouse plus ClickHouse setup.
- **Per-policy ingestion limits** (`policy_stream_mapping`): see the
  [divergences](https://github.com/bzed/lokihouse/blob/main/docs/divergences.md#ingestion).
- **SQL push-down for multi-tenant queries:** with
  `query.querier.multi_tenant_queries_enabled`, multi-tenant queries run on the
  plain engine, because push-down is single-tenant by construction.
- **`detected_metric_names`:** not served by lokihouse.
