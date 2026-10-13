# Roadmap

## Deferred from Phase 5

- **ClickHouse Cloud verification** (spec §5.1): run the unit suite and the
  oracle against a ClickHouse Cloud service (single-server DDL, which Cloud maps
  to SharedMergeTree) and record the result here. Needs a Cloud account;
  `LOKIHOUSE_TEST_CH_ADDR`, `LOKIHOUSE_TEST_CH_USER`, `LOKIHOUSE_TEST_CH_PASSWORD`
  already select the server, but the native protocol over TLS (port 9440) must
  be wired into `chtest.Options` and `clickhouse.*` config (a `tls` key).
- **podman compose example** (spec §8 Phase 5 packaging row): not in scope for
  Phase 5.
- **Per-policy ingestion limits** (`policy_stream_mapping`): see
  `docs/divergences.md`.
