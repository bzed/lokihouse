# Lokihouse Phase 3 (Remaining APIs) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the rest of Loki's read and admin API from ClickHouse: per-stream retention, `index/stats`, `index/volume`, `index/volume_range`, `detected_labels`, `detected_fields`, `detected_field/{name}/values`, `patterns`, the delete API (with query-time filtering and physical deletion), `tail` over WebSocket, and `format_query`. Each endpoint is compared against real Loki in the oracle.

**Architecture:** Wherever Loki's own HTTP layer is exported, lokihouse plugs its handlers in behind it instead of re-implementing it:
- `queryrange.NewSerializeHTTPHandler(handler, queryrange.DefaultCodec)` decodes the request and encodes the response with Loki's codec. Our `api.lokiHandler` implements `queryrangebase.Handler` for the decoded request types. This covers index stats, volume, detected labels, detected fields and patterns.
- Loki's frontend middlewares wrap that handler unchanged: `queryrange.NewVolumeMiddleware()` (step bucketing and the Prometheus-shaped volume response), `queryrange.NewDetectedLabelsCardinalityFilter` and `queryrange.NewDetectedFieldsHandler`.
- Tail reuses Loki's `tail.Querier`, `Tailer` and `TailHandler`. Lokihouse supplies one in-process "ingester" whose `Querier_TailClient` polls ClickHouse.

Statistics come from a new materialized view `stream_stats_hourly`. It stores per (tenant, stream, hour) the entry count, the bytes, and the first and last entry timestamps. Loki's own chunk pro-rating rule is then applied per hourly bucket. Unexported Loki code (the delete request handler, the detected-labels cardinality helpers, pattern pruning, `format_query`) is copied with attribution.

**Tech Stack:** Go 1.27, `github.com/grafana/loki/v3` @ `fbc3f0770b8c` (unchanged; new imports: `pkg/querier/queryrange`, `pkg/querier/tail`, `pkg/querier/deletion`, `pkg/util/deletion`, `pkg/compactor/deletion` (for `deletionproto`, `TimeRange`, `ErrDeleteRequestNotFound`), `pkg/compactor/deletionmode`, `pkg/compactor/retention`, `pkg/pattern`, `pkg/pattern/drain`, `pkg/pattern/iter`, `pkg/querier/pattern`, `pkg/storage/stores/index/seriesvolume`, `pkg/util`), `clickhouse-go/v2` v2.49.0, ClickHouse 26.9, `github.com/gorilla/websocket` (already in Loki's graph), `github.com/gorilla/mux` (for `mux.SetURLVars`, already in Loki's graph).

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md` (§1 goal, §5 schema incl. `stream_stats_hourly`, §5.1 cluster rules for MVs and mutations, §6 feature mapping rows marked Phase 3, §7 correctness strategy incl. the divergence register).

## Global Constraints

All Phase 1 and Phase 2 Global Constraints still apply (`docs/superpowers/plans/2026-10-10-lokihouse-phase1-foundation.md`, `docs/superpowers/plans/2026-10-11-lokihouse-phase2-pushdown.md`). In addition:

- Never bump the Loki commit inside a Phase 3 task. Importing `pkg/querier/queryrange` adds many indirect modules (cloud storage SDKs). Run `go mod tidy` and commit `go.mod`/`go.sum` in the task that first imports it (Task 2). The spec accepts the heavy dependency tree (§3).
- **Never re-implement a Loki wire format.** Requests are decoded and responses encoded by Loki code: `queryrange.DefaultCodec`, `loghttp.Parse*`, `marshal.*`, or Loki handlers copied verbatim. If a response differs from Loki only in encoding, the fix is to route it through Loki's encoder.
- Code copied from Loki keeps its copyright header and gets `// Copied from grafana/loki@fbc3f077 <path>; modifications: <list>` on top (Phase 1 rule).
- Every query bound bound into SQL is Unix nanoseconds through `fromUnixTimestamp64Nano(?, 'UTC')`, never a `time.Time` parameter. clickhouse-go formats `time.Time` at second precision (Phase 2 review finding).
- **Materialized views and mutations on a cluster follow §5.1:**
  - The MV is created `ON CLUSTER` on `logs_local` and writes to `stream_stats_hourly_local` (`Replicated` when replicated). `stream_stats_hourly` is a `Distributed` table over it, sharded by `cityHash64(tenant, stream_id)`.
  - Deletes run as `DELETE FROM <db>.logs_local ON CLUSTER <c> WHERE …` with `lightweight_deletes_sync = 2`. On a single server the table is `<db>.logs` and there is no `ON CLUSTER`.
  - All of this goes through `Topology.Local()` / `Topology.OnCluster()`.
- **Deliberate deviation from spec §5:** `stream_stats_hourly` does not carry the `uniqState` of structured-metadata keys. No Phase 3 endpoint reads it: `detected_fields` comes from Loki's handler over sampled log lines, and `detected_labels` from stream labels. It also adds `min_ts`/`max_ts`, which the spec does not list but Loki's pro-rating needs.
- `stream_stats_hourly` only covers rows inserted after its migration ran. There is no automatic backfill: the project has no production deployments yet. `docs/phase3-status.md` documents a manual backfill statement.
- Statistics follow Loki's ingester rules, with an hourly bucket in place of a chunk: `pkg/ingester/instance.go` `getStats`/`getVolume` at the pinned commit.
  - A bucket counts when `from < max_ts && through > min_ts`.
  - It contributes `uint64(factor × value)`, with `factor = util.GetFactorOfTime(from, through, min_ts, max_ts)` (all in ns).
- Bytes per entry are `length(line) + 8 × (number of structured-metadata pairs)`. That is Loki's head-block accounting (`pkg/chunkenc/unordered.go`), without the per-chunk symbol table.
- Config gains a `compactor:` block with Loki's key names and defaults:
  - `compaction_interval` (10m; the delete-processing loop)
  - `delete_request_cancel_period` (24h)
  - `delete_max_interval` (24h)
- `config.Load` must call `LimitsConfig.Validate()`. Without it, `retention_stream[].Matchers` stay nil and every rule matches every stream.
- Every accepted difference from Loki goes into `docs/divergences.md` (spec §7.4) with its reason, in the task that introduces it.
- Every task's tests must pass on the single node and on the 2×2 podman cluster (`make test`, `make test-cluster`). Tasks 2–9 also need `make oracle` and `make oracle-cluster`.

## Review Focus

1. **A tail client that disconnects** (browser tab closed, network drop): the polling goroutine and its ClickHouse reads must stop. The concurrent-tail count must drop back, or `max_concurrent_tail_requests` locks the tenant out. Owner: Task 8 (`TestTail_CloseStopsPolling`).
2. **Entries that reach ClickHouse after the tail already polled past their timestamp** (normal with batching clients): they must still be delivered exactly once if they are late by less than the lookback. Owner: Task 8 (`TestTail_LateEntryWithinLookback`).
3. **Physical deletion versus the query-time filter:** for a delete with a regex line filter, rows exactly at the end millisecond, and rows 0.5 ms after it, the processor must remove exactly the rows that query-time filtering hid, no more and no fewer. Owner: Task 7 (`TestProcessor_MatchesQueryTimeFilter`).
4. **Stats and volume for ranges that cut an hour bucket, and buckets with a single entry** (`min_ts == max_ts`): Loki's factor rule, no division by zero, no wrap-around of unsigned counts. Owner: Task 2 (`TestStatsFromHours`).
5. **Delete requests across tenants:** tenant B must neither list nor cancel tenant A's request, and A's request must not hide B's lines. Owner: Task 6 (`TestDeletes_TenantIsolation`).

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/config/config.go` (modify) | `Validate()` limits (Task 1); `compactor:` block (Task 6) |
| `internal/ingest/handler.go` (modify) | Per-stream retention via Loki's `retention.TenantRetentionSnapshot` (Task 1) |
| `internal/chstore/schema.sql` (modify) | `stream_stats_hourly` + MV (Task 2); `delete_requests` (Task 6) |
| `internal/chstore/stats.go` (new) | `HourlyStats` SQL, `IndexStats`, `Volume`, the pure `statsFromHours`/`volumeFromHours` (Tasks 2, 3) |
| `internal/chstore/detected.go` (new) | `DetectedLabels` + copied Loki cardinality helpers (Task 4) |
| `internal/chstore/querier.go` (modify) | Query-time delete filtering in `SelectLogs`/`SelectSamples` (Task 6) |
| `internal/chstore/deleterows.go` (new) | `RowKeys`, `DeleteRows`, `DeleteRange` for physical deletion (Task 7) |
| `internal/api/lokicodec.go` (new) | `lokiHandler` (queryrangebase.Handler) and the Loki codec route set (Tasks 2–5) |
| `internal/api/format_query.go` (new) | Copied `format_query` handler (Task 9) |
| `internal/api/api.go` (modify) | Routes and new `API` fields (Tasks 2–9) |
| `internal/patterns/patterns.go` (new) | Query-time Drain over ClickHouse rows; copied `prunePatterns` (Task 5) |
| `internal/deletes/store.go` (new) | Delete requests in ClickHouse; implements Loki's `deletion.DeleteGetter` (Task 6) |
| `internal/deletes/handler.go` (new) | Copied Loki delete request HTTP handler + tenant `deletion_mode` gate (Task 6) |
| `internal/deletes/processor.go` (new) | Background physical deletion after the cancel period (Task 7) |
| `internal/tail/ingester.go` (new) | `tail.Ingester` + polling `Querier_TailClient` (Task 8) |
| `internal/pushdown/tiera.go` (modify) | `ErrFallback` while a pending delete overlaps the leaf (Task 6) |
| `cmd/lokihouse/main.go` (modify) | Wiring (Tasks 2–9) |
| `test/oracle/compare.go` (modify) | `CompareMut` + JSON mutators (Task 2) |
| `test/oracle/phase3_test.go` (new) | Phase 3 endpoint cases + delete and tail oracles (Tasks 2–9) |
| `test/oracle/loki.go`, `test/oracle/lokihouse.go` (modify) | Loki/lokihouse oracle config additions (Tasks 3, 5, 6) |
| `docs/divergences.md` (new), `docs/phase3-status.md` (new) | Divergence register (Tasks 2–8 append), status (Task 9) |

---

### Task 1: Per-stream retention (`retention_stream`)

Phase 1 applies only the tenant `retention_period`. Loki's compactor picks a retention per stream from `retention_stream` rules: among matching rules the highest `priority` wins, and on equal priority the shorter `period` wins. Lokihouse applies the same rule at ingest by calling Loki's own function. The result goes into `retention_days`, which TTL and partitioning already use.

**Files:**
- Modify: `internal/ingest/handler.go` (`retentionDays`)
- Modify: `internal/config/config.go` (`Load` calls `Validate`)
- Test: `internal/ingest/handler_test.go`, `internal/config/config_test.go`

**Interfaces:**
- Consumes: `retention.NewTenantRetentionSnapshot(limits retention.Limits, userID string) *TenantRetentionSnapshot` and `(*TenantRetentionSnapshot).RetentionPeriodFor(labels.Labels) time.Duration` from `github.com/grafana/loki/v3/pkg/compactor/retention`. `*validation.Overrides` satisfies `retention.Limits`.
- Produces: no new exported API.

- [ ] **Step 1: Write the failing ingest test.** Add to `internal/ingest/handler_test.go`:

```go
func TestPush_StreamRetention(t *testing.T) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.RetentionPeriod = model.Duration(31 * 24 * time.Hour)
	l.StreamRetention = []validation.StreamRetention{
		{Period: model.Duration(48 * time.Hour), Priority: 1, Selector: `{app="short"}`},
		// equal priority: the shorter period wins
		{Period: model.Duration(72 * time.Hour), Priority: 1, Selector: `{app="short"}`},
		// higher priority wins over a shorter period
		{Period: model.Duration(96 * time.Hour), Priority: 2, Selector: `{app="prio"}`},
		{Period: model.Duration(24 * time.Hour), Priority: 1, Selector: `{app="prio"}`},
		// a selector on the label discovered at ingest
		{Period: model.Duration(120 * time.Hour), Priority: 1, Selector: `{service_name="svc"}`},
	}
	require.NoError(t, l.Validate())
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()}

	ts := itoa(time.Now().UnixNano())
	body := `{"streams":[` +
		`{"stream":{"app":"short"},"values":[["` + ts + `","a"]]},` +
		`{"stream":{"app":"prio"},"values":[["` + ts + `","b"]]},` +
		`{"stream":{"app":"other","service_name":"svc"},"values":[["` + ts + `","c"]]},` +
		`{"stream":{"app":"default"},"values":[["` + ts + `","d"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	got := map[string]uint16{}
	rows, err := conn.Query(context.Background(), "SELECT line, retention_days FROM logs WHERE tenant = 't1'")
	require.NoError(t, err)
	for rows.Next() {
		var line string
		var d uint16
		require.NoError(t, rows.Scan(&line, &d))
		got[line] = d
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]uint16{"a": 2, "b": 4, "c": 5, "d": 31}, got)

	var sd uint16
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT retention_days FROM streams WHERE tenant = 't1' AND labels['app'] = 'short'").Scan(&sd))
	require.Equal(t, uint16(2), sd, "streams rows carry the same retention as their logs")
}
```

(`model` is `github.com/prometheus/common/model`. If `service_name` is already given in the push, Loki's discovery keeps it, so stream `c` keeps `service_name="svc"`.)

- [ ] **Step 2: Write the failing config test.** Add to `internal/config/config_test.go`:

```go
func TestLoad_RetentionStreamMatchersPopulated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 48h
`), 0o600))
	c, err := config.Load(p)
	require.NoError(t, err)
	require.Len(t, c.LimitsConfig.StreamRetention, 1)
	require.NotEmpty(t, c.LimitsConfig.StreamRetention[0].Matchers, "Validate must populate matchers")
}

func TestLoad_InvalidRetentionStreamRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 1h
`), 0o600))
	_, err := config.Load(p)
	require.ErrorContains(t, err, "retention period must be >= 24h")
}
```

- [ ] **Step 3: Run both and verify they fail.**

Run: `go test -count=1 -run 'TestPush_StreamRetention|TestLoad_RetentionStream|TestLoad_InvalidRetentionStream' ./internal/ingest/ ./internal/config/`
Expected: FAIL. `TestPush_StreamRetention` gets 31 for every line; the config tests see empty matchers / no error.

- [ ] **Step 4: Implement.** In `internal/ingest/handler.go` replace `retentionDays`:

```go
// retentionDays returns the row TTL in whole days (rounded up) for each stream:
// Loki's compactor rule (retention_stream, highest priority wins, shorter period
// on a tie, else the tenant retention_period). 0 keeps rows forever.
func (h *Handler) retentionDays(tenantID string) func(labels.Labels) uint16 {
	snap := retention.NewTenantRetentionSnapshot(h.Limits, tenantID)
	return func(ls labels.Labels) uint16 {
		d := min((snap.RetentionPeriodFor(ls)+24*time.Hour-1)/(24*time.Hour), math.MaxUint16)
		return uint16(d)
	}
}
```

Import `github.com/grafana/loki/v3/pkg/compactor/retention`. In `internal/config/config.go` `Load`, after the decode and before the pushdown check:

```go
	if err := c.LimitsConfig.Validate(); err != nil {
		return nil, fmt.Errorf("limits_config: %w", err)
	}
```

- [ ] **Step 5: Run the tests and verify they pass**, then the packages: `go test -count=1 ./internal/ingest/ ./internal/config/`. Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add internal/ingest internal/config go.mod go.sum
git commit -m "feat(ingest): per-stream retention via Loki's retention_stream rule"
```

---

### Task 2: `stream_stats_hourly` and `index/stats`

**Files:**
- Modify: `internal/chstore/schema.sql`
- Create: `internal/chstore/stats.go`, `internal/chstore/stats_test.go`
- Create: `internal/api/lokicodec.go`; Modify: `internal/api/api.go`, `cmd/lokihouse/main.go`
- Test: `internal/api/lokicodec_test.go`
- Modify: `test/oracle/compare.go`; Create: `test/oracle/phase3_test.go`; Modify: `test/oracle/oracle_test.go` (one call)
- Create: `docs/divergences.md`

**Interfaces:**
- Produces (used by Tasks 3–5):
  - `type HourStat struct { StreamID, Entries, Bytes uint64; MinTS, MaxTS int64 }` (ns)
  - `func HourlyStats(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, from, through time.Time) ([]HourStat, error)`
  - `func (q *Querier) IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error)`
  - `api.lokiHandler` with `Do(ctx, queryrangebase.Request) (queryrangebase.Response, error)`, plus `func (a *API) lokiRoutes(mux *http.ServeMux)`
  - New `API` field `Limits *validation.Overrides`
  - Oracle: `CompareMut(t, name string, a, b []byte, mut func(any))`, `dropKeys(...string) func(any)`, `sortArrayBy(field, key string) func(any)`, `chain(...func(any)) func(any)`, `type endpointCase struct{ name, path string; params url.Values; mut func(any); check func(t *testing.T, loki, house []byte) }`, `var phase3Cases []endpointCase`, `func phase3Endpoints(t *testing.T, loki, house string, start, end time.Time)`

- [ ] **Step 1: Write the failing pure-function test** `internal/chstore/stats_test.go` (package `chstore`, internal test). It pins Loki's factor and overlap rules (Review Focus 4):

```go
package chstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatsFromHours(t *testing.T) {
	const s = int64(1e9)
	rows := []HourStat{
		{StreamID: 1, Entries: 100, Bytes: 1000, MinTS: 0, MaxTS: 100 * s},    // half inside [50s, 200s]
		{StreamID: 1, Entries: 10, Bytes: 10, MinTS: 150 * s, MaxTS: 160 * s}, // fully inside
		{StreamID: 2, Entries: 1, Bytes: 7, MinTS: 120 * s, MaxTS: 120 * s},   // single entry: factor 1
		{StreamID: 3, Entries: 5, Bytes: 5, MinTS: 300 * s, MaxTS: 310 * s},   // outside
		{StreamID: 4, Entries: 1, Bytes: 1, MinTS: 50 * s, MaxTS: 50 * s},     // from == min == max: Loki skips (from < max is false)
	}
	got := statsFromHours(rows, 50*s, 200*s)
	require.Equal(t, uint64(2), got.Streams)
	require.Equal(t, uint64(3), got.Chunks)
	require.Equal(t, uint64(50+10+1), got.Entries)
	require.Equal(t, uint64(500+10+7), got.Bytes)

	empty := statsFromHours(nil, 0, s)
	require.Zero(t, empty.Streams+empty.Chunks+empty.Entries+empty.Bytes)
}
```

- [ ] **Step 2: Run it and verify it fails.** Run: `go test -count=1 -run TestStatsFromHours ./internal/chstore/`. Expected: FAIL (undefined `HourStat`, `statsFromHours`).

- [ ] **Step 3: Add the schema.** In `internal/chstore/schema.sql`, insert after the `streams` table and before `{{if .Cluster}}`:

```sql
-- stream_stats_hourly: per (tenant, stream, hour) entry count, bytes and first/last entry
-- timestamp, fed at insert time by the MV below. It serves index/stats and index/volume
-- with Loki's chunk pro-rating rule applied per hourly bucket. Bytes follow Loki's head
-- block accounting: len(line) + 8 per structured-metadata pair (no symbol table).
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "stream_stats_hourly"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant         LowCardinality(String),
  stream_id      UInt64,
  hour           DateTime('UTC'),
  retention_days UInt16,
  entries        SimpleAggregateFunction(sum, UInt64),
  bytes          SimpleAggregateFunction(sum, UInt64),
  min_ts         SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
  max_ts         SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = {{.Engine "AggregatingMergeTree"}}
PARTITION BY (toDate(hour), retention_days)
ORDER BY (tenant, stream_id, hour)
TTL {{ttl "toDate32(hour)"}}
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DBName}}.{{.Local "stream_stats_hourly"}}_mv{{with .OnCluster}} {{.}}{{end}}
TO {{.DBName}}.{{.Local "stream_stats_hourly"}} AS
SELECT tenant, stream_id, toStartOfHour(toDateTime(ts, 'UTC')) AS hour, retention_days,
       toUInt64(count()) AS entries,
       toUInt64(sum(length(line) + 8 * length(mapKeys(structured_metadata)))) AS bytes,
       min(ts) AS min_ts, max(ts) AS max_ts
FROM {{.DBName}}.{{.Local "logs"}}
GROUP BY tenant, stream_id, hour, retention_days;
```

Inside the `{{if .Cluster}}` block add:

```sql
CREATE TABLE IF NOT EXISTS {{.DBName}}.stream_stats_hourly{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.stream_stats_hourly_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'stream_stats_hourly_local', cityHash64(tenant, stream_id));
```

If `internal/chstore/schema_test.go` asserts the statement list or count, update it to include the three new statements (two on a single server).

- [ ] **Step 4: Implement `internal/chstore/stats.go`.**

```go
package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/util"
)

// HourStat is one (stream, hour) bucket of stream_stats_hourly; timestamps in ns.
type HourStat struct {
	StreamID, Entries, Bytes uint64
	MinTS, MaxTS             int64
}

// HourlyStats returns the hourly buckets of the given streams that can overlap
// [from, through]. Unmerged SimpleAggregateFunction rows are merged by the GROUP BY.
func HourlyStats(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, from, through time.Time) ([]HourStat, error) {
	const q = `SELECT stream_id, sum(entries), sum(bytes),
       toUnixTimestamp64Nano(min(min_ts)), toUnixTimestamp64Nano(max(max_ts))
FROM stream_stats_hourly
WHERE tenant = ? AND stream_id IN ?
  AND hour >= toStartOfHour(fromUnixTimestamp64Nano(?, 'UTC'))
  AND hour <= fromUnixTimestamp64Nano(?, 'UTC')
GROUP BY stream_id, hour`
	rows, err := conn.Query(ctx, q, tenant, ids, from.UnixNano(), through.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HourStat
	for rows.Next() {
		var h HourStat
		if err := rows.Scan(&h.StreamID, &h.Entries, &h.Bytes, &h.MinTS, &h.MaxTS); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// overlaps and factor follow Loki's ingester getStats/getVolume, with an hourly
// bucket in place of a chunk: a bucket counts when from < max && through > min.
func overlaps(h HourStat, from, through int64) bool { return from < h.MaxTS && through > h.MinTS }

func statsFromHours(rows []HourStat, from, through int64) *logproto.IndexStatsResponse {
	res := &logproto.IndexStatsResponse{}
	streams := map[uint64]struct{}{}
	for _, h := range rows {
		if !overlaps(h, from, through) {
			continue
		}
		f := util.GetFactorOfTime(from, through, h.MinTS, h.MaxTS)
		res.Chunks++
		res.Entries += uint64(f * float64(h.Entries))
		res.Bytes += uint64(f * float64(h.Bytes))
		streams[h.StreamID] = struct{}{}
	}
	res.Streams = uint64(len(streams))
	return res
}

// IndexStats serves /loki/api/v1/index/stats from stream_stats_hourly.
func (q *Querier) IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	ms, err := syntax.ParseMatchers(req.Matchers, true)
	if err != nil {
		return nil, err
	}
	from, through := req.From.Time(), req.Through.Time()
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
	if err != nil || len(streams) == 0 {
		return &logproto.IndexStatsResponse{}, err
	}
	rows, err := HourlyStats(ctx, q.Conn, tenantID, sortedIDs(streams), from, through)
	if err != nil {
		return nil, err
	}
	return statsFromHours(rows, from.UnixNano(), through.UnixNano()), nil
}
```

- [ ] **Step 5: Run `TestStatsFromHours` and verify it passes.** Run: `go test -count=1 -run TestStatsFromHours ./internal/chstore/`. Expected: PASS.

- [ ] **Step 6: Write the failing API test** `internal/api/lokicodec_test.go`. Reuse `newServer` and `do` from `api_test.go`, and extend `newServer` to set `Limits: o`:

```go
func TestAPI_IndexStats(t *testing.T) {
	srv, conn := newServerConn(t)
	now := time.Now()
	var vals []string
	for i, line := range []string{"a", "bb", "ccc"} {
		vals = append(vals, `["`+strconv.FormatInt(now.Add(time.Duration(i)*time.Second).UnixNano(), 10)+`","`+line+`"]`)
	}
	code, body := do(t, "POST", srv.URL+"/loki/api/v1/push", "", `{"streams":[{"stream":{"app":"x"},"values":[`+strings.Join(vals, ",")+`]}]}`)
	require.Equal(t, http.StatusNoContent, code, body)

	var wantBytes uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT sum(length(line) + 8 * length(mapKeys(structured_metadata))) FROM logs WHERE tenant = 'fake'").Scan(&wantBytes))

	qs := url.Values{"query": {`{app="x"}`},
		"start": {strconv.FormatInt(now.Add(-time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	code, body = do(t, "GET", srv.URL+"/loki/api/v1/index/stats?"+qs.Encode(), "", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, fmt.Sprintf(`{"streams":1,"chunks":1,"bytes":%d,"entries":3}`, wantBytes), body)
}
```

`newServerConn(t)` is `newServer` refactored to also return the `driver.Conn`; `newServer` becomes `srv, _ := newServerConn(t); return srv`. If the three entries straddle an hour boundary, `chunks` is 2. Push them at `now.Truncate(time.Hour).Add(30*time.Minute)` and use that as `now`, so `chunks` is always 1.

- [ ] **Step 7: Run it and verify it fails** (404 from the mux). Run: `go test -count=1 -run TestAPI_IndexStats ./internal/api/`.

- [ ] **Step 8: Implement `internal/api/lokicodec.go`.**

```go
package api

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// lokiHandler answers the requests Loki's query-frontend codec decodes for the
// index and drilldown endpoints. Requests are decoded and responses encoded by
// queryrange.DefaultCodec, so the wire format is Loki's own.
type lokiHandler struct{ a *API }

func (h lokiHandler) Do(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	switch r := req.(type) {
	case *logproto.IndexStatsRequest:
		res, err := h.a.Q.IndexStats(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.IndexStatsResponse{Response: res}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "unsupported request type %T", req)
	}
}

// lokiRoutes registers the endpoints served through Loki's codec.
func (a *API) lokiRoutes(mux *http.ServeMux) {
	var h queryrangebase.Handler = lokiHandler{a: a}
	codec := a.tenant(queryrange.NewSerializeHTTPHandler(h, queryrange.DefaultCodec))
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/index/stats", codec)
	}
}
```

In `api.go` add the field `Limits *validation.Overrides` to `API` and call `a.lokiRoutes(mux)` in `Router()`. In `cmd/lokihouse/main.go` set `Limits: limits`. Run `go mod tidy`.

- [ ] **Step 9: Run the API test and verify it passes.** Run: `go test -count=1 ./internal/api/ ./internal/chstore/`. Expected: PASS.

- [ ] **Step 10: Add the oracle plumbing.** In `test/oracle/compare.go` add:

```go
// CompareMut applies mut to both decoded bodies before Compare. Use it for fields
// that legitimately differ (storage-specific counters, map-iteration order).
func CompareMut(t *testing.T, name string, a, b []byte, mut func(any)) {
	t.Helper()
	if mut != nil {
		a, b = mutate(a, mut), mutate(b, mut)
	}
	Compare(t, name, a, b)
}

func mutate(raw []byte, mut func(any)) []byte {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return raw
	}
	mut(v)
	out, _ := json.Marshal(v)
	return out
}

// dropKeys deletes keys from the top-level object (and from its "data" object).
func dropKeys(keys ...string) func(any) {
	return func(v any) {
		for _, m := range objects(v) {
			for _, k := range keys {
				delete(m, k)
			}
		}
	}
}

// sortArrayBy sorts the array field of the top-level (or "data") object by the
// string value of key in each element.
func sortArrayBy(field, key string) func(any) {
	return func(v any) {
		for _, m := range objects(v) {
			arr, _ := m[field].([]any)
			sort.SliceStable(arr, func(i, j int) bool {
				a, _ := arr[i].(map[string]any)
				b, _ := arr[j].(map[string]any)
				return fmt.Sprint(a[key]) < fmt.Sprint(b[key])
			})
		}
	}
}

func chain(fs ...func(any)) func(any) {
	return func(v any) {
		for _, f := range fs {
			f(v)
		}
	}
}

func objects(v any) []map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := []map[string]any{m}
	if d, ok := m["data"].(map[string]any); ok {
		out = append(out, d)
	}
	return out
}
```

Create `test/oracle/phase3_test.go`:

```go
//go:build oracle

package oracle

import (
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// endpointCase is one Phase 3 API call compared between Loki and lokihouse.
// check, when set, replaces the default CompareMut.
type endpointCase struct {
	name   string
	path   string
	params url.Values
	mut    func(any)
	check  func(t *testing.T, loki, house []byte)
}

// Stats and volume run on selectors without web/dev/p1, which carries the
// fixture's deliberate duplicates (see docs/divergences.md).
var phase3Cases = []endpointCase{
	{name: "index_stats_api", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app="api"}`}}, check: checkStats},
	{name: "index_stats_api_legacy", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app=~"api|legacy"}`}}, check: checkStats},
	{name: "index_stats_prod", path: "/loki/api/v1/index/stats", params: url.Values{"query": {`{app=~"api|legacy", env="prod"}`}}, check: checkStats},
}

// phase3Endpoints runs every case for both tenants over [start, end].
func phase3Endpoints(t *testing.T, loki, house string, start, end time.Time) {
	for _, c := range phase3Cases {
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			t.Run("p3/"+tenant+"/"+c.name, func(t *testing.T) {
				v := url.Values{}
				for k, vs := range c.params {
					v[k] = vs
				}
				v.Set("start", strconv.FormatInt(start.UnixNano(), 10))
				v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
				u := c.path + "?" + v.Encode()
				a, b := Get(t, loki+u, tenant), Get(t, house+u, tenant)
				if c.check != nil {
					c.check(t, a, b)
					return
				}
				CompareMut(t, c.name, a, b, c.mut)
			})
		}
	}
}

// checkStats compares index/stats: streams and entries exactly, bytes within
// 1% (Loki adds a per-chunk symbol table; docs/divergences.md). "chunks" is
// storage-specific and ignored.
func checkStats(t *testing.T, loki, house []byte) {
	t.Helper()
	var a, b struct{ Streams, Entries, Bytes uint64 }
	if err := json.Unmarshal(loki, &a); err != nil {
		t.Fatalf("loki: %v: %s", err, loki)
	}
	if err := json.Unmarshal(house, &b); err != nil {
		t.Fatalf("lokihouse: %v: %s", err, house)
	}
	if a.Streams != b.Streams || a.Entries != b.Entries || !within(float64(a.Bytes), float64(b.Bytes), 0.01) {
		t.Errorf("index/stats differ:\nloki=%s\nhouse=%s", loki, house)
	}
}

func within(a, b, rel float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= rel*math.Max(math.Abs(a), math.Abs(b))
}
```

In `TestOracle` (`oracle_test.go`), after the `meta` loop and before the Tier A coverage block, add:

```go
	phase3Endpoints(t, loki, house, start.Add(-time.Minute), end)
```

- [ ] **Step 11: Run the oracle.** Run: `make oracle`. Expected: PASS, including `p3/*/index_stats_*`. If entries differ, check first whether Loki saw the same duplicates (`WaitQueryable` counts after dedup). Do not loosen `checkStats`.

- [ ] **Step 12: Create `docs/divergences.md`** with this content:

```markdown
# Known divergences from Loki

Spec §7.4: every accepted difference, with its reason. Target: empty apart from these.

## Tie order inside one stream and timestamp (Phase 1)
Loki keeps insertion order; lokihouse cannot. The oracle compares such groups as multisets.

## index/stats and index/volume (Phase 3)
- **Granularity:** Loki pro-rates per chunk (bounds = first/last entry of the chunk); lokihouse pro-rates per hourly bucket (bounds = first/last entry in that hour). Ranges that cover all data in a bucket/chunk agree; ranges cutting through a chunk can differ.
- **Duplicates:** Loki's head block drops exact duplicates before counting; the materialized view counts every inserted row, so client retries of the same push count twice.
- **Bytes:** Loki adds each chunk's structured-metadata symbol table; lokihouse counts `len(line) + 8 × pairs` only (≈0.2% for typical metadata).
- **chunks:** the number of overlapping hourly buckets, not Loki chunks.
```

- [ ] **Step 13: Run the full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`. All must pass.

```bash
git add internal/chstore internal/api cmd go.mod go.sum test/oracle docs/divergences.md
git commit -m "feat: stream_stats_hourly MV and /loki/api/v1/index/stats via Loki's codec"
```

---

### Task 3: `index/volume` and `index/volume_range`

Loki's querier returns a `VolumeResponse` per bucket. Its frontend `NewVolumeMiddleware` splits the request by `step` and turns the per-bucket answers into a Prometheus vector or matrix. Lokihouse wraps `lokiHandler` with that exported middleware, so the bucketing and response shape are Loki's. The per-bucket volume copies the aggregation logic of the ingester's `getVolume`.

**Files:**
- Modify: `internal/chstore/stats.go`, `internal/chstore/stats_test.go`
- Modify: `internal/api/lokicodec.go`, `internal/api/lokicodec_test.go`
- Modify: `test/oracle/loki.go`, `test/oracle/lokihouse.go`, `test/oracle/phase3_test.go`

**Interfaces:**
- Consumes: `HourlyStats`, `overlaps`, `ResolveStreams` (Task 2); `util.PrepareLabelsAndMatchers` (`pkg/util`), `seriesvolume.MatchAny`, `seriesvolume.AggregateBySeries`, `seriesvolume.MapToVolumeResponse` (`pkg/storage/stores/index/seriesvolume`), `queryrange.NewVolumeMiddleware()`.
- Produces: `func (q *Querier) Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error)`; `func volumeFromHours(streams []Stream, rows []HourStat, from, through int64, req *logproto.VolumeRequest, labelsToMatch map[string]struct{}, matchAny bool) *logproto.VolumeResponse`.

- [ ] **Step 1: Write the failing pure test** (add to `stats_test.go`):

```go
func TestVolumeFromHours(t *testing.T) {
	const s = int64(1e9)
	a := Stream{ID: 1, Labels: labels.FromStrings("app", "a", "env", "dev")}
	b := Stream{ID: 2, Labels: labels.FromStrings("app", "b", "env", "dev")}
	rows := []HourStat{
		{StreamID: 1, Bytes: 100, MinTS: 0, MaxTS: 10 * s},
		{StreamID: 2, Bytes: 40, MinTS: 0, MaxTS: 10 * s},
	}
	// aggregate by series, all labels
	got := volumeFromHours([]Stream{a, b}, rows, 0, 20*s, &logproto.VolumeRequest{Limit: 10, AggregateBy: "series"}, nil, true)
	require.Equal(t, []logproto.Volume{
		{Name: `{app="a", env="dev"}`, Volume: 100},
		{Name: `{app="b", env="dev"}`, Volume: 40},
	}, got.Volumes)
	// aggregate by labels with a target label: per label name
	got = volumeFromHours([]Stream{a, b}, rows, 0, 20*s,
		&logproto.VolumeRequest{Limit: 10, AggregateBy: "labels", TargetLabels: []string{"env"}},
		map[string]struct{}{"env": {}}, false)
	require.Equal(t, []logproto.Volume{{Name: "env", Volume: 140}}, got.Volumes)
	// limit
	got = volumeFromHours([]Stream{a, b}, rows, 0, 20*s, &logproto.VolumeRequest{Limit: 1, AggregateBy: "series"}, nil, true)
	require.Len(t, got.Volumes, 1)
}
```

- [ ] **Step 2: Run it and verify it fails.** Run: `go test -count=1 -run TestVolumeFromHours ./internal/chstore/`.

- [ ] **Step 3: Implement** in `stats.go`. `volumeFromHours` is the stream loop of `pkg/ingester/instance.go` `getVolume` (lines 961–1021 at the pinned commit), copied with the attribution comment. Modifications: iterate `streams` instead of ingester streams; a stream's size is the sum over its overlapping buckets of `uint64(float64(h.Bytes) * factor)`; streams without an overlapping bucket are skipped.

```go
// Copied from grafana/loki@fbc3f077 pkg/ingester/instance.go (getVolume); modifications:
// ClickHouse streams and hourly buckets instead of ingester streams and chunks.
func volumeFromHours(streams []Stream, rows []HourStat, from, through int64, req *logproto.VolumeRequest, labelsToMatch map[string]struct{}, matchAny bool) *logproto.VolumeResponse {
	sizes := map[uint64]uint64{}
	seen := map[uint64]bool{}
	for _, h := range rows {
		if !overlaps(h, from, through) {
			continue
		}
		f := util.GetFactorOfTime(from, through, h.MinTS, h.MaxTS)
		sizes[h.StreamID] += uint64(float64(h.Bytes) * f)
		seen[h.StreamID] = true
	}
	aggregateBySeries := seriesvolume.AggregateBySeries(req.AggregateBy) || req.AggregateBy == ""
	volumes := map[string]uint64{}
	b := labels.NewScratchBuilder(len(labelsToMatch))
	for _, s := range streams {
		if !seen[s.ID] {
			continue
		}
		size := sizes[s.ID]
		if aggregateBySeries {
			b.Reset()
			s.Labels.Range(func(l labels.Label) {
				if _, ok := labelsToMatch[l.Name]; matchAny || ok {
					b.Add(l.Name, l.Value)
				}
			})
			volumes[b.Labels().String()] += size
			continue
		}
		s.Labels.Range(func(l labels.Label) {
			if len(req.TargetLabels) > 0 {
				if _, ok := labelsToMatch[l.Name]; matchAny || ok {
					volumes[l.Name] += size
				}
			} else {
				volumes[l.Name] += size
			}
		})
	}
	return seriesvolume.MapToVolumeResponse(volumes, int(req.Limit))
}

// Volume serves one bucket of /loki/api/v1/index/volume(_range).
func (q *Querier) Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	ms, err := syntax.ParseMatchers(req.Matchers, true)
	if err != nil && req.Matchers != seriesvolume.MatchAny {
		return nil, err
	}
	labelsToMatch, ms, matchAny := util.PrepareLabelsAndMatchers(req.TargetLabels, ms)
	matchAny = matchAny || len(ms) == 0
	from, through := req.From.Time(), req.Through.Time()
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, from, through)
	if err != nil || len(streams) == 0 {
		return &logproto.VolumeResponse{Volumes: []logproto.Volume{}}, err
	}
	rows, err := HourlyStats(ctx, q.Conn, tenantID, sortedIDs(streams), from, through)
	if err != nil {
		return nil, err
	}
	return volumeFromHours(streams, rows, from.UnixNano(), through.UnixNano(), req, labelsToMatch, matchAny), nil
}
```

In `lokicodec.go`:
- Add a case to `Do`. Return Loki's feature-flag error first, the same as the frontend's `volumeFeatureFlagRoundTripper`:

```go
	case *logproto.VolumeRequest:
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}
		if !h.a.Limits.VolumeEnabled(tenantID) {
			return nil, httpgrpc.Errorf(http.StatusNotFound, "not found")
		}
		res, err := h.a.Q.Volume(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.VolumeResponse{Response: res}, nil
```

- In `lokiRoutes`, wrap the handler and register the routes:

```go
	h = queryrange.NewVolumeMiddleware().Wrap(h)
	// …
		mux.Handle(m+" /loki/api/v1/index/volume", codec)
		mux.Handle(m+" /loki/api/v1/index/volume_range", codec)
```

- [ ] **Step 4: Add the API test** to `lokicodec_test.go`. Push the same three entries as in Task 2's test. Then:
  - with default limits (`volume_enabled: false`), `GET /loki/api/v1/index/volume?query={app="x"}` returns 404 `not found`;
  - with `l.VolumeEnabled = true` (add a `newServerWithLimits(t, func(*validation.Limits))` helper), the body is `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"x","service_name":"x"},"value":[<ts>,"<bytes>"]}]}}`. Check `status`, `resultType`, the metric and the value from `SELECT sum(length(line) + 8 * length(mapKeys(structured_metadata))) FROM logs`; the value's timestamp is whatever the request `end` aligns to.

- [ ] **Step 5: Run and verify it passes.** Run: `go test -count=1 -run 'TestVolumeFromHours|TestAPI_' ./internal/chstore/ ./internal/api/`. Expected: PASS.

- [ ] **Step 6: Oracle.** Add `  volume_enabled: true` under `limits_config:` in both `lokiConfigTemplate` (`test/oracle/loki.go`) and the lokihouse config in `test/oracle/lokihouse.go`. Append to `phase3Cases`:

```go
	{name: "volume_series", path: "/loki/api/v1/index/volume", params: url.Values{"query": {`{app=~"api|legacy"}`}}, check: checkVolume},
	{name: "volume_labels_env", path: "/loki/api/v1/index/volume", params: url.Values{"query": {`{app=~"api|legacy"}`}, "targetLabels": {"env"}, "aggregateBy": {"labels"}}, check: checkVolume},
	{name: "volume_range_10m", path: "/loki/api/v1/index/volume_range", params: url.Values{"query": {`{app="api"}`}, "step": {"600"}}, check: checkVolume},
```

with

```go
// checkVolume compares index/volume(_range): same metrics and timestamps,
// values within 1% (docs/divergences.md: bytes accounting).
func checkVolume(t *testing.T, loki, house []byte) {
	t.Helper()
	type res struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	var a, b res
	if json.Unmarshal(loki, &a) != nil || json.Unmarshal(house, &b) != nil {
		t.Fatalf("decode:\nloki=%s\nhouse=%s", loki, house)
	}
	points := func(r res) map[string]float64 {
		out := map[string]float64{}
		for _, s := range r.Data.Result {
			key, _ := json.Marshal(s.Metric)
			vs := s.Values
			if s.Value != nil {
				vs = [][]any{s.Value}
			}
			for _, p := range vs {
				f, _ := strconv.ParseFloat(p[1].(string), 64)
				out[string(key)+"@"+fmt.Sprint(p[0])] = f
			}
		}
		return out
	}
	pa, pb := points(a), points(b)
	ok := a.Data.ResultType == b.Data.ResultType && len(pa) == len(pb)
	for k, v := range pa {
		if w, found := pb[k]; !found || !within(v, w, 0.01) {
			ok = false
		}
	}
	if !ok {
		t.Errorf("volume differs:\nloki=%s\nhouse=%s", loki, house)
	}
}
```

Run `make oracle`. Expected: PASS.

- [ ] **Step 7: Full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal test/oracle
git commit -m "feat: /loki/api/v1/index/volume and volume_range via Loki's volume middleware"
```

---

### Task 4: `detected_labels`, `detected_fields`, `detected_field/{name}/values`

**detected_labels:** Loki's querier collects label → values for the matching streams and turns them into `DetectedLabel{Label, Cardinality, Sketch}`, using unexported helpers that are copied here. The frontend's exported `NewDetectedLabelsCardinalityFilter` then strips the sketches.

**detected_fields:** Loki's frontend handler `queryrange.NewDetectedFieldsHandler(limitedHandler, logHandler, limits)` runs a BACKWARD log query with the `categorize-labels` encoding flag and parses the fields from the result. Both handlers passed in run that log query on lokihouse's engine.

**Files:**
- Create: `internal/chstore/detected.go`, `internal/chstore/detected_test.go`
- Modify: `internal/api/lokicodec.go`, `internal/api/lokicodec_test.go`
- Modify: `test/oracle/phase3_test.go`

**Interfaces:**
- Consumes: `ResolveStreams`; `querier_limits.ValidateQueryTimeRangeLimits(ctx, userID, limits, start, end)` (`pkg/querier/limits`); `queryrange.NewDetectedLabelsCardinalityFilter`, `queryrange.NewDetectedFieldsHandler`, `queryrange.ParamsFromRequest`, `queryrange.ResultToResponse`; `mux.SetURLVars` (`github.com/gorilla/mux`).
- Produces: `func (q *Querier) DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error)`.

- [ ] **Step 1: Write the failing chstore test** `internal/chstore/detected_test.go` (package `chstore_test`). Seed three streams with `chstore.Writer`:
  - `{app="a", pod="p1", env="dev", id="0f8e8c1a-5b2d-4c7e-9a3f-1d2e3f4a5b6c"}`
  - `{app="a", pod="p2", env="dev", id="7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d"}`
  - `{app="b", pod="p1", env="prod", id="9a2b3c4d-5e6f-4789-a0b1-c2d3e4f5a6b7"}`

  Call `DetectedLabels` with `Query: ""` and the time range. Assert:
  - the label set is `{app, pod, env}`: `id` is rejected because every value is an ID type (`shouldRejectLabel`);
  - cardinalities are `app=2, pod=2, env=2`;
  - every `Sketch` is non-empty.

  With `Query: {app="a"}`: `app` has a single value and is not static, so it is rejected; `pod` is static and kept with cardinality 2; `env` has the single value `dev`. Expected set: `{pod}`. Verify this against the copied `shouldRejectLabel` while implementing, and fix the expectation only if the copied Loki code says otherwise. The oracle decides.

- [ ] **Step 2: Run and verify it fails** (undefined method).

- [ ] **Step 3: Implement `internal/chstore/detected.go`.**
  - Copy from `pkg/querier/querier.go` at the pinned commit, verbatim, with the attribution header: `countLabelsAndCardinality`, `shouldRejectLabel`, `containsAllIDTypes` and any helper they call, the `parsedFields` type, `newParsedLabels`, and the `parsedFields` methods `Insert`, `Estimate`, `Marshal`, `DetermineType`.
  - Then:

```go
// DetectedLabels serves /loki/api/v1/detected_labels from the streams table:
// the store path of Loki's SingleTenantQuerier.DetectedLabels.
func (q *Querier) DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	var ms []*labels.Matcher
	if req.Query != "" {
		if ms, err = syntax.ParseMatchers(req.Query, true); err != nil {
			return nil, err
		}
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	values := map[string]map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) {
			if values[l.Name] == nil {
				values[l.Name] = map[string]struct{}{}
			}
			values[l.Name][l.Value] = struct{}{}
		})
	}
	if len(values) == 0 {
		return &logproto.DetectedLabelsResponse{DetectedLabels: []*logproto.DetectedLabel{}}, nil
	}
	store := make(map[string][]string, len(values))
	for name, vs := range values {
		for v := range vs {
			store[name] = append(store[name], v)
		}
		sort.Strings(store[name])
	}
	static := map[string]struct{}{"cluster": {}, "namespace": {}, "instance": {}, "pod": {}}
	return &logproto.DetectedLabelsResponse{DetectedLabels: countLabelsAndCardinality(store, nil, static)}, nil
}
```

- [ ] **Step 4: Wire `lokicodec.go`.** Add these cases to `Do`:

```go
	case *queryrange.DetectedLabelsRequest:
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}
		dl := r.DetectedLabelsRequest
		if dl.Start, dl.End, err = querier_limits.ValidateQueryTimeRangeLimits(ctx, tenantID, h.a.Limits, dl.Start, dl.End); err != nil {
			return nil, err
		}
		res, err := h.a.Q.DetectedLabels(ctx, &dl)
		if err != nil {
			return nil, err
		}
		return &queryrange.DetectedLabelsResponse{Response: res}, nil
	case *queryrange.DetectedFieldsRequest:
		logs := queryrangebase.HandlerFunc(h.logQuery)
		return queryrange.NewDetectedFieldsHandler(logs, logs, h.a.Limits).Do(ctx, r)
	case *queryrange.LokiRequest:
		return h.logQuery(ctx, r)
```

and

```go
// logQuery runs a log/range query on lokihouse's engine and returns Loki's
// queryrange response, as the querier does for the frontend.
func (h lokiHandler) logQuery(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	params, err := queryrange.ParamsFromRequest(req)
	if err != nil {
		return nil, err
	}
	res, err := h.a.Queries.Query(ctx, params).Exec(ctx)
	if err != nil {
		return nil, err
	}
	return queryrange.ResultToResponse(res, params)
}
```

In `lokiRoutes`, apply `h = queryrange.NewDetectedLabelsCardinalityFilter(h)` before the volume middleware, and register:

```go
		mux.Handle(m+" /loki/api/v1/detected_labels", codec)
		mux.Handle(m+" /loki/api/v1/detected_fields", codec)
		// loghttp reads {name} with gorilla mux.Vars.
		mux.Handle(m+" /loki/api/v1/detected_field/{name}/values", a.tenant(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			plain.ServeHTTP(w, mux.SetURLVars(r, map[string]string{"name": r.PathValue("name")}))
		})))
```

Here `plain := queryrange.NewSerializeHTTPHandler(h, queryrange.DefaultCodec)` is the codec handler without the tenant wrapper. Name the gorilla import `gmux` to avoid shadowing the `mux` variable. `*validation.Overrides` satisfies `queryrange.Limits` (verified at plan time).

- [ ] **Step 5: API test.** Push `level=info msg="a" status=200` and `level=error msg="b" status=500` into `{app="x"}`. Then:
  - `GET /loki/api/v1/detected_fields?query={app="x"}&start&end` returns `fields` containing `level`, `msg` and `status`, each with `parsers: ["logfmt"]` and `status` typed `int`;
  - `GET /loki/api/v1/detected_field/level/values?query={app="x"}&start&end` returns `values` = `["error","info"]` in either order;
  - `GET /loki/api/v1/detected_labels?query={app="x"}&start&end` returns 200 with `detectedLabels` holding no `sketch` keys.

- [ ] **Step 6: Run and verify it passes.** Run: `go test -count=1 ./internal/chstore/ ./internal/api/`.

- [ ] **Step 7: Oracle cases.** Append:

```go
	{name: "detected_labels_all", path: "/loki/api/v1/detected_labels", params: url.Values{"query": {`{app=~".+"}`}}, mut: sortArrayBy("detectedLabels", "label")},
	{name: "detected_labels_api", path: "/loki/api/v1/detected_labels", params: url.Values{"query": {`{app="api"}`}}, mut: sortArrayBy("detectedLabels", "label")},
	{name: "detected_fields_api_json", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="api"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_fields_web_logfmt", path: "/loki/api/v1/detected_fields", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortArrayBy("fields", "label")},
	{name: "detected_field_values_level", path: "/loki/api/v1/detected_field/level/values", params: url.Values{"query": {`{app="web"}`}, "line_limit": {"1000"}}, mut: sortStrings("values")},
```

with

```go
// sortStrings sorts a top-level array of strings.
func sortStrings(field string) func(any) {
	return func(v any) {
		for _, m := range objects(v) {
			arr, _ := m[field].([]any)
			sort.Slice(arr, func(i, j int) bool { return fmt.Sprint(arr[i]) < fmt.Sprint(arr[j]) })
		}
	}
}
```

Run `make oracle`. Expected: PASS. If `parsers` arrays differ only in order, add a mutator that sorts each field's `parsers`, and record it in `docs/divergences.md` only if the order is not deterministic in Loki.

- [ ] **Step 8: Full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal test/oracle docs/divergences.md
git commit -m "feat: detected_labels, detected_fields and detected_field values via Loki's frontend handlers"
```

---

### Task 5: `patterns`

Loki's pattern ingester trains one Drain instance per (stream, `detected_level`) in push order and answers queries from the clusters. Lokihouse rebuilds the same state at query time:
1. Read the matching rows in timestamp order.
2. Per stream, detect the format from the stream's first line.
3. Train a Drain per level, configured exactly as `pkg/pattern/ingester.go` lines 242–246 configure it.
4. Iterate the clusters as `stream.Iterator` does, merge as `IngesterQuerier.Patterns` does, and prune with the copied `prunePatterns`.

**Files:**
- Create: `internal/patterns/patterns.go`, `internal/patterns/patterns_test.go`
- Modify: `internal/api/lokicodec.go`, `internal/api/api.go`, `cmd/lokihouse/main.go`
- Modify: `test/oracle/loki.go`, `test/oracle/phase3_test.go`

**Interfaces:**
- Consumes: `(*chstore.Querier).SelectLogs`; from Loki: `drain.New`, `drain.DetectLogFormat`, `drain.TimeResolution`, `(*drain.Drain).Train`, `(*drain.Drain).Clusters`, `(*drain.LogCluster).Iterator` (`pkg/pattern/drain`); `iter.NewMerge`, `iter.ReadBatch` (`pkg/pattern/iter`); `pattern.Config` + `RegisterFlags` (`pkg/pattern`); `querierpattern.MergePatternResponses` (`pkg/querier/pattern`); `constants.LevelLabel`, `constants.LogLevels`, `constants.LogLevelUnknown` (`pkg/util/constants`); `util.RoundToMilliseconds` (`pkg/util`).
- Produces: `type Querier struct { Q *chstore.Querier; Limits drain.Limits; MaxLines int }`, `func (p *Querier) Query(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error)`; new `API` field `Patterns *patterns.Querier`.

- [ ] **Step 1: Write the failing test** `internal/patterns/patterns_test.go`. Seed `{app="w"}` with 60 lines over 10 minutes:
  - 40 lines `level=info msg="user <n> logged in"` with structured metadata `detected_level=info`;
  - 20 lines `level=error msg="db timeout after <n>ms"` with `detected_level=error`.

  Query `{app="w"}` with step 1m. Assert:
  - two series, `level=info` with total 40 and `level=error` with total 20 (sum of sample values);
  - each pattern contains `<_>`.

  Then seed a third pattern with 5 lines and assert it is pruned (total < 30, `minClusterSize`).

- [ ] **Step 2: Run and verify it fails.**

- [ ] **Step 3: Implement `internal/patterns/patterns.go`.**

```go
// Package patterns serves /loki/api/v1/patterns by running Loki's Drain over the
// stored rows at query time, configured and iterated like Loki's pattern ingester.
package patterns

// Querier answers pattern queries. MaxLines caps the rows read per query
// (default 1_000_000); rows past it are not trained (docs/divergences.md).
type Querier struct {
	Q        *chstore.Querier
	Limits   drain.Limits
	MaxLines int
}

func drainConfig() *drain.Config {
	var pc pattern.Config
	pc.RegisterFlags(flag.NewFlagSet("pattern", flag.PanicOnError))
	cfg := drain.DefaultConfig() // as pkg/pattern/ingester.go:242-246
	cfg.MaxClusters = pc.MaxClusters
	cfg.MaxEvictionRatio = pc.MaxEvictionRatio
	cfg.MaxChunkAge = pc.MaxChunkAge
	cfg.SampleInterval = pc.PatternSampleInterval
	return cfg
}

type streamDrains struct {
	format string
	byLvl  map[string]*drain.Drain
}

func (p *Querier) Query(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	if _, err := syntax.ParseMatchers(req.Query, true); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	expr, err := syntax.ParseLogSelector(req.Query, true)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	it, err := p.Q.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: req.Query, Start: req.Start, End: req.End, Direction: logproto.FORWARD,
		Plan: &plan.QueryPlan{AST: expr},
	}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	cfg := drainConfig()
	maxLines := p.MaxLines
	if maxLines == 0 {
		maxLines = 1_000_000
	}
	streams := map[string]*streamDrains{}
	var order []string
	for n := 0; n < maxLines && it.Next(); n++ {
		e := it.At()
		sd := streams[it.Labels()]
		if sd == nil {
			sd = &streamDrains{format: drain.DetectLogFormat(e.Line), byLvl: map[string]*drain.Drain{}}
			streams[it.Labels()] = sd
			order = append(order, it.Labels())
		}
		lvl := constants.LogLevelUnknown
		for _, md := range e.StructuredMetadata {
			if md.Name == constants.LevelLabel {
				lvl = strings.ToLower(md.Value)
			}
		}
		if !slices.Contains(constants.LogLevels, lvl) {
			lvl = constants.LogLevelUnknown
		}
		d := sd.byLvl[lvl]
		if d == nil {
			d = drain.New(tenantID, cfg, p.Limits, sd.format, nil)
			sd.byLvl[lvl] = d
		}
		d.Train(e.Line, e.Timestamp.UnixNano())
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	from, through := util.RoundToMilliseconds(req.Start, req.End)
	step := model.Time(req.Step)
	if step < drain.TimeResolution {
		step = drain.TimeResolution
	}
	var iters []iter.Iterator
	for _, key := range order {
		for lvl, d := range streams[key].byLvl {
			for _, c := range d.Clusters() {
				if c.String() == "" {
					continue
				}
				iters = append(iters, c.Iterator(lvl, from, through, step, model.Time(cfg.SampleInterval.Milliseconds())))
			}
		}
	}
	resp, err := iter.ReadBatch(iter.NewMerge(iters...), math.MaxInt32)
	if err != nil {
		return nil, err
	}
	return querierpattern.MergePatternResponses([]*logproto.QueryPatternsResponse{prunePatterns(resp)}), nil
}
```

Copy `prunePatterns` from `pkg/pattern/ingester_querier.go` with the attribution header. Drop its metrics parameter and keep `minClusterSize = 30` and `maxPatterns = 300`.

Before relying on it, verify the level key in `pkg/pattern/stream.go` `Push`: Loki reads `constants.LevelLabel` from the entry's structured metadata. Lokihouse stores `detected_level` as structured metadata (Phase 1 discovery), and the entry iterator returns it in `e.StructuredMetadata`. If `SelectLogs` with an empty pipeline returns no structured metadata, use the categorized-labels context flag as `detected_fields` does.

- [ ] **Step 4: Wire it.** In `Do`:

```go
	case *logproto.QueryPatternsRequest:
		res, err := h.a.Patterns.Query(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.QueryPatternsResponse{Response: res}, nil
```

Register `GET`/`POST /loki/api/v1/patterns` on `codec`. Add `Patterns *patterns.Querier` to `API`. In `main.go` set `Patterns: &patterns.Querier{Q: q, Limits: limits}`.

- [ ] **Step 5: Run and verify it passes.** Run: `go test -count=1 ./internal/patterns/ ./internal/api/`.

- [ ] **Step 6: Oracle.** In `lokiConfigTemplate` add:

```yaml
pattern_ingester:
  enabled: true
```

Start the oracle once. If Loki fails to start, read `loki.log`: the pattern ingester ring may need `pattern_ingester.lifecycler.ring.kvstore.store: inmemory`.

Add a wait helper that polls Loki's `/loki/api/v1/patterns?query={app="web"}` for tenant-a until `data` is non-empty, for at most 60s, and call it before `phase3Endpoints`. Append:

```go
	{name: "patterns_web", path: "/loki/api/v1/patterns", params: url.Values{"query": {`{app="web"}`}, "step": {"60s"}}, mut: sortArrayBy("data", "pattern")},
	{name: "patterns_legacy", path: "/loki/api/v1/patterns", params: url.Values{"query": {`{app="legacy"}`}, "step": {"300s"}}, mut: sortArrayBy("data", "pattern")},
```

The goal is equality. If patterns differ, find the cause (training order, format detection, sample interval). Fix it if lokihouse can match. If the difference is inherent (e.g. Loki trains duplicates or out-of-order lines in push order), compare only the sorted pattern strings for that case, and write the reason into `docs/divergences.md`. Also record `MaxLines` there.

- [ ] **Step 7: Full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal cmd test/oracle docs/divergences.md go.mod go.sum
git commit -m "feat: /loki/api/v1/patterns via query-time Drain configured like Loki's pattern ingester"
```

---

### Task 6: Delete API and query-time filtering

Requests are stored in ClickHouse, so every lokihouse instance sees them. The HTTP handler is Loki's `pkg/compactor/deletion/request_handler.go`, copied: its store interface has an unexported method and cannot be implemented from outside the package. Until a request is processed (Task 7), `SelectLogs`/`SelectSamples` hide matching lines with Loki's own `util/deletion.SetupPipeline`/`SetupExtractor`, and Tier A falls back to Tier B for overlapping leaves.

**Files:**
- Modify: `internal/chstore/schema.sql` (`delete_requests`)
- Create: `internal/deletes/store.go`, `internal/deletes/handler.go`, `internal/deletes/store_test.go`, `internal/deletes/handler_test.go`
- Modify: `internal/chstore/querier.go` (field `Deletes`, filtering), `internal/chstore/querier_test.go`
- Modify: `internal/pushdown/tiera.go` (field `Deletes`), `internal/pushdown/tiera_test.go`
- Modify: `internal/config/config.go` (`compactor:` block), `internal/api/api.go`, `cmd/lokihouse/main.go`
- Modify: `test/oracle/loki.go`, `test/oracle/phase3_test.go`, `test/oracle/oracle_test.go`

**Interfaces:**
- Consumes: `deletionproto.DeleteRequest`, `deletionproto.StatusReceived`/`StatusProcessed` (`pkg/compactor/deletion/deletionproto`); `deletion.TimeRange`, `deletion.ErrDeleteRequestNotFound` (`pkg/compactor/deletion`); `deletionmode.ParseMode` (`pkg/compactor/deletionmode`); `querierdeletion.DeleteGetter`, `querierdeletion.DeletesForUserQuery` (`pkg/querier/deletion`); `utildeletion.SetupPipeline`, `utildeletion.SetupExtractor` (`pkg/util/deletion`).
- Produces (used by Tasks 7, 8):
  - `type Store struct{ Conn driver.Conn; Now func() time.Time }`
  - `(*Store) Add(ctx, tenant, query string, start, end model.Time) (string, error)`
  - `(*Store) GetAllDeleteRequestsForUser(ctx, userID string, forQuerytimeFiltering bool, tr *deletion.TimeRange) ([]deletionproto.DeleteRequest, error)` (satisfies `querierdeletion.DeleteGetter`)
  - `(*Store) Get(ctx, tenant, id string) (deletionproto.DeleteRequest, error)`
  - `(*Store) Cancel(ctx, tenant, id string) error`
  - `(*Store) MarkProcessed(ctx context.Context, r deletionproto.DeleteRequest) error`
  - `(*Store) Unprocessed(ctx context.Context) ([]deletionproto.DeleteRequest, error)`
  - `type Handler struct{ Store *Store; Limits *validation.Overrides; MaxInterval, CancelPeriod time.Duration }` with methods `Add`, `List`, `Cancel` (`http.HandlerFunc` signatures) and `Gate(http.Handler) http.Handler`
  - `chstore.Querier.Deletes querierdeletion.DeleteGetter`; `pushdown.TierA.Deletes querierdeletion.DeleteGetter`
  - `config.Config.Compactor{CompactionInterval, DeleteRequestCancelPeriod, DeleteMaxInterval time.Duration}`

- [ ] **Step 1: Schema.** Add to `schema.sql` before `{{if .Cluster}}`:

```sql
-- delete_requests: Loki delete API requests. Rows are never updated in place: a status
-- change inserts a newer version and readers take argMax(…, version).
CREATE TABLE IF NOT EXISTS {{.DBName}}.{{.Local "delete_requests"}}{{with .OnCluster}} {{.}}{{end}} (
  tenant     LowCardinality(String),
  request_id String,
  query      String,
  start_ms   Int64,
  end_ms     Int64,
  created_ms Int64,
  status     LowCardinality(String), -- received | processed | cancelled
  version    UInt64
) ENGINE = {{.Engine "ReplacingMergeTree"}}
ORDER BY (tenant, request_id);
```

Inside `{{if .Cluster}}`:

```sql
CREATE TABLE IF NOT EXISTS {{.DBName}}.delete_requests{{with .OnCluster}} {{.}}{{end}} AS {{.DBName}}.delete_requests_local
ENGINE = Distributed({{lit .Cluster}}, {{lit .DB}}, 'delete_requests_local', cityHash64(tenant, request_id));
```

- [ ] **Step 2: Write failing store tests** `internal/deletes/store_test.go`:
  - `Add` then `GetAllDeleteRequestsForUser(…, true, nil)` returns the request with status `received`, matching start/end/query, `CreatedAt` = `Now()`.
  - `Cancel` hides it from both the list and `Get` (`Get` returns `deletion.ErrDeleteRequestNotFound`).
  - `MarkProcessed` keeps it in the `false` list with status `processed`, removes it from the `true` (query-time) list, and removes it from `Unprocessed`.
  - A `TimeRange` filter keeps only requests overlapping it.
  - **Review Focus 5 (`TestDeletes_TenantIsolation`):** a request added for tenant `a` is not returned for `b`, and `Get`/`Cancel` with tenant `b` returns `ErrDeleteRequestNotFound`.

- [ ] **Step 3: Implement `internal/deletes/store.go`.**

```go
// Store keeps delete requests in ClickHouse; it implements Loki's
// querier/deletion.DeleteGetter for query-time filtering.
type Store struct {
	Conn driver.Conn
	Now  func() time.Time // default time.Now
}

const statusCancelled = "cancelled"

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Store) write(ctx context.Context, r deletionproto.DeleteRequest, status string) error {
	return s.Conn.Exec(ctx,
		"INSERT INTO delete_requests (tenant, request_id, query, start_ms, end_ms, created_ms, status, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		r.UserID, r.RequestID, r.Query, int64(r.StartTime), int64(r.EndTime), int64(r.CreatedAt), status, uint64(s.now().UnixNano()))
}

func (s *Store) Add(ctx context.Context, tenant, query string, start, end model.Time) (string, error) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	r := deletionproto.DeleteRequest{RequestID: hex.EncodeToString(b), UserID: tenant, Query: query,
		StartTime: start, EndTime: end, CreatedAt: model.TimeFromUnixNano(s.now().UnixNano())}
	return r.RequestID, s.write(ctx, r, string(deletionproto.StatusReceived))
}

// list returns the latest version of every non-cancelled request matching where.
func (s *Store) list(ctx context.Context, where string, args ...any) ([]deletionproto.DeleteRequest, error) {
	q := `SELECT tenant, request_id, argMax(query, version), argMax(start_ms, version), argMax(end_ms, version),
       argMax(created_ms, version), argMax(status, version) AS st
FROM delete_requests WHERE ` + where + ` GROUP BY tenant, request_id HAVING st != '` + statusCancelled + `'`
	rows, err := s.Conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deletionproto.DeleteRequest
	for rows.Next() {
		var r deletionproto.DeleteRequest
		var st string
		var start, end, created int64
		if err := rows.Scan(&r.UserID, &r.RequestID, &r.Query, &start, &end, &created, &st); err != nil {
			return nil, err
		}
		r.StartTime, r.EndTime, r.CreatedAt = model.Time(start), model.Time(end), model.Time(created)
		r.Status = deletionproto.DeleteRequestStatus(st)
		out = append(out, r)
	}
	return out, rows.Err()
}
```

Then implement:
- `GetAllDeleteRequestsForUser`: `list(ctx, "tenant = ?", userID)`, then filter in Go: `forQuerytimeFiltering` → status `received`; `tr != nil` → `r.StartTime <= tr.End && r.EndTime >= tr.Start`.
- `Get`: `list` with `tenant = ? AND request_id = ?`; return `deletion.ErrDeleteRequestNotFound` on empty.
- `Cancel`: `Get`, then `write(…, statusCancelled)`.
- `MarkProcessed`: `write(r, string(deletionproto.StatusProcessed))`.
- `Unprocessed`: `list(ctx, "1")`, filtered to status `received`.

Verify the `deletionproto` field names and status constants against `pkg/compactor/deletion/deletionproto` at the pinned commit; adjust the names, not the behaviour.

- [ ] **Step 4: Write failing handler tests** `internal/deletes/handler_test.go`. Go through `httptest` with a tenant-injecting wrapper:
  - `POST /loki/api/v1/delete?query={app="x"} |= "secret"&start=<unix s>&end=<unix s>` → 204 with an `X-Delete-Request-ID` header.
  - `GET` → JSON array with one object holding `request_id`, `start_time`, `end_time`, `query`, `status:"received"`, `created_at`.
  - Error cases with Loki's exact messages: missing query → 400 `query not set`; `end` in the future → 400 `deletes in the future are not allowed`; `start >= end` → 400 `start time can't be greater than or equal to end time`; `max_interval=1h` with a 30m range → 400 `max_interval can't be greater than the interval to be deleted (30m0s)`.
  - `DELETE ?request_id=<id>` → 204; again → 404 `could not find delete request with given id`.
  - Cancel after the cancel period (`CancelPeriod: time.Nanosecond`) without `force` → 400, with `force=true` → 204.
  - Tenant with `deletion_mode: disabled` → 403 with Loki's `deletionNotAvailableMsg` text (copy the constant).

- [ ] **Step 5: Implement `internal/deletes/handler.go`.** Copy from `pkg/compactor/deletion/request_handler.go`, with the attribution header:
  - `AddDeleteRequestHandler`, `interval`, `GetAllDeleteRequestsHandler`, `CancelDeleteRequestHandler`, `query`, `startTime`, `endTime`, `parseTime`, `timeFromInt`;
  - `parseDeletionQuery` from `util.go`;
  - the gate from `tenant_request_handler.go` (`TenantMiddleware` + `validDeletionLimit` + `deleteModeFromLimits`).

  Modifications to list in the header:
  - The store calls go to `*Store`.
  - `max_interval` is validated exactly as Loki does, but requests are stored unsharded. The shards only drive Loki's compactor, and the listing merges them back.
  - Drop the metrics.
  - The gate reads `Limits.DeletionMode(tenant)`.

- [ ] **Step 6: Run the deletes tests and verify they pass.** Run: `go test -count=1 ./internal/deletes/`.

- [ ] **Step 7: Write the failing query-time filtering test** (`internal/chstore/querier_test.go`):

```go
type fakeDeletes []deletionproto.DeleteRequest

func (f fakeDeletes) GetAllDeleteRequestsForUser(context.Context, string, bool, *deletion.TimeRange) ([]deletionproto.DeleteRequest, error) {
	return f, nil
}

func TestQuerier_HidesPendingDeletes(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "keep"}, {Timestamp: t0.Add(time.Second), Line: "secret 1"},
		{Timestamp: t0.Add(2 * time.Second), Line: "secret 2"}}})
	del := fakeDeletes{{Query: `{app="a"} |= "secret"`, StartTime: model.TimeFromUnixNano(t0.UnixNano()),
		EndTime: model.TimeFromUnixNano(t0.Add(time.Second).UnixNano()), Status: deletionproto.StatusReceived}}
	ng := engine(t, &chstore.Querier{Conn: conn, Deletes: del})
	res := run(t, ng, `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"keep", "secret 2"}, lines(res.Data.(logqlmodel.Streams)[0]), "only inside [start, end]")
	m := run(t, ng, `count_over_time({app="a"}[1m])`, t0.Add(time.Minute), t0.Add(time.Minute), 0, logproto.FORWARD, 0)
	require.Equal(t, 2.0, m.Data.(promql.Vector)[0].F)
}
```

- [ ] **Step 8: Implement the filtering.** In `chstore.Querier` add `Deletes querierdeletion.DeleteGetter` (nil = no deletes). In `SelectLogs`, after building `pipeline`:

```go
	if q.Deletes != nil && len(p.Deletes) == 0 {
		if p.Deletes, err = querierdeletion.DeletesForUserQuery(ctx, p.Start, p.End, q.Deletes); err != nil {
			return nil, err
		}
	}
	if pipeline, err = utildeletion.SetupPipeline(p, pipeline); err != nil {
		return nil, err
	}
```

Do the same in `SelectSamples` with `p.SampleQueryRequest.Deletes` and `extractor, err = utildeletion.SetupExtractor(p, extractor)`. Loki's tail querier already sets `Deletes` on its history request, which is why the code fetches only when the field is empty.

- [ ] **Step 9: Tier A guard.** Add `Deletes querierdeletion.DeleteGetter` to `TierA`. At the start of `Exec`, after computing `origin` and `E`:

```go
	if a.Deletes != nil {
		dels, err := querierdeletion.DeletesForUserQuery(ctx, time.Unix(0, origin), time.Unix(0, E), a.Deletes)
		if err != nil {
			return logqlmodel.Result{}, err
		}
		if len(dels) > 0 {
			return logqlmodel.Result{}, ErrFallback // Tier B applies the delete filter
		}
	}
```

Add `TestTierA_PendingDeleteFallsBack`, modelled on `TestTierA_TooManyBucketsFallsBackToTierB`: with a pending delete the result equals Tier B's filtered result, and `TierALeaves` does not grow.

- [ ] **Step 10: Config and wiring.** In `config.Config` add:

```go
	Compactor struct {
		CompactionInterval        time.Duration `yaml:"compaction_interval"`          // delete processing loop
		DeleteRequestCancelPeriod time.Duration `yaml:"delete_request_cancel_period"` // deletes run after this
		DeleteMaxInterval         time.Duration `yaml:"delete_max_interval"`
	} `yaml:"compactor"`
```

with defaults set in `Load` before decoding: `10 * time.Minute`, `24 * time.Hour`, `24 * time.Hour` (Loki's flag defaults). Add a config test that `compactor: {delete_request_cancel_period: 2h}` decodes to 2h and the other two keep their defaults.

In `api.go` add the field `Deletes *deletes.Handler` and register:

```go
	if a.Deletes != nil {
		g := func(h http.HandlerFunc) http.Handler { return t(a.Deletes.Gate(h)) }
		mux.Handle("PUT /loki/api/v1/delete", g(a.Deletes.Add))
		mux.Handle("POST /loki/api/v1/delete", g(a.Deletes.Add))
		mux.Handle("GET /loki/api/v1/delete", g(a.Deletes.List))
		mux.Handle("DELETE /loki/api/v1/delete", g(a.Deletes.Cancel))
	}
```

In `main.go`: create `store := &deletes.Store{Conn: conn}`, set `q.Deletes = store`, `TierA.Deletes = store`, and `Deletes: &deletes.Handler{Store: store, Limits: limits, MaxInterval: cfg.Compactor.DeleteMaxInterval, CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod}`.

- [ ] **Step 11: Oracle.**
  - In `lokiConfigTemplate` add a compactor section, plus `common.compactor_address` so the querier reads deletes from the local compactor. Add a `${HTTP_PORT}` replacement in `StartLoki`, next to `${DIR}`:

```yaml
compactor:
  working_directory: ${DIR}/compactor
  retention_enabled: true
  delete_request_store: filesystem
```

  - In `phase3_test.go` add `deletesOracle(t, loki, house string, base time.Time)` and call it from `TestOracle` after `phase3Endpoints`. Use the fresh tenant `tenant-del`: Loki caches a tenant's delete list for 5 minutes from its first lookup, so this tenant must not be queried before the request exists. The function:
    1. Pushes `{app="del"}` with 20 lines over 20 seconds starting at `base` to both stores (every other line contains `secret`), then waits until Loki counts 20 (as `WaitQueryable` does).
    2. Sends `POST /loki/api/v1/delete?query={app="del"} |= "secret"&start=<base unix s>&end=<now unix s>` to both and requires 204 from both.
    3. Requires that Loki's `query_range {app="del"}` over [base, base+1m] returns 10 lines (sanity check that Loki filters). Then compares that query, and `count_over_time({app="del"}[1m])` as an instant query, with `Compare`.
    4. Compares `GET /loki/api/v1/delete` with `CompareMut(…, chain(dropArrayKeys("request_id", "created_at")))`, where `dropArrayKeys` deletes those keys from every object of a top-level array.

  If step 3's sanity check fails on Loki itself, inspect `loki.log` for delete-client errors and fix the Loki config. Never weaken the comparison.

- [ ] **Step 12: Divergences.** Append to `docs/divergences.md`:
  - **Delete request IDs:** lokihouse uses 8 random hex characters; the format and the IDs differ from Loki.
  - **Stats and volume after a delete:** `index/stats`, `index/volume`, patterns and `detected_labels` ignore deletes (Loki's index stats also count unprocessed deletes; after processing, Loki's rewritten chunks shrink while lokihouse's MV does not).

- [ ] **Step 13: Full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal cmd test/oracle docs/divergences.md go.mod go.sum
git commit -m "feat: delete API with query-time filtering (Loki's handler and filter pipeline)"
```

---

### Task 7: Delete processing (physical deletion)

After `delete_request_cancel_period`, Loki's compactor rewrites the chunks. Lokihouse deletes the rows with a ClickHouse lightweight `DELETE` and then marks the request processed. The rows to delete are chosen by running each candidate row through `utildeletion.SetupPipeline` with that single request: exactly the rows query-time filtering hid (Review Focus 3).

**Files:**
- Create: `internal/chstore/deleterows.go`, `internal/chstore/deleterows_test.go`
- Create: `internal/deletes/processor.go`, `internal/deletes/processor_test.go`
- Modify: `cmd/lokihouse/main.go`

**Interfaces:**
- Consumes: `deletes.Store` (Task 6); `chstore.ResolveStreams`, `Topology`; `utildeletion.SetupPipeline`; `log.NewNoopPipeline()` (`pkg/logql/log`).
- Produces:
  - `type RowKey struct{ StreamID uint64; TS int64; EntryHash uint64 }`
  - `type Row struct{ RowKey; Line string; MD labels.Labels }`
  - `func ScanRows(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, fromNs, throughNs int64, fn func(Row) error) error` (inclusive bounds)
  - `func DeleteRows(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, keys []RowKey) error`
  - `func DeleteRange(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, ids []uint64, fromNs, throughNs int64) error`
  - `type Processor struct { Store *Store; Conn driver.Conn; DB string; Topo chstore.Topology; CancelPeriod, Interval time.Duration; Logger log.Logger }` with `ProcessDue(ctx context.Context, now time.Time) error` and `Run(ctx context.Context)`

- [ ] **Step 1: Write the failing processor test** `internal/deletes/processor_test.go`, `TestProcessor_MatchesQueryTimeFilter`:
  - Seed `{app="a"}` with lines `keep`, `secret a`, `secret b` (ts = t0, t0+1ms, t0+2ms), `secret c` at exactly `end` ms, and `secret d` at `end` + 0.5 ms (just outside Loki's inclusive end). Also seed `{app="b"}` with `secret x` (other stream). Use a second database part for `secret b` to exercise unmerged parts.
  - Add a request `{app="a"} |~ "secret [a-c]"` with `start` = t0 ms and `end` = t0+3ms ms. Record the lines a querier with `Deletes: store` returns (the query-time view).
  - Call `ProcessDue(ctx, now)` with `CancelPeriod: 0`. Assert:
    - `SELECT line FROM logs WHERE tenant='t1' ORDER BY line` equals the recorded query-time view plus `secret x` (both stores agree);
    - the request is now `processed`;
    - a second `ProcessDue` is a no-op.
  - Add `TestProcessor_SelectorOnly`: request `{app="b"}` with no filters takes the `DeleteRange` path and removes exactly the rows of `{app="b"}` in range.
  - Add `TestProcessor_RespectsCancelPeriod`: with `CancelPeriod: time.Hour` and `now` = created + 59m, nothing is deleted.

- [ ] **Step 2: Run and verify it fails.**

- [ ] **Step 3: Implement `internal/chstore/deleterows.go`.**

```go
// RowKey identifies one stored row exactly (the logs sorting key).
type RowKey struct {
	StreamID  uint64
	TS        int64
	EntryHash uint64
}

type Row struct {
	RowKey
	Line string
	MD   labels.Labels
}

// ScanRows streams every raw row (no dedup: each copy must be deleted) of the given
// streams with fromNs <= ts <= throughNs.
func ScanRows(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, fromNs, throughNs int64, fn func(Row) error) error {
	rows, err := conn.Query(ctx, `SELECT stream_id, toUnixTimestamp64Nano(ts), entry_hash, line, structured_metadata
FROM logs WHERE tenant = ? AND stream_id IN ?
  AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC')`,
		tenant, ids, fromNs, throughNs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r Row
		var md map[string]string
		if err := rows.Scan(&r.StreamID, &r.TS, &r.EntryHash, &r.Line, &md); err != nil {
			return err
		}
		r.MD = labels.FromMap(md)
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func deleteCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"lightweight_deletes_sync": 2}))
}

// DeleteRows removes the given rows with a lightweight DELETE on the storage
// tables (ON CLUSTER on a cluster). The tuple list is rendered from integers only.
func DeleteRows(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, keys []RowKey) error {
	if len(keys) == 0 {
		return nil
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d,%d,%d)", k.StreamID, k.TS, k.EntryHash)
	}
	sql := fmt.Sprintf("DELETE FROM %s.%s %s WHERE tenant = ? AND (stream_id, toUnixTimestamp64Nano(ts), entry_hash) IN (%s)",
		quoteIdent(db), topo.Local("logs"), topo.OnCluster(), b.String())
	return conn.Exec(deleteCtx(ctx), sql, tenant)
}

// DeleteRange removes every row of the given streams with fromNs <= ts <= throughNs.
func DeleteRange(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, ids []uint64, fromNs, throughNs int64) error {
	sql := fmt.Sprintf("DELETE FROM %s.%s %s WHERE tenant = ? AND stream_id IN ? AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC')",
		quoteIdent(db), topo.Local("logs"), topo.OnCluster())
	return conn.Exec(deleteCtx(ctx), sql, tenant, ids, fromNs, throughNs)
}
```

Add `deleterows_test.go` checking that both delete exactly the requested rows, on the single node and under `make test-cluster`.

- [ ] **Step 4: Implement `internal/deletes/processor.go`.**

```go
// ProcessDue physically deletes every received request older than CancelPeriod,
// then marks it processed. Query-time filtering stops once it is processed, and
// lightweight_deletes_sync=2 makes the rows invisible on every replica first.
func (p *Processor) ProcessDue(ctx context.Context, now time.Time) error {
	reqs, err := p.Store.Unprocessed(ctx)
	if err != nil {
		return err
	}
	for _, r := range reqs {
		if r.CreatedAt.Time().Add(p.CancelPeriod).After(now) {
			continue
		}
		if err := p.process(ctx, r); err != nil {
			return fmt.Errorf("delete request %s: %w", r.RequestID, err)
		}
		if err := p.Store.MarkProcessed(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

const deleteBatch = 10_000

func (p *Processor) process(ctx context.Context, r deletionproto.DeleteRequest) error {
	expr, err := syntax.ParseLogSelector(r.Query, true)
	if err != nil {
		return err
	}
	from, through := r.StartTime.UnixNano(), r.EndTime.UnixNano()
	streams, err := chstore.ResolveStreams(ctx, p.Conn, r.UserID, expr.Matchers(), time.Unix(0, from), time.Unix(0, through))
	if err != nil || len(streams) == 0 {
		return err
	}
	ids := make([]uint64, 0, len(streams))
	byID := make(map[uint64]labels.Labels, len(streams))
	for _, s := range streams {
		ids, byID[s.ID] = append(ids, s.ID), s.Labels
	}
	// A bare selector (no pipeline stage of any kind) deletes the whole range in SQL.
	if _, bare := expr.(*syntax.MatchersExpr); bare {
		return chstore.DeleteRange(ctx, p.Conn, p.DB, p.Topo, r.UserID, ids, from, through)
	}
	// Rows the query-time filter hides are exactly the rows to delete.
	filter, err := utildeletion.SetupPipeline(logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Deletes: []*logproto.Delete{{Selector: r.Query, Start: from, End: through}},
	}}, log.NewNoopPipeline())
	if err != nil {
		return err
	}
	sp := map[uint64]log.StreamPipeline{}
	var keys []chstore.RowKey
	flush := func() error {
		err := chstore.DeleteRows(ctx, p.Conn, p.DB, p.Topo, r.UserID, keys)
		keys = keys[:0]
		return err
	}
	err = chstore.ScanRows(ctx, p.Conn, r.UserID, ids, from, through, func(row chstore.Row) error {
		s := sp[row.StreamID]
		if s == nil {
			s = filter.ForStream(byID[row.StreamID])
			sp[row.StreamID] = s
		}
		if _, _, keep := s.ProcessString(row.TS, row.Line, row.MD); keep {
			return nil
		}
		keys = append(keys, row.RowKey)
		if len(keys) == deleteBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// Run calls ProcessDue every Interval until ctx ends; errors are logged.
func (p *Processor) Run(ctx context.Context) {
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		if err := p.ProcessDue(ctx, time.Now()); err != nil {
			level.Error(p.Logger).Log("msg", "processing delete requests", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
```

Only a bare `*syntax.MatchersExpr` takes the `DeleteRange` path. `HasFilter()` would miss parser and label-filter stages, so a selector like `{app="a"} | logfmt | level="debug"` must take the scan path. Add a test for that case.

- [ ] **Step 5: Wire it in `main.go`.** Start it with `go (&deletes.Processor{Store: store, Conn: conn, DB: cfg.ClickHouse.Database, Topo: cfg.Topology(), CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod, Interval: cfg.Compactor.CompactionInterval, Logger: logger}).Run(ctx)`.

- [ ] **Step 6: Run and verify it passes.** Run `go test -count=1 ./internal/deletes/ ./internal/chstore/`, then `make test-cluster`. On the cluster the `DELETE … ON CLUSTER` must succeed against the `_local` tables.

- [ ] **Step 7: Commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal cmd
git commit -m "feat(deletes): physical deletion after the cancel period, exact to the query-time filter"
```

---

### Task 8: `tail` over WebSocket

Loki's `tail.Querier` asks an `Ingester` interface for live tail clients and the store for history. It then merges, orders and delays them in its `Tailer` and serves the WebSocket in `TailHandler`. Lokihouse reuses all of that and implements `tail.Ingester` with one in-process "ingester". Its `Querier_TailClient` polls ClickHouse every second for rows with `ts` newer than the previous poll, minus a 10 s lookback for late arrivals, deduplicated by a seen-set.

**Files:**
- Create: `internal/tail/ingester.go`, `internal/tail/ingester_test.go`
- Modify: `internal/api/api.go`, `cmd/lokihouse/main.go`
- Modify: `test/oracle/phase3_test.go`, `test/oracle/oracle_test.go`

**Interfaces:**
- Consumes: `tail.NewQuerier(ingester tail.Ingester, store tail.Store, deleteGetter querierdeletion.DeleteGetter, limits querier_limits.Limits, tailMaxDuration time.Duration, metrics *tail.Metrics, logger log.Logger) *tail.Querier`, `(*tail.Querier).TailHandler`, `tail.NewMetrics(prometheus.Registerer)` (verify the constructor name in `pkg/querier/tail/metrics.go`); `(*chstore.Querier).SelectLogs`; `deletes.Store` (Task 6).
- Produces:
  - `type Ingester struct { Q *chstore.Querier; Poll, Lookback time.Duration }` (defaults 1s, 10s) implementing `tail.Ingester`
  - new `API` field `Tail http.Handler`

- [ ] **Step 1: Write the failing tests** `internal/tail/ingester_test.go`. Call `Ingester.Tail(ctx, req)` directly with `req` = `{Query: {app="t"}, Start: now}` and `Poll: 50ms`, `Lookback: 2s`, then `Recv` from the returned client:
  - `TestTail_DeliversNewEntries`: after `Tail`, write 3 entries with ts = now+1ms…+3ms; `Recv` returns them (possibly across several responses) and the labels match `{app="t", service_name="t"}`.
  - `TestTail_LateEntryWithinLookback` (Review Focus 2): write entry A (ts = now+1s) and receive it. Then write B with ts = now+500ms (older than A, inside the lookback); B is received once. Over the next 5 polls, nothing is received twice: `Recv` with a 300 ms context deadline returns `context.DeadlineExceeded`.
  - `TestTail_NoHistoryDuplicates`: an entry with ts < `req.Start` written before `Tail` is never sent by the live client (history is the `Tailer`'s job).
  - `TestTail_CloseStopsPolling` (Review Focus 1): after cancelling the ctx passed to `Tail`, `Recv` returns an error, `TailersCount` returns `[0]`, and no ClickHouse queries run afterwards. Count them with a `driver.Conn` wrapper that increments on `Query`.

- [ ] **Step 2: Run and verify they fail.**

- [ ] **Step 3: Implement `internal/tail/ingester.go`.**

```go
// Package tail adapts ClickHouse to Loki's tail.Querier: one in-process
// "ingester" whose tail client polls for new rows.
package tail

const addr = "clickhouse"

type Ingester struct {
	Q        *chstore.Querier
	Poll     time.Duration // default 1s
	Lookback time.Duration // default 10s
	active   atomic.Int64
}

func (i *Ingester) Tail(ctx context.Context, req *logproto.TailRequest) (map[string]logproto.Querier_TailClient, error) {
	poll, look := i.Poll, i.Lookback
	if poll == 0 {
		poll = time.Second
	}
	if look == 0 {
		look = 10 * time.Second
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &client{ctx: ctx, cancel: cancel, i: i, req: req, poll: poll, look: look,
		start: req.Start, seen: map[seenKey]int64{}}
	i.active.Add(1)
	context.AfterFunc(ctx, func() { i.active.Add(-1) })
	return map[string]logproto.Querier_TailClient{addr: c}, nil
}

func (i *Ingester) TailDisconnectedIngesters(ctx context.Context, req *logproto.TailRequest, connected []string) (map[string]logproto.Querier_TailClient, error) {
	if slices.Contains(connected, addr) {
		return map[string]logproto.Querier_TailClient{}, nil
	}
	return i.Tail(ctx, req)
}

func (i *Ingester) TailersCount(context.Context) ([]uint32, error) {
	return []uint32{uint32(i.active.Load())}, nil
}

type seenKey struct {
	labels string
	ts     int64
	line   string
}

// client is a logproto.Querier_TailClient. Each poll reads rows with
// ts > max(start, pollTime-lookback), skips those already sent, and forgets
// sent rows once they are older than the window.
type client struct {
	ctx        context.Context
	cancel     context.CancelFunc
	i          *Ingester
	req        *logproto.TailRequest
	poll, look time.Duration
	start      time.Time
	seen       map[seenKey]int64
	pending    []*logproto.TailResponse
}

func (c *client) Recv() (*logproto.TailResponse, error) {
	t := time.NewTicker(c.poll)
	defer t.Stop()
	for len(c.pending) == 0 {
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-t.C:
		}
		if err := c.pollOnce(time.Now()); err != nil {
			return nil, err
		}
	}
	r := c.pending[0]
	c.pending = c.pending[1:]
	return r, nil
}

func (c *client) pollOnce(now time.Time) error {
	lower := now.Add(-c.look)
	if lower.Before(c.start) {
		lower = c.start
	}
	it, err := c.i.Q.SelectLogs(c.ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: c.req.Query, Start: lower.Add(time.Nanosecond), End: now.Add(24 * time.Hour),
		Direction: logproto.FORWARD, Plan: c.req.Plan,
	}})
	if err != nil {
		return err
	}
	defer it.Close()
	byLabels := map[string]*logproto.Stream{}
	var order []string
	for it.Next() {
		e := it.At()
		k := seenKey{it.Labels(), e.Timestamp.UnixNano(), e.Line}
		if _, ok := c.seen[k]; ok {
			continue
		}
		c.seen[k] = k.ts
		s := byLabels[k.labels]
		if s == nil {
			s = &logproto.Stream{Labels: k.labels}
			byLabels[k.labels] = s
			order = append(order, k.labels)
		}
		s.Entries = append(s.Entries, e)
	}
	if err := it.Err(); err != nil {
		return err
	}
	for k, ts := range c.seen {
		if ts <= lower.UnixNano() {
			delete(c.seen, k)
		}
	}
	for _, l := range order {
		c.pending = append(c.pending, &logproto.TailResponse{Stream: byLabels[l]})
	}
	return nil
}

func (c *client) Header() (metadata.MD, error) { return nil, nil }
func (c *client) Trailer() metadata.MD         { return nil }
func (c *client) CloseSend() error             { c.cancel(); return nil }
func (c *client) Context() context.Context     { return c.ctx }
func (c *client) SendMsg(any) error            { return nil }
func (c *client) RecvMsg(any) error            { return nil }
```

(`metadata` is `google.golang.org/grpc/metadata`.) The seen-set prune must not drop a key that the next window can still return: the next window's lower bound is ≥ this one's, so keys with `ts <= lower` can no longer be read. Keep that comparison as `<=`.

- [ ] **Step 4: Wire it.** In `main.go`:

```go
	tq := tail.NewQuerier(&lhtail.Ingester{Q: q}, q, store, limits, time.Hour, tail.NewMetrics(nil), logger)
```

`time.Hour` is Loki's `-querier.tail-max-duration` default. Set `Tail: http.HandlerFunc(tq.TailHandler)` on `API`. In `Router`:

```go
	if a.Tail != nil {
		for _, m := range []string{"GET", "POST"} {
			mux.Handle(m+" /loki/api/v1/tail", t(a.Tail))
			mux.Handle(m+" /api/prom/tail", t(a.Tail))
		}
	}
```

If `*validation.Overrides` does not satisfy `querier_limits.Limits` at compile time, wrap it in the smallest adapter that does and note why in a comment.

- [ ] **Step 5: Run and verify it passes.** Run: `go test -count=1 ./internal/tail/ ./internal/api/`.

- [ ] **Step 6: Oracle.** Add `tailOracle(t, loki, house string)` and call it last in `TestOracle`, because it pushes new data. For tenant `tenant-tail`, it:
  1. Dials both `ws://…/loki/api/v1/tail?query={app="tail"}&start=<now ns>&limit=100` with `github.com/gorilla/websocket` and the `X-Scope-OrgID` header.
  2. Pushes 20 lines (`tail line <i>`, ts = now + i ms) to both.
  3. Reads from each socket until 20 entries have arrived or 20 s pass.
  4. Compares the sorted lists of `(stream labels, ts, line)` and requires 20 from each.

  Decode with `loghttp.TailResponse` (`pkg/loghttp`) so both sides are parsed by Loki's types.

- [ ] **Step 7: Divergences.** Append:
  - **Tail delivery:** Loki streams entries as ingesters receive them. lokihouse polls every second and catches entries whose timestamp is up to 10 s older than the poll. An entry pushed later with an older timestamp, or with a timestamp before the tail started, is not sent live.

- [ ] **Step 8: Full gate and commit.** Run `make test`, `make test-cluster`, `make oracle` and `make oracle-cluster`.

```bash
git add internal cmd test/oracle docs/divergences.md go.mod go.sum
git commit -m "feat: /loki/api/v1/tail via Loki's tail querier over a polling ClickHouse ingester"
```

---

### Task 9: `format_query`, Phase 3 status and verification

**Files:**
- Create: `internal/api/format_query.go`, `internal/api/format_query_test.go`
- Modify: `internal/api/api.go`, `test/oracle/phase3_test.go`, `test/oracle/oracle_test.go`
- Create: `docs/phase3-status.md`

**Interfaces:**
- Consumes: `syntax.ParseExpr`, `syntax.Prettify`.
- Produces: route `GET`/`POST /loki/api/v1/format_query`. It is not tenant-wrapped: Loki registers it on the server without the auth middleware.

- [ ] **Step 1: Write the failing test.**
  - `GET /loki/api/v1/format_query?query=sum(rate({a="b"}[1m]))` without `X-Scope-OrgID` on an `auth_enabled: true` server returns 200 with `{"status":"success","data":"<syntax.Prettify output>"}`. Compute the expected `data` with `syntax.Prettify` in the test.
  - An invalid query returns 400 with `"status":"invalid-query"` and the parser error in `"error"`.

- [ ] **Step 2: Run and verify it fails.**

- [ ] **Step 3: Implement.** Copy `pkg/loki/format_query_handler.go` (the handler and `FormatQueryResponse`) into `internal/api/format_query.go` with the attribution header. Register it in `Router` for `GET` and `POST` without `t(...)`.

- [ ] **Step 4: Oracle.** In `TestOracle`, add a loop over every corpus query (reusing the corpus scan). It calls `/loki/api/v1/format_query?query=<q>` on both and runs `Compare`. Loki answers this route without tenant auth, and `Get` sends the header anyway, which is harmless.

- [ ] **Step 5: Write `docs/phase3-status.md`.** Include:
  - **Exit criteria table:** `make test`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`, each with the run result.
  - **Endpoint table:** endpoint → implementation (Loki code reused / copied / new) → oracle coverage.
  - **Operations:** the delete processing loop (`compactor.compaction_interval`, `delete_request_cancel_period`); the tail poll interval and lookback; the patterns `MaxLines` cap.
  - **The manual `stream_stats_hourly` backfill** for databases created before Phase 3. On a single server:

    ```sql
    INSERT INTO <db>.stream_stats_hourly
    SELECT tenant, stream_id, toStartOfHour(toDateTime(ts, 'UTC')) AS hour, retention_days,
           count(), sum(length(line) + 8 * length(mapKeys(structured_metadata))), min(ts), max(ts)
    FROM <db>.logs WHERE ts < <migration time> GROUP BY tenant, stream_id, hour, retention_days
    ```

    On a cluster, the same statement runs on one replica of every shard against the `_local` tables.
  - **Known issues:** link to `docs/divergences.md`.

- [ ] **Step 6: Final verification.** Run `make test`, `make test-cluster`, `make oracle-all` and `make oracle-cluster`. All must pass. Record the results in the status doc.

- [ ] **Step 7: Commit.**

```bash
git add internal test/oracle docs
git commit -m "feat: format_query; docs: phase 3 status"
```
