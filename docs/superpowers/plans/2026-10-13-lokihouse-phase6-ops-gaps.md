# Lokihouse Phase 6 (Operational Gaps) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close four gaps: one wide push can fail itself and every push sharing its async flush on ClickHouse's partition limit, the ClickHouse client runs on implicit defaults, deleted rows never leave the disk, and multi-tenant queries do not cover the index and drilldown endpoints.

**Architecture:** The writer rejects a push touching more than 60 `(day, retention)` partitions with 400, and inserts pushes over 8 partitions synchronously so they never share the async-insert buffer. Client timeouts and pool size become `clickhouse.*` config keys built into `clickhouse.Options` by the config package, and `/ready` pings ClickHouse. The delete processor runs `ALTER TABLE … APPLY DELETED MASK` after a pass that deleted rows. A new `multitenant.Index` fans the index/stats, volume, detected_labels and patterns endpoints out per tenant with Loki's merge functions. The codec handler calls it through a per-tenant adapter that keeps Loki's per-tenant checks.

**Tech Stack:** Go, clickhouse-go v2.49.0, ClickHouse 26.9, Loki at the pinned commit (`pkg/storage/stores/index/stats`, `pkg/storage/stores/index/seriesvolume`, `pkg/storage/detected`, `pkg/querier/pattern`).

**Spec:** this plan carries its own spec in the Background section below. Its evidence comes from the code at `5ecc3ad` and from probes run on ClickHouse 26.9.14 on 2026-10-13.

## Background (the findings this plan acts on)

1. **Inserts and partitions.** Every push is one `INSERT` per table with `async_insert=1, wait_for_async_insert=1` (`internal/chstore/writer.go`). That follows ClickHouse's advice for many small inserts, and no client-side chunking is needed for it. The problem is partitions. `logs` is `PARTITION BY (toDate(ts), retention_days)`, and ClickHouse rejects an insert block that touches more than `max_partitions_per_insert_block` (100) partitions. It throws `Too many partitions for single INSERT block`. A probe confirmed two things:
   - a single push spanning 150 days fails. Old data is only accepted when `reject_old_samples` is off (a backfill), and a push then fails with 500 on every retry;
   - **two concurrent pushes of 60 partitions each both fail**, because the async-insert buffer merges them into one flush block (`system.asynchronous_insert_log` status `FlushError`). This needs no backfill: a single wide push makes every push that shares its flush fail, *for all tenants*.
   With `throw_on_max_partitions_per_insert_block=0` on the insert, both cases succeed (ClickHouse logs a warning instead). **Decision (2026-10-13, user):** very wide pushes are unsupported rather than forced through. A push over 60 partitions is rejected with 400. Pushes over 8 partitions bypass the shared async buffer, so they cannot fail other pushes. The setting stays only on the async path, as a safety net for merged flushes of individually narrow pushes (see Task 1).
2. **ClickHouse unavailable.**
   - At startup, `Migrate` fails and the process exits non-zero. systemd restarts it after 5 s (`Restart=on-failure`), and Kubernetes would crash-loop. This is acceptable and is documented in Task 7.
   - At runtime, a push returns 500 with the driver error. Loki clients (Promtail, Alloy) retry 5xx with backoff, the same as when Loki's ingesters are down. There is no local buffer, so data is lost only when the client gives up. Queries return 500, and the delete processor logs and retries on its next interval.
   - The client has no explicit timeouts. AGENTS.md requires them, but clickhouse-go's defaults apply: 30 s dial and pool-acquire timeout, 5 min read timeout, and **10 open connections**. Each push holds a connection while it waits for the async flush (about 200 ms by default), so one instance tops out at about 50 pushes/s, and pushes, queries, tails and deletes all share those 10 connections.
   - `/ready` always answers 200, even when ClickHouse is unreachable.
3. **Deletes.** Lightweight `DELETE` only masks rows (`_row_exists`). They stay on disk until a merge rewrites their part, and an old day partition gets no more inserts, so it is never merged. With `retention_days = 0` (Loki's default), deleted data stays on disk forever. Loki's compactor rewrites the chunks, so the data is physically gone. For delete requests made for compliance, this matters.
   - `OPTIMIZE TABLE … FINAL` would rewrite whole day partitions (all tenants), which is far too heavy.
   - `ALTER TABLE … APPLY DELETED MASK` rewrites only the parts that carry a mask. A probe showed the masked part shrinking from 10000 to 5000 rows while the untouched partitions kept their rows. This is the right tool.
4. **Multi-tenant queries.** Loki's `MultiTenantQuerier` also fans out `IndexStats`, `Volume`, `DetectedLabels` and `Patterns`, using merge helpers that are already in Loki's module graph (`stats.MergeStats`, `seriesvolume.Merge`, `detected.MergeLabels`, `pattern.MergePatternResponses`). Copying them is small and closes the divergence in `docs/divergences.md`. Grafana Logs Drilldown uses all four, so a multi-tenant data source breaks there today. Two things stay out of scope:
   - SQL push-down for multi-tenant queries stays out, because it is single-tenant by construction.
   - `detected_metric_names` stays out, because lokihouse does not serve it at all.

## Global Constraints

- `make check` passes after every task (gofmt/goimports, vet, golangci-lint, govulncheck, `-race` tests, coverage ≥ 85 %).
- Tasks 1, 4, 5 and 6 touch ingest/query/SQL code, so before the phase ends, `make test-cluster`, `make oracle-all` and `make oracle-cluster` also pass.
- ClickHouse for tests: `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`.
- `LOKI_COMMIT` is not bumped.
- Code copied from Loki starts with `// Copied from grafana/loki@bc196286 <path>; modifications: <list>`.
- No new third-party module (every import above is already in Loki's module graph).
- Every user-visible change adds a line under `## [Unreleased]` in `CHANGELOG.md` in the same commit.
- Conventional commit prefixes, one logical change per commit, and each commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Never `t.Parallel()` in ruler tests; no sleeps in tests (`require.Eventually`).
- Wrap errors with context (`fmt.Errorf("…: %w", err)`); no secrets in errors or logs.

## Review Focus

1. **A backfill push (9–60 days) arriving while live pushes are in flight.** Expected: neither fails. Task 1 asserts that the wide push runs with `async_insert=0`, so it is never merged into the live pushes' flush. A merged flush of narrow pushes from many tenants with many distinct retention periods relies on the async-path safety-net setting, as shown by the probe above.
2. **`APPLY DELETED MASK` fails** (timeout, or ClickHouse restarts mid-mutation). Expected: the next pass retries it even though no new request is due. Task 4 tests the retry.
3. **A multi-tenant request where one tenant has `volume_enabled: false`.** Expected: 404 for the whole request (Loki's frontend requires the flag for every tenant). Task 6 tests it.
4. **A multi-tenant header on index/stats with `multi_tenant_queries_enabled: false`.** Expected: still `multiple org IDs present`, unchanged. Task 6 tests it.
5. **`/ready` while ClickHouse hangs rather than refuses.** Expected: 503 within the 2 s readiness timeout, not a hang until the read timeout. Task 3 tests it with a blocking ping.

---

### Task 1: Bound the partitions one push may touch

Policy:
- A push touching more than `MaxPushPartitions = 60` `(UTC day, retention_days)` partitions is rejected with 400 and nothing is stored. That is 60 days for a stream set with one retention period. Clients do not retry a 400, so the push is dropped instead of retried forever.
- A push touching more than `AsyncMaxPartitions = 8` partitions (normal pushes touch 1–2, and late data with Loki's default `reject_old_samples_max_age: 1w` touches at most 8) is inserted synchronously (`async_insert=0`). It then never shares an async flush with other pushes, so it cannot make them fail, and on its own it stays below ClickHouse's 100-partition limit.
- The async path keeps `throw_on_max_partitions_per_insert_block=0` as a safety net only. It holds pushes of ≤ 8 partitions each, but one flush merges pushes from all tenants. With many distinct retention periods, or tenants that disabled `reject_old_samples`, their union can still exceed 100, and then ClickHouse would fail every push in the flush. These pushes were each acceptable, and their rows have to be written anyway, so the setting does not admit any wider data than the per-push bound already allows.

**Files:**
- Modify: `internal/chstore/writer.go`
- Modify: `internal/ingest/handler.go` (400 on `ErrTooManyPartitions`)
- Test: `internal/chstore/writer_test.go`, `internal/ingest/handler_test.go`
- Modify: `docs/divergences.md` (Ingestion), `docs/reference/http-api.md` (push section, if it lists error statuses), `CHANGELOG.md`

**Interfaces:**
- Produces in `chstore`: `const MaxPushPartitions = 60`, `const AsyncMaxPartitions = 8`, `var ErrTooManyPartitions = errors.New("too many partitions")`. `Writer.Write` returns an error wrapping `ErrTooManyPartitions` (check it with `errors.Is`) before inserting anything.
- Produces in `ingest`: discard reason `too_many_partitions` on `loki_discarded_samples_total`/`loki_discarded_bytes_total`.

- [ ] **Step 1: Write the failing writer tests** (append to `internal/chstore/writer_test.go`; add `"errors"` and `"fmt"` to the imports)

```go
// dailyEntries returns one entry per UTC day for the n days before today.
func dailyEntries(n int) []logproto.Entry {
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Hour)
	var entries []logproto.Entry
	for i := n; i > 0; i-- {
		entries = append(entries, logproto.Entry{Timestamp: day.AddDate(0, 0, -i), Line: fmt.Sprintf("day %d", i)})
	}
	return entries
}

// lastLogsInsertAsync reports whether the latest INSERT INTO logs in db ran as an async insert.
func lastLogsInsertAsync(t *testing.T, conn driver.Conn, db string) bool {
	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var v string
	require.NoError(t, conn.QueryRow(ctx, `SELECT Settings['async_insert'] FROM system.query_log
		WHERE type = 'QueryFinish' AND is_initial_query AND current_database = ? AND query LIKE 'INSERT INTO logs%'
		ORDER BY event_time_microseconds DESC LIMIT 1`, db).Scan(&v))
	return v == "1"
}

func TestWriter_RejectsTooManyPartitions(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	err := w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="backfill"}`, Entries: dailyEntries(chstore.MaxPushPartitions + 1)}}, nil)
	require.True(t, errors.Is(err, chstore.ErrTooManyPartitions), "got %v", err)

	var logs, streams uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant='t1'").Scan(&logs))
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM streams WHERE tenant='t1'").Scan(&streams))
	require.Zero(t, logs, "a rejected push stores nothing")
	require.Zero(t, streams, "a rejected push registers no streams")
}

// Two retention periods double the partitions: 31 days x 2 > 60.
func TestWriter_PartitionsCountRetention(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	ret := func(ls labels.Labels) uint16 {
		if ls.Get("app") == "short" {
			return 7
		}
		return 30
	}
	err := w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="short"}`, Entries: dailyEntries(31)},
		{Labels: `{app="long"}`, Entries: dailyEntries(31)},
	}, ret)
	require.ErrorIs(t, err, chstore.ErrTooManyPartitions)
}

func TestWriter_WidePushIsSynchronous(t *testing.T) {
	conn, db := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}

	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="live"}`, Entries: dailyEntries(1)}}, nil))
	require.True(t, lastLogsInsertAsync(t, conn, db), "a narrow push uses the async-insert buffer")

	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{app="backfill"}`, Entries: dailyEntries(chstore.MaxPushPartitions)}}, nil))
	require.False(t, lastLogsInsertAsync(t, conn, db), "a wide push bypasses the shared async buffer")

	var rows uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant='t1'").Scan(&rows))
	require.Equal(t, uint64(1+chstore.MaxPushPartitions), rows)
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/chstore/ -run 'TestWriter_(RejectsTooManyPartitions|PartitionsCountRetention|WidePushIsSynchronous)' -v`
Expected: FAIL to compile (`undefined: chstore.MaxPushPartitions`).

- [ ] **Step 3: Implement in `internal/chstore/writer.go`**

Add next to `maxRegistered` (and add `"errors"` to the imports):

```go
const (
	// MaxPushPartitions bounds the (UTC day, retention_days) partitions one push
	// may touch; wider pushes are rejected whole (split them by time range).
	// It stays well below ClickHouse's max_partitions_per_insert_block (100).
	MaxPushPartitions = 60
	// AsyncMaxPartitions is the widest push that goes through the async-insert
	// buffer: one buffer flush merges the pushes of all tenants, so a wide push
	// there could take the flush past ClickHouse's partition limit and fail
	// every push in it. Wider pushes are inserted synchronously, on their own.
	// Live pushes touch 1–2 partitions; late data under Loki's default
	// reject_old_samples_max_age (1w) at most 8.
	AsyncMaxPartitions = 8
)

// ErrTooManyPartitions is returned (wrapped) by Write for a push touching more
// than MaxPushPartitions partitions; nothing is stored.
var ErrTooManyPartitions = errors.New("too many partitions")

type partitionKey struct {
	day time.Time
	ret uint16
}
```

Restructure the start of `Write`. The settings now depend on the push width, so the batches are prepared after a first pass that counts partitions:

```go
func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream, retentionDays func(labels.Labels) uint16) error {
	type prepared struct {
		ls  labels.Labels
		id  uint64
		ret uint16
	}
	ps := make([]prepared, len(streams))
	parts := map[partitionKey]struct{}{}
	for i, s := range streams {
		ls, id, err := StreamIdentity(s.Labels)
		if err != nil {
			return fmt.Errorf("stream labels %q: %w", s.Labels, err)
		}
		var ret uint16
		if retentionDays != nil {
			ret = retentionDays(ls)
		}
		ps[i] = prepared{ls, id, ret}
		for _, e := range s.Entries {
			parts[partitionKey{e.Timestamp.UTC().Truncate(24 * time.Hour), ret}] = struct{}{}
		}
	}
	if len(parts) > MaxPushPartitions {
		return fmt.Errorf("%w: push spans %d (day, retention) partitions, at most %d are accepted per push; split it by time range",
			ErrTooManyPartitions, len(parts), MaxPushPartitions)
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(insertSettings(len(parts))))
	logs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO logs (tenant, stream_id, ts, line, structured_metadata, retention_days)")
	// … unchanged from here, except: the per-stream loop uses ps[i].ls, ps[i].id and
	// ps[i].ret instead of calling StreamIdentity and retentionDays again …
```

Change the per-stream loop header to `for i, s := range streams { ls, id, ret := ps[i].ls, ps[i].id, ps[i].ret`, and delete the old `StreamIdentity`/`retentionDays` lines inside it.

Add below `Write`:

```go
// insertSettings returns the session settings for a push touching n partitions.
// wait_for_async_insert makes an async insert synchronous for the caller: the
// push API returns 204 only once rows are durable, so an immediate query sees
// them. throw_on_max_partitions_per_insert_block=0 is only a safety net for the
// merged flush: each push in it is within AsyncMaxPartitions, but their union
// across tenants may not be, and failing it would fail every one of them.
func insertSettings(n int) clickhouse.Settings {
	if n > AsyncMaxPartitions {
		return clickhouse.Settings{"async_insert": 0}
	}
	return clickhouse.Settings{
		"async_insert": 1, "wait_for_async_insert": 1,
		"throw_on_max_partitions_per_insert_block": 0,
	}
}
```

Remove the old settings block and its comment. Update the `Writer` doc comment so it says: "every push of at most AsyncMaxPartitions partitions is one async insert per table … wider pushes are synchronous inserts."

- [ ] **Step 4: Run the writer tests**

Run: `go test -race ./internal/chstore/`
Expected: PASS.

- [ ] **Step 5: Write the failing handler test** (append to `internal/ingest/handler_test.go`, using its `push` helper and the handler constructor the other tests use)

```go
func TestPush_TooManyPartitionsIs400(t *testing.T) {
	h := newHandler(t) // the existing constructor in this file; adapt the name
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Hour)
	var values []string
	for i := chstore.MaxPushPartitions + 1; i > 0; i-- {
		values = append(values, fmt.Sprintf(`[%q,"x"]`, strconv.FormatInt(day.AddDate(0, 0, -i).UnixNano(), 10)))
	}
	body := `{"streams":[{"stream":{"app":"a"},"values":[` + strings.Join(values, ",") + `]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "at most 60 are accepted per push")
}
```

The push limits must accept 61-day-old samples. If the test's limits keep Loki's default `reject_old_samples: true` (max age 1w), set `RejectOldSamples = false` on the limits this test uses, the way other tests in the file adjust limits.

Run: `go test ./internal/ingest/ -run TestPush_TooManyPartitionsIs400 -v`
Expected: FAIL (status 500).

- [ ] **Step 6: Implement in `internal/ingest/handler.go`**

Add the reason constant:

```go
// reasonTooManyPartitions labels pushes rejected for spanning more than
// chstore.MaxPushPartitions partitions in Loki's discarded-data metrics.
const reasonTooManyPartitions = "too_many_partitions"
```

Replace the write-error branch:

```go
		if err := h.Writer.Write(r.Context(), tenantID, res.Streams, h.retentionDays(tenantID)); err != nil {
			if errors.Is(err, chstore.ErrTooManyPartitions) {
				distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, reasonTooManyPartitions, res.Streams, time.Now())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			level.Error(h.Logger).Log("msg", "write failed", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
```

Add `"errors"` to the imports.

- [ ] **Step 7: Run the ingest tests**

Run: `go test -race ./internal/ingest/ ./internal/chstore/`
Expected: PASS.

- [ ] **Step 8: Divergence, docs, CHANGELOG**

`docs/divergences.md`, Ingestion section:

```markdown
- **Pushes spanning many days:** Loki accepts a push of any time span that passes its per-entry checks. lokihouse rejects a push touching more than 60 `(UTC day, retention period)` partitions with 400 (`too many partitions: … split it by time range`), stores nothing from it, and counts it under the discard reason `too_many_partitions`. With one retention period that is 60 days. Each partition becomes a separate ClickHouse part, and ClickHouse refuses inserts over 100 partitions. Pushes over 8 partitions are inserted synchronously instead of through the shared async-insert buffer, so a backfill cannot fail other tenants' pushes. Backfills must send at most 60 days per push.
```

If `docs/reference/http-api.md` documents push responses, add the 400 case there in one line.

CHANGELOG under `## [Unreleased]`:

```markdown
### Changed

- A push spanning more than 60 day/retention partitions is rejected with 400 (discard reason `too_many_partitions`) instead of failing in ClickHouse with 500 forever; pushes over 8 partitions bypass the async-insert buffer, so a backfill can no longer fail other pushes that share its flush.
```

- [ ] **Step 9: Commit**

```bash
make check
git add internal/chstore/writer.go internal/chstore/writer_test.go internal/ingest docs CHANGELOG.md
git commit -m "fix(ingest): reject pushes spanning more than 60 partitions, insert wide pushes synchronously"
```

---

### Task 2: Explicit ClickHouse client timeouts and pool size

**Files:**
- Modify: `internal/config/config.go` (ClickHouse struct, defaults, validation, new `CHOptions`)
- Modify: `internal/server/server.go` (`openClickHouse` uses `cfg.CHOptions`)
- Test: `internal/config/config_test.go`
- Modify: `docs/reference/configuration.md` (the `clickhouse` table)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Produces: `func (c *Config) CHOptions(db string) *clickhouse.Options`. It returns the options for every lokihouse connection to database `db`.
- Produces config keys: `clickhouse.dial_timeout` (default `5s`), `clickhouse.read_timeout` (default `5m`), `clickhouse.max_open_conns` (default `100`), `clickhouse.max_idle_conns` (default `20`).

- [ ] **Step 1: Write the failing tests** (append to `internal/config/config_test.go`)

```go
func TestLoad_ClickHouseClientDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("{}\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	o := c.CHOptions("logs")
	require.Equal(t, 5*time.Second, o.DialTimeout)
	require.Equal(t, 5*time.Minute, o.ReadTimeout)
	require.Equal(t, 100, o.MaxOpenConns)
	require.Equal(t, 20, o.MaxIdleConns)
	require.Equal(t, "logs", o.Auth.Database)
	require.Equal(t, []string{"localhost:9000"}, o.Addr)
}

func TestLoad_ClickHouseClientOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse: {dial_timeout: 2s, read_timeout: 1m, max_open_conns: 8, max_idle_conns: 4}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	o := c.CHOptions("default")
	require.Equal(t, 2*time.Second, o.DialTimeout)
	require.Equal(t, time.Minute, o.ReadTimeout)
	require.Equal(t, 8, o.MaxOpenConns)
	require.Equal(t, 4, o.MaxIdleConns)
}

func TestLoad_ClickHouseClientInvalid(t *testing.T) {
	for _, body := range []string{
		"clickhouse: {max_open_conns: 0}\n",
		"clickhouse: {max_open_conns: 4, max_idle_conns: 5}\n",
		"clickhouse: {dial_timeout: 0s}\n",
		"clickhouse: {read_timeout: -1s}\n",
	} {
		p := filepath.Join(t.TempDir(), "c.yaml")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		_, err := Load(p, false)
		require.Error(t, err, body)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/config/ -run TestLoad_ClickHouseClient -v`
Expected: FAIL to compile (`c.CHOptions undefined`).

- [ ] **Step 3: Implement the config**

In the `ClickHouse` struct in `internal/config/config.go`, add after `Settings`:

```go
		DialTimeout  time.Duration  `yaml:"dial_timeout"`   // connect and pool-acquire timeout; default 5s
		ReadTimeout  time.Duration  `yaml:"read_timeout"`   // longest wait for a server packet; default 5m
		MaxOpenConns int            `yaml:"max_open_conns"` // connection pool size; default 100
		MaxIdleConns int            `yaml:"max_idle_conns"` // idle connections kept; default 20
```

Next to the other `c.ClickHouse.*` defaults:

```go
	// clickhouse-go's own defaults (30s dial, 10 connections) cap one instance
	// at ~50 pushes/s: each push holds a connection for the async-insert flush.
	c.ClickHouse.DialTimeout = 5 * time.Second
	c.ClickHouse.ReadTimeout = 5 * time.Minute
	c.ClickHouse.MaxOpenConns = 100
	c.ClickHouse.MaxIdleConns = 20
```

Add a validation function and call it from `Load` where the other validations (for example `readPasswordFile`) are called, wrapping its error the same way they do:

```go
// validateClickHouseClient rejects pool and timeout values clickhouse-go would
// silently replace with its own defaults.
func (c *Config) validateClickHouseClient() error {
	ch := c.ClickHouse
	switch {
	case ch.DialTimeout <= 0:
		return errors.New("clickhouse: dial_timeout must be positive")
	case ch.ReadTimeout <= 0:
		return errors.New("clickhouse: read_timeout must be positive")
	case ch.MaxOpenConns <= 0:
		return errors.New("clickhouse: max_open_conns must be positive")
	case ch.MaxIdleConns <= 0 || ch.MaxIdleConns > ch.MaxOpenConns:
		return errors.New("clickhouse: max_idle_conns must be between 1 and max_open_conns")
	}
	return nil
}
```

Add next to `CHSettings`:

```go
// CHOptions returns the clickhouse-go options for a connection to db.
func (c *Config) CHOptions(db string) *clickhouse.Options {
	ch := c.ClickHouse
	return &clickhouse.Options{
		Addr:         ch.Addr,
		Auth:         clickhouse.Auth{Database: db, Username: ch.Username, Password: ch.Password},
		Settings:     c.CHSettings(),
		DialTimeout:  ch.DialTimeout,
		ReadTimeout:  ch.ReadTimeout,
		MaxOpenConns: ch.MaxOpenConns,
		MaxIdleConns: ch.MaxIdleConns,
	}
}
```

- [ ] **Step 4: Use it in the server**

In `internal/server/server.go`, `openClickHouse`: delete the local `opts` closure and use `clickhouse.Open(cfg.CHOptions("default"))` and `clickhouse.Open(cfg.CHOptions(cfg.ClickHouse.Database))`.

- [ ] **Step 5: Run the tests**

Run: `go test -race ./internal/config/ ./internal/server/`
Expected: PASS.

- [ ] **Step 6: Docs and CHANGELOG**

In `docs/reference/configuration.md`, add four rows to the `clickhouse` table, in the same column format as its existing rows:

```markdown
| `clickhouse.dial_timeout` | duration | `5s` | Timeout for opening a ClickHouse connection and for waiting for a free pooled connection. |
| `clickhouse.read_timeout` | duration | `5m` | Longest wait for a packet from ClickHouse during a query or insert. |
| `clickhouse.max_open_conns` | int | `100` | Connection pool size. Each in-flight push holds a connection until its async insert is flushed; pushes, queries, tails and delete processing share the pool. |
| `clickhouse.max_idle_conns` | int | `20` | Idle connections kept open (≤ `max_open_conns`). |
```

CHANGELOG under `## [Unreleased]`, `### Added`:

```markdown
- `clickhouse.dial_timeout`, `clickhouse.read_timeout`, `clickhouse.max_open_conns` and `clickhouse.max_idle_conns`. The pool now defaults to 100 connections (was clickhouse-go's 10) and the dial timeout to 5s (was 30s).
```

- [ ] **Step 7: Commit**

```bash
make check
git add internal/config internal/server/server.go docs/reference/configuration.md CHANGELOG.md
git commit -m "feat(clickhouse): explicit client timeouts and pool size"
```

---

### Task 3: `/ready` reports ClickHouse reachability

**Files:**
- Modify: `internal/api/api.go` (`Ready` field, `/ready` handler)
- Modify: `internal/server/server.go` (`newAPI` sets `Ready: conn.Ping`)
- Test: `internal/api/api_test.go`
- Modify: `docs/operations.md`, `CHANGELOG.md`

**Interfaces:**
- Produces: `api.API.Ready func(context.Context) error`. When it is nil, `/ready` always answers ready (tests).

- [ ] **Step 1: Write the failing tests** (append to `internal/api/api_test.go`; build the API the way the existing `/ready` test at line ~112 does, then set `Ready`)

```go
func TestReady_ClickHouseDown(t *testing.T) {
	a := &api.API{Ready: func(context.Context) error { return errors.New("dial tcp: connection refused") }}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	code, body := do(t, "GET", srv.URL+"/ready", "", "")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Contains(t, body, "ClickHouse not ready: dial tcp: connection refused")
}

// A hanging ClickHouse must not hang the probe: the ping gets a short deadline.
func TestReady_ClickHouseHangs(t *testing.T) {
	a := &api.API{Ready: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	start := time.Now()
	code, _ := do(t, "GET", srv.URL+"/ready", "", "")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Less(t, time.Since(start), 10*time.Second)
}

func TestReady_ClickHouseUp(t *testing.T) {
	a := &api.API{Ready: func(context.Context) error { return nil }}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	code, body := do(t, "GET", srv.URL+"/ready", "", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ready\n", body)
}
```

If `Router()` needs more fields than a bare `&api.API{}` provides, copy the fields the existing `/ready` test uses. Look at its helper before writing these tests.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/api/ -run TestReady -v`
Expected: FAIL to compile (`unknown field Ready`).

- [ ] **Step 3: Implement**

In the `API` struct in `internal/api/api.go`:

```go
	// Ready, when set, is called by GET /ready; an error answers 503
	// (lokihouse is not ready without ClickHouse).
	Ready func(context.Context) error
```

Add a package constant:

```go
// readyTimeout bounds the ClickHouse ping behind /ready, so a hanging server
// fails the probe instead of hanging it.
const readyTimeout = 2 * time.Second
```

Replace the `/ready` route:

```go
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if a.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
			defer cancel()
			if err := a.Ready(ctx); err != nil {
				http.Error(w, "ClickHouse not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready\n"))
	})
```

In `internal/server/server.go` `newAPI`, add `Ready: conn.Ping,` to the `api.API` literal.

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/api/ ./internal/server/`
Expected: PASS.

- [ ] **Step 5: Docs and CHANGELOG**

In `docs/operations.md`, add a section (or extend the existing health-check text, if there is one):

```markdown
## Health checks

`GET /ready` answers `200 ready` when lokihouse can reach ClickHouse (a ping with a 2 s deadline) and `503 ClickHouse not ready: <error>` otherwise. All lokihouse instances usually share one ClickHouse, so they become unready together; use `/ready` for load-balancer readiness, not for liveness or restarts.
```

CHANGELOG `### Changed`:

```markdown
- `GET /ready` returns 503 when ClickHouse is unreachable (it always returned 200 before).
```

- [ ] **Step 6: Commit**

```bash
make check
git add internal/api internal/server/server.go docs/operations.md CHANGELOG.md
git commit -m "feat(api): /ready reports whether ClickHouse is reachable"
```

---

### Task 4: Delete processing physically removes deleted rows

**Files:**
- Modify: `internal/chstore/deleterows.go` (new `ApplyDeletedMask`)
- Modify: `internal/deletes/processor.go` (`ProcessDue` runs it; retry state)
- Test: `internal/deletes/processor_test.go`
- Modify: `docs/operations.md`, `CHANGELOG.md`

**Interfaces:**
- Produces: `func ApplyDeletedMask(ctx context.Context, conn driver.Conn, db string, topo Topology) error` in `chstore`.
- Processor gets an unexported field `maskPending bool`. Only the goroutine calling `ProcessDue` touches it, and `Run` calls `ProcessDue` from one goroutine.

- [ ] **Step 1: Write the failing tests** (append to `internal/deletes/processor_test.go`)

```go
// storedRows counts a tenant's rows including those only masked by a
// lightweight DELETE (apply_deleted_mask=0 shows them).
func storedRows(t *testing.T, conn driver.Conn, tenant string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count() FROM logs WHERE tenant = ? SETTINGS apply_deleted_mask = 0", tenant).Scan(&n))
	return n
}

func seedSecret(t *testing.T, conn driver.Conn, store *deletes.Store) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep"}}},
		{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "secret x"}, {Timestamp: t0.Add(time.Second), Line: "secret y"}}},
	}, nil))
	_, err := store.Add(context.Background(), "t1", `{app="b"}`,
		model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Hour).UnixNano()))
	require.NoError(t, err)
}

func TestProcessor_PhysicallyRemovesRows(t *testing.T) {
	p, store, conn, _ := newProcessor(t, 0, time.Hour, t0, t0)
	seedSecret(t, conn, store)
	require.NoError(t, p.ProcessDue(context.Background(), t0))
	require.Equal(t, []string{"keep"}, lines(t, conn, "t1"))
	require.Equal(t, uint64(1), storedRows(t, conn, "t1"), "masked rows must be gone from disk")
}

// maskFailConn fails the first APPLY DELETED MASK, to check the next pass retries it.
type maskFailConn struct {
	driver.Conn
	failed bool
}

func (f *maskFailConn) Exec(ctx context.Context, q string, args ...any) error {
	if !f.failed && strings.Contains(q, "APPLY DELETED MASK") {
		f.failed = true
		return context.DeadlineExceeded
	}
	return f.Conn.Exec(ctx, q, args...)
}

func TestProcessor_RetriesDeletedMask(t *testing.T) {
	conn, db := chtest.New(t)
	fc := &maskFailConn{Conn: conn}
	p, store, _ := buildProcessor(fc, db, 0, time.Hour, t0, t0)
	seedSecret(t, conn, store)

	require.Error(t, p.ProcessDue(context.Background(), t0))
	require.Equal(t, []string{"keep"}, lines(t, conn, "t1"), "rows are deleted even when the mask step fails")
	require.Equal(t, uint64(3), storedRows(t, conn, "t1"))

	require.NoError(t, p.ProcessDue(context.Background(), t0), "no request is due, the mask is retried")
	require.Equal(t, uint64(1), storedRows(t, conn, "t1"))
}
```

Add `"strings"` to the imports.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/deletes/ -run 'TestProcessor_(PhysicallyRemovesRows|RetriesDeletedMask)' -v`
Expected: FAIL. `storedRows` returns 3 where 1 is expected, and the first `ProcessDue` in the retry test returns nil.

- [ ] **Step 3: Implement `ApplyDeletedMask`** (append to `internal/chstore/deleterows.go`)

```go
// ApplyDeletedMask physically removes rows that lightweight DELETEs only
// masked. Without it they stay on disk until a merge rewrites their part, which
// for a day partition that no longer receives inserts may be never; Loki's
// compactor rewrites the chunks. Only parts carrying a mask are rewritten.
// mutations_sync=2 waits for every replica.
func ApplyDeletedMask(ctx context.Context, conn driver.Conn, db string, topo Topology) error {
	sql := fmt.Sprintf("ALTER TABLE %s.%s %s APPLY DELETED MASK", quoteIdent(db), topo.Local("logs"), topo.OnCluster())
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2}))
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("apply deleted mask: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Call it from `ProcessDue`**

In `internal/deletes/processor.go`, add the field to `Processor`:

```go
	// maskPending is set after a pass deleted rows and cleared once
	// ApplyDeletedMask succeeded; only the goroutine calling ProcessDue uses it.
	maskPending bool
```

In `ProcessDue`, set `p.maskPending = true` right after a successful `p.process(ctx, r)` (before `MarkProcessed`), and before `return first` add:

```go
	if p.maskPending {
		if err := chstore.ApplyDeletedMask(ctx, p.Conn, p.DB, p.Topo); err != nil {
			if p.Logger != nil {
				level.Error(p.Logger).Log("msg", "removing deleted rows from disk failed; will retry", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			if first == nil {
				first = err
			}
		} else {
			p.maskPending = false
		}
	}
```

Update the `ProcessDue` doc comment by appending: "After a pass that deleted rows it runs APPLY DELETED MASK so the rows leave the disk too; a failed mask step is retried on the next pass."

- [ ] **Step 5: Run the tests**

Run: `go test -race ./internal/deletes/ ./internal/chstore/`
Expected: PASS. Also run `make test-cluster`, because `ON CLUSTER` and `logs_local` are exercised there. Expected: PASS.

- [ ] **Step 6: Docs and CHANGELOG**

In `docs/operations.md`, under the delete-request section (or a new `## Deletes and disk space` section):

```markdown
## Deletes and disk space

A processed delete request removes rows with lightweight `DELETE`s and then runs `ALTER TABLE logs_local APPLY DELETED MASK` (ON CLUSTER on a cluster), which rewrites only the parts that hold deleted rows. Deleted lines therefore leave the disk at the end of the compactor pass that processed the request, as with Loki's compactor. The rewrite costs I/O proportional to the affected parts; it runs once per pass, not once per request.
```

CHANGELOG `### Changed`:

```markdown
- Processed delete requests now physically remove the deleted rows from disk (`APPLY DELETED MASK`); before, they stayed on disk, hidden, until a merge or retention dropped their partition.
```

- [ ] **Step 7: Commit**

```bash
make check
git add internal/chstore/deleterows.go internal/deletes docs/operations.md CHANGELOG.md
git commit -m "feat(deletes): remove deleted rows from disk after processing"
```

---

### Task 5: Multi-tenant fan-out for index stats, volume, detected labels and patterns

**Files:**
- Create: `internal/multitenant/index.go`
- Test: `internal/multitenant/index_test.go`

**Interfaces:**
- Produces (used by Task 6):

```go
type IndexStore interface {
	IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error)
	Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error)
	DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error)
	Patterns(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error)
}
type Index struct{ Next IndexStore } // implements IndexStore
```

- [ ] **Step 1: Write the failing tests** (`internal/multitenant/index_test.go`; no ClickHouse needed)

```go
package multitenant_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/multitenant"
)

// fakeIndex answers per tenant from fixed data and records the tenants it saw.
type fakeIndex struct {
	seen []string
	fail string // tenant whose calls fail
}

func (f *fakeIndex) tenant(ctx context.Context) (string, error) {
	id, err := tenant.TenantID(ctx)
	if err != nil {
		return "", err
	}
	f.seen = append(f.seen, id)
	if id == f.fail {
		return "", errors.New("boom " + id)
	}
	return id, nil
}

func (f *fakeIndex) IndexStats(ctx context.Context, _ *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	if _, err := f.tenant(ctx); err != nil {
		return nil, err
	}
	return &logproto.IndexStatsResponse{Streams: 1, Chunks: 2, Entries: 3, Bytes: 4}, nil
}

func (f *fakeIndex) Volume(ctx context.Context, _ *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	if _, err := f.tenant(ctx); err != nil {
		return nil, err
	}
	return &logproto.VolumeResponse{Volumes: []logproto.Volume{{Name: `{app="a"}`, Volume: 10}}, Limit: 100}, nil
}

func (f *fakeIndex) DetectedLabels(ctx context.Context, _ *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	id, err := f.tenant(ctx)
	if err != nil {
		return nil, err
	}
	sk := hyperloglog.New()
	sk.Insert([]byte(id)) // a different value per tenant: merged cardinality 2
	b, err := sk.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &logproto.DetectedLabelsResponse{DetectedLabels: []*logproto.DetectedLabel{{Label: "app", Cardinality: 1, Sketch: b}}}, nil
}

func (f *fakeIndex) Patterns(ctx context.Context, _ *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	id, err := f.tenant(ctx)
	if err != nil {
		return nil, err
	}
	return &logproto.QueryPatternsResponse{Series: []*logproto.PatternSeries{{Pattern: "user <_> " + id, Samples: []*logproto.PatternSample{{Timestamp: 1000, Value: 1}}}}}, nil
}

func mtCtx(ids string) context.Context { return user.InjectOrgID(context.Background(), ids) }

func TestIndex_SingleTenantPassesThrough(t *testing.T) {
	f := &fakeIndex{}
	q := &multitenant.Index{Next: f}
	res, err := q.IndexStats(mtCtx("t1"), &logproto.IndexStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, &logproto.IndexStatsResponse{Streams: 1, Chunks: 2, Entries: 3, Bytes: 4}, res)
	require.Equal(t, []string{"t1"}, f.seen)
}

func TestIndex_IndexStatsMerged(t *testing.T) {
	f := &fakeIndex{}
	res, err := (&multitenant.Index{Next: f}).IndexStats(mtCtx("t1|t2"), &logproto.IndexStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, &logproto.IndexStatsResponse{Streams: 2, Chunks: 4, Entries: 6, Bytes: 8}, res)
	require.Equal(t, []string{"t1", "t2"}, f.seen)
}

func TestIndex_VolumeMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).Volume(mtCtx("t1|t2"), &logproto.VolumeRequest{Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []logproto.Volume{{Name: `{app="a"}`, Volume: 20}}, res.Volumes)
}

func TestIndex_DetectedLabelsMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).DetectedLabels(mtCtx("t1|t2"), &logproto.DetectedLabelsRequest{})
	require.NoError(t, err)
	require.Len(t, res.DetectedLabels, 1)
	require.Equal(t, "app", res.DetectedLabels[0].Label)
	require.Equal(t, uint64(2), res.DetectedLabels[0].Cardinality)
}

func TestIndex_PatternsMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).Patterns(mtCtx("t1|t2"), &logproto.QueryPatternsRequest{})
	require.NoError(t, err)
	var got []string
	for _, s := range res.Series {
		got = append(got, s.Pattern)
	}
	require.ElementsMatch(t, []string{"user <_> t1", "user <_> t2"}, got)
}

func TestIndex_TenantErrorFailsRequest(t *testing.T) {
	q := &multitenant.Index{Next: &fakeIndex{fail: "t2"}}
	ctx := mtCtx("t1|t2")
	_, err := q.IndexStats(ctx, &logproto.IndexStatsRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.Volume(ctx, &logproto.VolumeRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.DetectedLabels(ctx, &logproto.DetectedLabelsRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.Patterns(ctx, &logproto.QueryPatternsRequest{})
	require.ErrorContains(t, err, "boom t2")
}
```

`github.com/axiomhq/hyperloglog` is the HLL package `internal/chstore/detected.go` already uses, and `PatternSample.Timestamp` is a `model.Time`; the untyped `1000` literal converts to it.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/multitenant/ -run TestIndex -v`
Expected: FAIL to compile (`undefined: multitenant.Index`).

- [ ] **Step 3: Implement** (`internal/multitenant/index.go`)

```go
package multitenant

// Copied from grafana/loki@bc196286 pkg/querier/multi_tenant_querier.go
// (IndexStats, Volume, Patterns, DetectedLabels); modifications: wraps
// lokihouse's IndexStore instead of Loki's querier.Querier, and IndexStats
// takes the codec's *logproto.IndexStatsRequest instead of *loghttp.RangeQuery.

import (
	"context"
	"sort"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/querier/pattern"
	"github.com/grafana/loki/v3/pkg/storage/detected"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/seriesvolume"
	"github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
)

// IndexStore answers the index and drilldown endpoints for the single tenant
// in the context.
type IndexStore interface {
	IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error)
	Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error)
	DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error)
	Patterns(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error)
}

// Index answers the index and drilldown endpoints for an X-Scope-OrgID naming
// several tenants: one call per tenant, merged as Loki merges them. A single
// tenant passes straight through (Volume always merges, as in Loki).
type Index struct {
	Next IndexStore
}

// IndexStats sums the per-tenant statistics.
func (q *Index) IndexStats(ctx context.Context, req *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.IndexStats(ctx, req)
	}
	responses := make([]*stats.Stats, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.IndexStats(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	merged := stats.MergeStats(responses...)
	return &merged, nil
}

// Volume merges the per-tenant volumes, keeping the request's limit.
func (q *Index) Volume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	responses := make([]*logproto.VolumeResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.Volume(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	return seriesvolume.Merge(responses, req.Limit), nil
}

// Patterns merges the per-tenant pattern series.
func (q *Index) Patterns(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.Patterns(ctx, req)
	}
	responses := make([]*logproto.QueryPatternsResponse, len(tenantIDs))
	for i, id := range tenantIDs {
		resp, err := q.Next.Patterns(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}
	return pattern.MergePatternResponses(responses), nil
}

// DetectedLabels merges the per-tenant HyperLogLog sketches and sorts the
// labels by cardinality.
func (q *Index) DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(tenantIDs) == 1 {
		return q.Next.DetectedLabels(ctx, req)
	}
	var all []*logproto.DetectedLabel
	for _, id := range tenantIDs {
		resp, err := q.Next.DetectedLabels(user.InjectOrgID(ctx, id), req)
		if err != nil {
			return nil, err
		}
		all = append(all, resp.DetectedLabels...)
	}
	merged, err := detected.MergeLabels(all)
	if err != nil {
		return nil, err
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Cardinality < merged[j].Cardinality })
	return &logproto.DetectedLabelsResponse{DetectedLabels: merged}, nil
}
```

Then update the package comment in `internal/multitenant/querier.go`. Remove "the IndexStats/Volume/Patterns/Detected* methods are omitted because … answers those endpoints for one tenant only", and add one sentence: "index.go fans out the index and drilldown endpoints the same way."

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/multitenant/`
Expected: PASS. If `TestIndex_VolumeMerged` differs only in field order or in an extra `Limit`, check `seriesvolume.Merge` in Loki and assert what Loki returns. Do not change the copied code.

- [ ] **Step 5: Commit**

```bash
make check
git add internal/multitenant
git commit -m "feat(query): multi-tenant fan-out for index stats, volume, detected labels and patterns"
```

---

### Task 6: Serve the index and drilldown endpoints through the fan-out

**Files:**
- Create: `internal/api/index.go` (per-tenant adapter)
- Modify: `internal/api/lokicodec.go` (dispatch through the adapter)
- Modify: `internal/api/api.go` (`MultiTenant bool` field)
- Modify: `internal/server/server.go` (`MultiTenant: cfg.Query.Querier.MultiTenantQueriesEnabled`)
- Test: `internal/multitenant/querier_test.go` (HTTP tests; set `MultiTenant: mt` in `newMTServer`)
- Modify: `docs/divergences.md`, `docs/roadmap.md`, `docs/reference/configuration.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `multitenant.IndexStore`, `multitenant.Index` (Task 5).
- Produces: `api.API.MultiTenant bool`.

- [ ] **Step 1: Write the failing tests** (append to `internal/multitenant/querier_test.go`; first add `MultiTenant: mt,` to the `api.API` literal in `newMTServer`)

```go
func indexGet(t *testing.T, base, path, tenant, query string, now time.Time) (int, string) {
	t.Helper()
	v := url.Values{"query": {query}, "start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}
	return mtDo(t, http.MethodGet, base+path+"?"+v.Encode(), tenant)
}

func TestMultiTenant_IndexEndpoints(t *testing.T) {
	srv := newMTServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now, "one")
	pushOne(t, srv.URL, "t2", now, "two")

	code, body := indexGet(t, srv.URL, "/loki/api/v1/index/stats", "t1|t2", `{app="a"}`, now)
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"entries":2`)

	code, body = indexGet(t, srv.URL, "/loki/api/v1/index/volume", "t1|t2", `{app="a"}`, now)
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"app":"a"`)

	code, body = indexGet(t, srv.URL, "/loki/api/v1/detected_labels", "t1|t2", `{app="a"}`, now)
	require.Equal(t, http.StatusOK, code, body)

	code, body = indexGet(t, srv.URL, "/loki/api/v1/patterns", "t1|t2", `{app="a"}`, now)
	require.Equal(t, http.StatusOK, code, body)
}

func TestMultiTenant_IndexDisabledRejects(t *testing.T) {
	srv := newMTServer(t, false)
	now := time.Now()
	code, body := indexGet(t, srv.URL, "/loki/api/v1/index/stats", "t1|t2", `{app="a"}`, now)
	require.NotEqual(t, http.StatusOK, code)
	require.Contains(t, body, "multiple org IDs present")
}
```

Then the per-tenant `volume_enabled` case. It needs a per-tenant override, so build the `validation.Overrides` with a tenant-limits provider the way `internal/api/limits_test.go` does (look there for the existing helper), with `volume_enabled: false` for `t2`:

```go
func TestMultiTenant_VolumeDisabledForOneTenant(t *testing.T) {
	srv := newMTServerWithOverrides(t, map[string]func(*validation.Limits){
		"t2": func(l *validation.Limits) { l.VolumeEnabled = false },
	})
	code, _ := indexGet(t, srv.URL, "/loki/api/v1/index/volume", "t1|t2", `{app="a"}`, time.Now())
	require.Equal(t, http.StatusNotFound, code)
}
```

To make that work, refactor `newMTServer(t, mt)` into `newMTServerWithOverrides(t, per map[string]func(*validation.Limits))`, which always sets `mt = true`. Keep `newMTServer` as a thin wrapper for the existing tests.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/multitenant/ -run 'TestMultiTenant_(Index|Volume)' -v`
Expected: FAIL to compile (`unknown field MultiTenant`). Once the field exists, `index/stats` with `t1|t2` fails with `multiple org IDs present`.

- [ ] **Step 3: Implement the adapter** (`internal/api/index.go`)

```go
package api

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	querier_limits "github.com/grafana/loki/v3/pkg/querier/limits"

	"github.com/bzed/lokihouse/internal/multitenant"
)

// tenantIndex answers the index and drilldown endpoints for the single tenant
// in the context, applying Loki's per-tenant checks (volume_enabled, the
// query time-range limits for detected_labels). Under multitenant.Index the
// checks therefore run once per tenant, so one tenant with volume disabled
// fails the whole request, as in Loki.
type tenantIndex struct{ a *API }

func (t tenantIndex) IndexStats(ctx context.Context, r *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	return t.a.Q.IndexStats(ctx, r)
}

func (t tenantIndex) Volume(ctx context.Context, r *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	if !t.a.Limits.VolumeEnabled(tenantID) {
		return nil, httpgrpc.Errorf(http.StatusNotFound, "not found")
	}
	return t.a.Q.Volume(ctx, r)
}

func (t tenantIndex) DetectedLabels(ctx context.Context, r *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	dl := *r // the per-tenant limits must not leak into the next tenant's request
	if dl.Start, dl.End, err = querier_limits.ValidateQueryTimeRangeLimits(ctx, tenantID, t.a.Limits, dl.Start, dl.End); err != nil {
		return nil, err
	}
	return t.a.Q.DetectedLabels(ctx, &dl)
}

func (t tenantIndex) Patterns(ctx context.Context, r *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	return t.a.Patterns.Query(ctx, r)
}

// index returns what the codec handler calls for the index and drilldown
// endpoints: the per-tenant adapter, fanned out when multi-tenant queries are on.
func (a *API) index() multitenant.IndexStore {
	var idx multitenant.IndexStore = tenantIndex{a: a}
	if a.MultiTenant {
		idx = &multitenant.Index{Next: idx}
	}
	return idx
}
```

In the `API` struct in `internal/api/api.go`:

```go
	// MultiTenant fans the index and drilldown endpoints out per tenant
	// (query.querier.multi_tenant_queries_enabled).
	MultiTenant bool
```

In `internal/api/lokicodec.go`, replace the four cases of `Do` with:

```go
	case *logproto.IndexStatsRequest:
		res, err := h.a.index().IndexStats(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.IndexStatsResponse{Response: res}, nil
	case *logproto.VolumeRequest:
		res, err := h.a.index().Volume(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.VolumeResponse{Response: res}, nil
	case *queryrange.DetectedLabelsRequest:
		res, err := h.a.index().DetectedLabels(ctx, &r.DetectedLabelsRequest)
		if err != nil {
			return nil, err
		}
		return &queryrange.DetectedLabelsResponse{Response: res}, nil
```

and:

```go
	case *logproto.QueryPatternsRequest:
		res, err := h.a.index().Patterns(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.QueryPatternsResponse{Response: res}, nil
```

Remove the imports that are now unused (`tenant`, `querier_limits`, and `httpgrpc` if nothing else uses it). Check `internal/multitenant` for an import of `internal/api` first: it imports `api` only from its `_test` package, so there is no cycle.

In `internal/server/server.go` `newAPI`, add `MultiTenant: cfg.Query.Querier.MultiTenantQueriesEnabled,` to the `api.API` literal.

- [ ] **Step 4: Run the tests**

Run: `go test -race ./internal/api/ ./internal/multitenant/ ./internal/server/`
Expected: PASS, including the existing single-tenant codec tests in `internal/api/lokicodec_test.go`.

- [ ] **Step 5: Oracle**

Run: `make oracle-all && make oracle-cluster`
Expected: PASS. If the oracle has no multi-tenant cases, that is fine. It guards the single-tenant paths this task rerouted.

- [ ] **Step 6: Docs and CHANGELOG**

`docs/divergences.md`, Query section: replace the multi-tenant bullet with

```markdown
- **Multi-tenant queries** (`query.querier.multi_tenant_queries_enabled`): lokihouse reuses Loki's `pkg/querier/multi_tenant_querier.go` fan-out for the log and sample select paths, `label/values*`, `series`, `index/stats`, `index/volume*`, `detected_labels` and `patterns`. lokihouse does not serve `detected_metric_names`. With the flag on, multi-tenant queries always run on the plain engine (no ClickHouse push-down, which is single-tenant by construction).
```

`docs/roadmap.md`: in the "Per-policy ingestion limits" bullet, drop "multi-tenant support for the index, volume, detected and pattern endpoints".

`docs/reference/configuration.md`: append to the `query.querier.multi_tenant_queries_enabled` description: "This also applies to `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns`."

CHANGELOG `### Added`:

```markdown
- With `query.querier.multi_tenant_queries_enabled`, `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns` accept several tenants (`a|b`), as in Loki.
```

- [ ] **Step 7: Commit**

```bash
make check
git add internal/api internal/multitenant internal/server/server.go docs CHANGELOG.md
git commit -m "feat(api): multi-tenant index, volume, detected_labels and patterns endpoints"
```

---

### Task 7: Document ClickHouse outages and close the phase

**Files:**
- Modify: `docs/operations.md`
- Create: `docs/phase6-status.md`
- Modify: `docs/divergences.md` (add a phase 6 link to the "Implemented in" lines of Query and Delete requests)

- [ ] **Step 1: Operations section** (append to `docs/operations.md`)

```markdown
## When ClickHouse is unavailable

lokihouse keeps no local buffer; ClickHouse is the only state.

- **At startup** lokihouse creates its tables, and exits non-zero if ClickHouse is unreachable. The Debian unit restarts it after 5 s (`Restart=on-failure`); under Kubernetes the pod restarts with back-off.
- **Pushes** fail with HTTP 500. Loki clients (Promtail, Alloy, the OpenTelemetry Collector) retry 5xx responses with back-off, so nothing is lost unless an outage outlasts the client's retry budget (for example Promtail's `backoff_config.max_retries`). Size the clients' retries for the longest ClickHouse maintenance window you expect.
- **Queries, tails and the ruler** fail while ClickHouse is down. Rule evaluations are counted as failed, as in Loki.
- **Delete processing** logs the error and retries on the next `compactor.compaction_interval`. Query-time filtering keeps pending deletes hidden.
- **`/ready`** answers 503 (see Health checks).
- Requests wait at most `clickhouse.dial_timeout` for a connection and `clickhouse.read_timeout` for each answer from ClickHouse.
```

- [ ] **Step 2: Phase status file**

Create `docs/phase6-status.md` in the format of `docs/phase5-status.md`: a header with the date, branch and latest commit; an executive summary; the completed tasks 1–7 with commit hashes; the gate results (`make check` coverage figure, `make test-cluster`, `make oracle-all`, `make oracle-cluster`, `make soak`); and the deferred item: "SQL push-down for multi-tenant queries". Record the gate results from real runs at the final commit, not from memory.

- [ ] **Step 3: Run every gate**

Run: `make check && make test-cluster && make oracle-all && make oracle-cluster && make soak`
Expected: all PASS; coverage ≥ 85 %. Put the actual numbers into `docs/phase6-status.md`.

- [ ] **Step 4: Commit**

```bash
git add docs
git commit -m "docs: ClickHouse outage behaviour, phase 6 status"
```
