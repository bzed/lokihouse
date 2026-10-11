# Lokihouse Phase 4 (Ruler) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Loki's ruler inside lokihouse: rule groups stored on the local filesystem and managed through Loki's rules API, alerting and recording rules evaluated with lokihouse's own query engine, alerts sent to Alertmanager, and recording-rule samples sent by Prometheus remote write. Everything is compared against real Loki in the oracle.

**Architecture:** Loki's ruler is an exported library, so lokihouse reuses it whole and supplies only the glue:
- `pkg/ruler.NewRuler(cfg, evaluator, reg, logger, store, limits, ns)` builds the Cortex-derived `base.Ruler` (a dskit service), the per-tenant Prometheus rule managers, the notifier and the remote-write WAL registry.
- `base.NewRuleStore` with backend `filesystem` gives a writable rule store in a local directory. Backend `local` gives Loki's read-only directory of rule files.
- `base.NewAPI` provides the HTTP handlers.

Lokihouse adds four things: an `Evaluator` that runs instant queries on its engine (Tier A push-down with Tier B fallback, including delete filtering), config loading for Loki's `ruler:` and `ruler_storage:` blocks, route registration on Go's `ServeMux`, and startup wiring. Sharding (Loki's ruler ring) is not supported: exactly one lokihouse instance runs the ruler.

**Tech Stack:** Go 1.27, `github.com/grafana/loki/v3` @ `fbc3f0770b8c` (unchanged). New imports:
- `pkg/ruler`, `pkg/ruler/base`, `pkg/ruler/rulestore`
- `pkg/util/constants` (route paths)
- `github.com/grafana/dskit/services`
- `github.com/prometheus/prometheus/storage/remote` (test receivers decode remote-write requests), `github.com/prometheus/prometheus/prompb`

Also `clickhouse-go/v2` v2.49.0 and ClickHouse 26.9.

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`: §4 architecture ("ruler: Loki ruler/Prometheus rules manager with the same engine"), §6 feature row "Ruler (alerts + recording rules, rules API) → Loki ruler / Prometheus rule manager, query func = our engine; rule storage local dir → Phase 4", §7 correctness strategy incl. the divergence register, §8 Phase 4 row "Rules API + evaluation + recording-rule remote write, rule storage".

## Global Constraints

All Phase 1–3 Global Constraints still apply (`docs/superpowers/plans/2026-10-10-lokihouse-phase1-foundation.md`, `…-phase2-pushdown.md`, `…-phase3-apis.md`). In addition:

- Never bump the Loki commit inside a Phase 4 task. Task 1 is the first task to import `pkg/ruler`: run `go mod tidy` there and commit `go.mod`/`go.sum` with it.
- **Reuse Loki's ruler, never re-implement it.** This covers rule storage, rule validation, rule-group scheduling, alert state, the notifier, the remote-write WAL and queue, and the HTTP handlers. They all come from `pkg/ruler`, `pkg/ruler/base` and `pkg/ruler/rulestore`. Lokihouse code is limited to the evaluator, config, routes and wiring. A behaviour difference is fixed by routing through Loki's code, not by patching around it.
- **Config uses Loki's own keys and defaults.** The top-level `ruler:` block is `pkg/ruler.Config` and `ruler_storage:` is `rulestore.Config`, defaulted through their `RegisterFlags`, exactly as Loki parses them. A Loki ruler config block works unchanged in lokihouse. The only changed default is `ruler.rule_path`: `rules-temp` (relative to the working directory) instead of Loki's `/rules`. Loki's single binary moves it under `common.path_prefix`, which lokihouse does not have.
- **The ruler runs only when `ruler_storage` is configured** (`!RulerStorage.IsDefaults()`). That is Loki's single-binary rule ("Ruler storage is not configured; ruler will not be started").
- **Unsupported modes are rejected at startup** with a clear error. These are `ruler.enable_sharding: true` (lokihouse has no ruler ring) and `ruler.evaluation.mode: remote` (lokihouse evaluates locally; there is no query-frontend to dial).
- **One ruler per process.** Loki keeps the remote-write registry in a package variable (`pkg/ruler/compat.go` `registry`). Tests must stop each ruler before starting the next (`t.Cleanup`) and must never call `t.Parallel()` in ruler tests.
- The ruler registers its metrics in a private `prometheus.NewRegistry()`. Lokihouse has no `/metrics` endpoint yet (Phase 5); the default registerer must not collect duplicate Loki metric names.
- Code copied from Loki keeps the Phase 1 attribution rule: `// Copied from grafana/loki@fbc3f077 <path>; modifications: <list>`.
- Every accepted difference from Loki goes into `docs/divergences.md` (spec §7.4) with its reason, in the task that introduces it.
- Every task's tests must pass on the single node and on the 2×2 podman cluster (`make test`, `make test-cluster`). Task 4 also needs `make oracle-all` and `make oracle-cluster`.

## Review Focus

1. **Invalid rule groups** (LogQL syntax error, a recording-rule name that is not a metric name, `for:` on a recording rule, a body that is not YAML): rejected with HTTP 400 and Loki's message, and nothing stored. Owner: Task 2 (`TestRules_InvalidGroupRejected`), plus the oracle in Task 4.
2. **Tenant isolation.** Tenant B can neither list nor delete tenant A's groups, and A's rules only see A's data. Owner: Task 2 (`TestRules_TenantIsolation`).
3. **ClickHouse failing during evaluation:** the rule reports `health: "err"` with the error in `lastError`, the ruler keeps running, and the rule recovers to `ok` once queries succeed again. Owner: Task 2 (`TestRules_EvaluationErrorReported`).
4. **Restart:** rule groups survive a lokihouse restart (filesystem storage) and resume evaluating without any API call. Owner: Task 2 (`TestRules_SurviveRestart`).
5. **Loki's WAL wipe.** With remote write enabled, Loki runs `os.RemoveAll(ruler.wal.dir)` at every startup (`pkg/ruler/registry.go` `newWALRegistry`). A `wal.dir` of `.`, `/`, or a parent of the rule storage or `rule_path` would delete the working directory or the stored rules. Config validation must refuse those values. Owner: Task 3 (`TestValidate_WALDir`).

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/ruler/config.go` (new) | `Defaults`, `Enabled`, `Validate` (Task 1); WAL-dir guard (Task 3) |
| `internal/ruler/evaluator.go` (new) | `Engine` interface; `Evaluator` (Loki's `LocalEvaluator.Eval` over lokihouse's engine) (Task 1) |
| `internal/ruler/ruler.go` (new) | `New`: rule store + Loki ruler + rules API (Task 2) |
| `internal/api/rules.go` (new) | Ruler route registration on `ServeMux` with gorilla URL vars (Task 2) |
| `internal/api/api.go` (modify) | `Rules *rulerbase.API` field; call `ruleRoutes` (Task 2) |
| `internal/config/config.go` (modify) | `Ruler`, `RulerStorage` fields; defaults, validation, `RulerEnabled` (Task 1) |
| `cmd/lokihouse/main.go` (modify) | Start the ruler when enabled (Task 2) |
| `internal/ruler/*_test.go` (new) | Evaluator (Task 1), API + evaluation (Task 2), remote write + Alertmanager + WAL guard (Task 3) |
| `test/oracle/loki.go`, `test/oracle/lokihouse.go` (modify) | `extra` config parameter (Task 4) |
| `test/oracle/ruler_test.go` (new) | Remote-write and Alertmanager sinks, ruler oracle (Task 4) |
| `test/oracle/oracle_test.go` (modify) | Start sinks, pass ruler config, call `rulerOracle` (Task 4) |
| `lokihouse.example.yaml`, `docs/divergences.md`, `docs/phase4-status.md` (new) | Config example, divergences, status (Tasks 2–4) |

---

### Task 1: Ruler config and evaluator

**Files:**
- Create: `internal/ruler/config.go`, `internal/ruler/evaluator.go`, `internal/ruler/evaluator_test.go`
- Modify: `internal/config/config.go`, `internal/config/config_test.go`, `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `api.NewPlainQuerier(*logql.QueryEngine) api.Querier` (existing), `chstore.Querier`, `chstore.Writer`, `chtest.New` (existing).
- Produces:
  - `ruler.Engine` — `interface{ Query(ctx context.Context, p logql.Params) logql.Query }` (same method set as `api.Querier`, so `*pushdown.Engine` and `api.NewPlainQuerier(...)` satisfy it).
  - `ruler.Evaluator{Engine Engine}` with `Eval(ctx, qs string, now time.Time) (*logqlmodel.Result, error)` (implements Loki's `pkg/ruler.Evaluator`).
  - `ruler.Defaults() (lokiruler.Config, rulestore.Config)`, `ruler.Enabled(rulestore.Config) bool`, `ruler.Validate(lokiruler.Config, rulestore.Config) error`.
  - `config.Config.Ruler lokiruler.Config` (`yaml:"ruler"`), `config.Config.RulerStorage rulestore.Config` (`yaml:"ruler_storage"`), `(*config.Config).RulerEnabled() bool`.
  - Test helpers in package `ruler_test` (reused by Tasks 2–3): `overrides(t) *validation.Overrides`, `engine(t, conn) ruler.Engine`, `writeLines(t, conn, tenant string, n int)` (n lines of `{app="a"}` ending a minute before now).

Import aliases used throughout: `lokiruler "github.com/grafana/loki/v3/pkg/ruler"`, `rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"`, `"github.com/grafana/loki/v3/pkg/ruler/rulestore"`. Lokihouse's own package is `github.com/bzed/lokihouse/internal/ruler` (package `ruler`; imported as `ruler` in tests and `lhruler` in `cmd/lokihouse`).

- [ ] **Step 1: Write the failing config tests**

Append to `internal/config/config_test.go`:

```go
func TestLoad_RulerDisabledByDefault(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.False(t, c.RulerEnabled())
	require.Equal(t, "rules-temp", c.Ruler.RulePath)       // lokihouse default (Loki: /rules)
	require.Equal(t, time.Minute, c.Ruler.PollInterval)    // Loki default kept
	require.Equal(t, "local", c.Ruler.Evaluation.Mode)     // Loki default kept
}

func TestLoad_Ruler(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
ruler:
  external_url: http://grafana.example
  alertmanager_url: http://am:9093
  poll_interval: 5s
  remote_write:
    enabled: true
    clients:
      prom: {url: "http://prom:9090/api/v1/write"}
ruler_storage:
  backend: filesystem
  filesystem: {dir: /var/lib/lokihouse/rules}
`), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.True(t, c.RulerEnabled())
	require.Equal(t, "http://grafana.example", c.Ruler.ExternalURL.String())
	require.Equal(t, "http://am:9093", c.Ruler.AlertmanagerURL)
	require.Equal(t, 5*time.Second, c.Ruler.PollInterval)
	require.Equal(t, time.Minute, c.Ruler.EvaluationInterval) // Loki default kept
	require.Equal(t, "http://prom:9090/api/v1/write", c.Ruler.RemoteWrite.Clients["prom"].URL.String())
	require.Equal(t, "filesystem", c.RulerStorage.Backend)
	require.Equal(t, "/var/lib/lokihouse/rules", c.RulerStorage.Filesystem.Directory)
}

func TestLoad_RulerRejectsUnsupportedModes(t *testing.T) {
	for name, tc := range map[string]struct{ ruler, want string }{
		"sharding":          {"enable_sharding: true", "enable_sharding"},
		"remote evaluation": {"evaluation: {mode: remote}", "evaluation.mode"},
		"remote write without clients": {"remote_write: {enabled: true}", "no clients"},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte("ruler: {"+tc.ruler+"}\nruler_storage: {backend: filesystem, filesystem: {dir: /tmp/r}}\n"), 0o600))
			_, err := Load(p)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/config/ -run 'TestLoad_Ruler' -count=1`
Expected: FAIL to compile with `c.RulerEnabled undefined` / `c.Ruler undefined`.

- [ ] **Step 3: Implement `internal/ruler/config.go`**

```go
// Package ruler runs Loki's ruler (rule storage, evaluation, alert
// notification, recording-rule remote write and the rules HTTP API) over
// lokihouse's query engine.
package ruler

import (
	"errors"
	"flag"
	"fmt"

	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore"
)

// Defaults returns Loki's ruler and ruler_storage defaults (their flag
// defaults). rule_path moves from Loki's /rules to rules-temp, relative to the
// working directory like Loki's default ruler WAL directory (ruler-wal); Loki's
// single binary moves it under common.path_prefix, which lokihouse lacks.
func Defaults() (lokiruler.Config, rulestore.Config) {
	var rc lokiruler.Config
	rc.RegisterFlags(flag.NewFlagSet("ruler", flag.PanicOnError))
	rc.RulePath = "rules-temp"
	var sc rulestore.Config
	sc.RegisterFlags(flag.NewFlagSet("ruler_storage", flag.PanicOnError))
	return rc, sc
}

// Enabled reports whether the ruler runs: as in Loki's single binary, only
// when ruler_storage is configured.
func Enabled(sc rulestore.Config) bool { return !sc.IsDefaults() }

// Validate runs Loki's ruler config validation and rejects the modes lokihouse
// does not support.
func Validate(rc lokiruler.Config, sc rulestore.Config) error {
	if err := rc.Validate(); err != nil {
		return err
	}
	if err := rc.Evaluation.Validate(); err != nil {
		return err
	}
	if rc.Evaluation.Mode != lokiruler.EvalModeLocal {
		return fmt.Errorf("ruler.evaluation.mode: %q is not supported (lokihouse evaluates rules locally)", rc.Evaluation.Mode)
	}
	if rc.EnableSharding {
		return errors.New("ruler.enable_sharding: not supported (lokihouse has no ruler ring); run the ruler on one instance")
	}
	return nil
}
```

(`sc` is used by the WAL-dir guard added in Task 3.)

- [ ] **Step 4: Wire it into `internal/config/config.go`**

Add the imports `lokiruler "github.com/grafana/loki/v3/pkg/ruler"`, `"github.com/grafana/loki/v3/pkg/ruler/rulestore"` and `"github.com/bzed/lokihouse/internal/ruler"`. Add two fields after `Compactor`:

```go
	Ruler        lokiruler.Config `yaml:"ruler"`         // Loki's ruler block, Loki's keys and defaults
	RulerStorage rulestore.Config `yaml:"ruler_storage"` // the ruler runs only when this is set
```

In `Load`, next to the other defaults (before decoding), add:

```go
	c.Ruler, c.RulerStorage = ruler.Defaults()
```

While there, delete the duplicated `c.Query.Pushdown = "on"` line. After the pushdown-mode check, add:

```go
	if c.RulerEnabled() {
		if err := ruler.Validate(c.Ruler, c.RulerStorage); err != nil {
			return nil, fmt.Errorf("ruler: %w", err)
		}
	}
```

and the method:

```go
// RulerEnabled reports whether ruler_storage is configured (Loki's rule for
// starting the ruler in the single binary).
func (c *Config) RulerEnabled() bool { return ruler.Enabled(c.RulerStorage) }
```

- [ ] **Step 5: Run the config tests**

Run: `go mod tidy && go test ./internal/config/ -count=1`
Expected: PASS. If the remote-write case fails with a different message, check `pkg/ruler/config.go` `RemoteWriteConfig.Validate` at the pinned commit and assert on its text ("remote-write enabled but no clients are configured").

- [ ] **Step 6: Write the failing evaluator test**

Create `internal/ruler/evaluator_test.go`:

```go
package ruler_test

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ruler"
)

// overrides returns Loki's default per-tenant limits.
func overrides(t *testing.T) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("limits", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

// engine returns Loki's engine over the ClickHouse querier (Tier B).
func engine(t *testing.T, conn driver.Conn) ruler.Engine {
	return api.NewPlainQuerier(logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides(t), log.NewNopLogger()))
}

// writeLines stores n lines of {app="a"} for tenant, one second apart, ending
// a minute before now.
func writeLines(t *testing.T, conn driver.Conn, tenant string, n int) {
	t.Helper()
	var es []logproto.Entry
	for i := 0; i < n; i++ {
		es = append(es, logproto.Entry{Timestamp: time.Now().Add(-time.Minute - time.Duration(i)*time.Second), Line: "line"})
	}
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), tenant, []logproto.Stream{{Labels: `{app="a"}`, Entries: es}}, nil))
}

func TestEvaluator_InstantQueryAtNow(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	writeLines(t, conn, "t2", 1)
	ev := &ruler.Evaluator{Engine: engine(t, conn)}
	now := time.Now()

	for tenant, want := range map[string]float64{"t1": 3, "t2": 1} {
		res, err := ev.Eval(user.InjectOrgID(context.Background(), tenant), `sum by (app) (count_over_time({app="a"}[5m]))`, now)
		require.NoError(t, err)
		vec, ok := res.Data.(promql.Vector)
		require.True(t, ok, "got %T", res.Data)
		require.Len(t, vec, 1)
		require.Equal(t, want, vec[0].F, tenant)
		require.Equal(t, `{app="a"}`, vec[0].Metric.String())
		require.Equal(t, now.UnixMilli(), vec[0].T)
	}
}

func TestEvaluator_ParseError(t *testing.T) {
	conn, _ := chtest.New(t)
	ev := &ruler.Evaluator{Engine: engine(t, conn)}
	_, err := ev.Eval(user.InjectOrgID(context.Background(), "t1"), `sum(`, time.Now())
	require.Error(t, err)
}
```

- [ ] **Step 7: Run it to verify it fails**

Run: `go test ./internal/ruler/ -run TestEvaluator -count=1`
Expected: FAIL to compile with `undefined: ruler.Evaluator`.

- [ ] **Step 8: Implement `internal/ruler/evaluator.go`**

```go
package ruler

import (
	"context"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// Engine executes a LogQL query; *pushdown.Engine and api.NewPlainQuerier
// satisfy it (it has api.Querier's method set).
type Engine interface {
	Query(ctx context.Context, p logql.Params) logql.Query
}

// Evaluator implements Loki's pkg/ruler.Evaluator on lokihouse's engine, so
// rules get Tier A push-down, Tier B fallback and query-time delete filtering
// like every other query.
type Evaluator struct {
	Engine Engine
}

// Copyright (c) 2015-2026 Grafana Labs.
// Copied from grafana/loki@fbc3f077 pkg/ruler/evaluator_local.go (LocalEvaluator.Eval);
// modifications: runs Engine instead of *logql.QueryEngine; the per-evaluation
// info log line is dropped.
func (e *Evaluator) Eval(ctx context.Context, qs string, now time.Time) (*logqlmodel.Result, error) {
	params, err := logql.NewLiteralParams(
		qs,
		now,
		now,
		0,
		0,
		logproto.FORWARD,
		0,
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}

	res, err := e.Engine.Query(ctx, params).Exec(ctx)
	if err != nil {
		return nil, err
	}
	return &res, nil
}
```

- [ ] **Step 9: Run the package tests**

Run: `go test ./internal/ruler/ ./internal/config/ -count=1 && go vet ./internal/ruler/ ./internal/config/`
Expected: PASS.

- [ ] **Step 10: Run on the cluster**

Run: `make cluster-up && LOKIHOUSE_TEST_CH_ADDR=localhost:19001 LOKIHOUSE_TEST_CH_CLUSTER=lokihouse go test -count=1 ./internal/ruler/ ./internal/config/`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add go.mod go.sum internal/ruler/config.go internal/ruler/evaluator.go internal/ruler/evaluator_test.go internal/config/config.go internal/config/config_test.go
git commit -m "feat(ruler): ruler config (Loki's ruler/ruler_storage keys) and evaluator over the lokihouse engine"
```

---

### Task 2: Ruler service, rule storage and rules API

**Files:**
- Create: `internal/ruler/ruler.go`, `internal/ruler/ruler_test.go`, `internal/api/rules.go`
- Modify: `internal/api/api.go`, `cmd/lokihouse/main.go`, `lokihouse.example.yaml`, `docs/divergences.md`

**Interfaces:**
- Consumes: `ruler.Engine`, `ruler.Evaluator`, `ruler.Defaults`, `ruler.Validate`, `overrides(t)`, `engine(t, conn)`, `writeLines(t, conn, tenant, n)` (Task 1); `api.API`, `api.API.Router()` (existing).
- Produces:
  - `ruler.New(ctx context.Context, rc lokiruler.Config, sc rulestore.Config, e Engine, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*rulerbase.Ruler, *rulerbase.API, error)`. The ruler is a dskit service, not yet started.
  - `api.API.Rules *rulerbase.API`. When non-nil, `Router()` serves the ruler routes.
  - Test helpers in package `ruler_test` (reused by Task 3): `startRuler(t, dir, eng, mut func(*lokiruler.Config)) *harness`, `(*harness).do(t, method, path, tenant, body) (int, string)`, `(*harness).stop()`, `(*harness).rules(t, tenant) promRules`, `(*harness).alerts(t, tenant) promAlerts`, `(*harness).evaluated(t, tenant, health string) bool`, and the constant `group` (recording rule `app:lines:count`, alert `Lines` with `severity: page` and a templated `summary`).

Loki registers these routes in `pkg/loki/modules.go` `initRuler`, each behind the tenant middleware:

| Method | Path | Handler |
|---|---|---|
| GET | `/prometheus/api/v1/rules` | `PrometheusRules` |
| GET | `/prometheus/api/v1/alerts` | `PrometheusAlerts` |
| GET | `/loki/api/v1/rules`, `/api/prom/rules` | `ListRules` |
| GET | `…/rules/{namespace}` | `ListRules` |
| POST | `…/rules/{namespace}` | `CreateRuleGroup` |
| DELETE | `…/rules/{namespace}` | `DeleteNamespace` |
| GET | `…/rules/{namespace}/{groupName}` | `GetRuleGroup` |
| DELETE | `…/rules/{namespace}/{groupName}` | `DeleteRuleGroup` |

The handlers read `{namespace}`/`{groupName}` with gorilla's `mux.Vars`. Our wrapper copies `r.PathValue(...)` into `gmux.SetURLVars`, as Phase 3 did for `detected_field/{name}/values`.

- [ ] **Step 1: Write the failing API and evaluation tests**

Create `internal/ruler/ruler_test.go`:

```go
package ruler_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ruler"
)

// group is a rule group evaluated every 100ms over {app="a"}.
const group = `name: g1
interval: 100ms
rules:
  - record: app:lines:count
    expr: sum by (app) (count_over_time({app="a"}[5m]))
  - alert: Lines
    expr: sum by (app) (count_over_time({app="a"}[5m])) > 0
    labels: {severity: page}
    annotations: {summary: '{{ $labels.app }} has {{ $value }} lines'}
`

type harness struct {
	srv  *httptest.Server
	r    *rulerbase.Ruler
	once sync.Once
}

// startRuler starts Loki's ruler over eng with filesystem rule storage in dir,
// polling and evaluating every 100ms, behind lokihouse's API router. mut, when
// set, adjusts the ruler config. The ruler is stopped when the test ends; only
// one ruler may run at a time (Loki keeps its remote-write registry global).
func startRuler(t *testing.T, dir string, eng ruler.Engine, mut func(*lokiruler.Config)) *harness {
	t.Helper()
	rc, sc := ruler.Defaults()
	rc.PollInterval = 100 * time.Millisecond
	rc.EvaluationInterval = 100 * time.Millisecond
	rc.RulePath = filepath.Join(dir, "rules-temp")
	rc.WAL.Dir = filepath.Join(dir, "ruler-wal")
	sc.Backend = "filesystem"
	sc.Filesystem.Directory = filepath.Join(dir, "rules")
	if mut != nil {
		mut(&rc)
	}
	require.NoError(t, ruler.Validate(rc, sc))
	r, rapi, err := ruler.New(context.Background(), rc, sc, eng, overrides(t), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	a := &api.API{Queries: eng, Rules: rapi, AuthEnabled: true}
	h := &harness{srv: httptest.NewServer(a.Router()), r: r}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.once.Do(func() {
		h.srv.Close()
		_ = services.StopAndAwaitTerminated(context.Background(), h.r)
	})
}

func (h *harness) do(t *testing.T, method, path, tenant, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-Scope-OrgID", tenant)
	if body != "" {
		req.Header.Set("Content-Type", "application/yaml")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

type promRules struct {
	Data struct {
		Groups []struct {
			Name, File string
			Rules      []struct {
				Name, Type, Health, LastError, State, LastEvaluation string
			}
		}
	}
}

type promAlerts struct {
	Data struct {
		Alerts []struct {
			Labels      map[string]string
			Annotations map[string]string
			State       string
			Value       string
		}
	}
}

func (h *harness) rules(t *testing.T, tenant string) promRules {
	t.Helper()
	code, body := h.do(t, http.MethodGet, "/prometheus/api/v1/rules", tenant, "")
	require.Equal(t, http.StatusOK, code, body)
	var r promRules
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	return r
}

func (h *harness) alerts(t *testing.T, tenant string) promAlerts {
	t.Helper()
	code, body := h.do(t, http.MethodGet, "/prometheus/api/v1/alerts", tenant, "")
	require.Equal(t, http.StatusOK, code, body)
	var a promAlerts
	require.NoError(t, json.Unmarshal([]byte(body), &a))
	return a
}

// evaluated reports whether every rule of tenant has been evaluated with the
// given health.
func (h *harness) evaluated(t *testing.T, tenant, health string) bool {
	r := h.rules(t, tenant)
	n := 0
	for _, g := range r.Data.Groups {
		for _, rule := range g.Rules {
			if rule.Health != health || strings.HasPrefix(rule.LastEvaluation, "0001-") {
				return false
			}
			n++
		}
	}
	return n > 0
}

func TestRules_CRUD(t *testing.T) {
	conn, _ := chtest.New(t)
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)

	code, body := h.do(t, http.MethodPost, "/loki/api/v1/rules/team%20a", "t1", group)
	require.Equal(t, http.StatusAccepted, code, body)
	require.Contains(t, body, `"status":"success"`)

	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, "team a:")
	require.Contains(t, body, "name: g1")

	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, "record: app:lines:count")

	code, _ = h.do(t, http.MethodGet, "/api/prom/rules/team%20a", "t1", "")
	require.Equal(t, http.StatusOK, code) // legacy path

	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusAccepted, code)
	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules/team%20a/g1", "t1", "")
	require.Equal(t, http.StatusNotFound, code, body)

	code, _ = h.do(t, http.MethodPost, "/loki/api/v1/rules/team%20a", "t1", group)
	require.Equal(t, http.StatusAccepted, code)
	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/team%20a", "t1", "")
	require.Equal(t, http.StatusAccepted, code)
	code, body = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusNotFound, code)
	require.Contains(t, body, "no rule groups found")
}

func TestRules_InvalidGroupRejected(t *testing.T) {
	conn, _ := chtest.New(t)
	dir := t.TempDir()
	h := startRuler(t, dir, engine(t, conn), nil)
	for name, tc := range map[string]struct{ body, want string }{
		"syntax error":        {"name: g\nrules:\n  - record: x\n    expr: sum(\n", "could not parse expression for record 'x'"},
		"bad record name":     {"name: g\nrules:\n  - record: 'not a name'\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "invalid recording rule name"},
		"for on record":       {"name: g\nrules:\n  - record: x\n    for: 1m\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "invalid field 'for' in recording rule"},
		"not yaml":            {"{{{", "unable to decoded rule group"},
		"empty group name":    {"rules:\n  - record: x\n    expr: sum(count_over_time({app=\"a\"}[1m]))\n", "Groupname must not be empty"},
	} {
		t.Run(name, func(t *testing.T) {
			code, body := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", tc.body)
			require.Equal(t, http.StatusBadRequest, code, body)
			require.Contains(t, body, tc.want)
		})
	}
	code, _ := h.do(t, http.MethodGet, "/loki/api/v1/rules", "t1", "")
	require.Equal(t, http.StatusNotFound, code, "nothing may be stored")

	// A namespace that escapes the storage directory never reaches the store.
	code, _ = h.do(t, http.MethodPost, "/loki/api/v1/rules/%2E%2E", "t1", group)
	require.NotEqual(t, http.StatusAccepted, code)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.Contains(t, []string{"rules", "rules-temp", "ruler-wal"}, e.Name())
	}
}

func TestRules_TenantIsolation(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t2", 3) // only t2 has data
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)

	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	code, _ = h.do(t, http.MethodGet, "/loki/api/v1/rules", "t2", "")
	require.Equal(t, http.StatusNotFound, code, "t2 must not see t1's groups")
	code, _ = h.do(t, http.MethodDelete, "/loki/api/v1/rules/ns", "t2", "")
	require.Equal(t, http.StatusNotFound, code, "t2 must not delete t1's namespace")
	code, _ = h.do(t, http.MethodGet, "/loki/api/v1/rules/ns/g1", "t1", "")
	require.Equal(t, http.StatusOK, code)

	// t1's rules only see t1's (empty) data: evaluated, healthy, not firing.
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
	require.Empty(t, h.alerts(t, "t1").Data.Alerts)
	require.Empty(t, h.rules(t, "t2").Data.Groups)
}

func TestRules_EvaluatesAgainstClickHouse(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { return len(h.alerts(t, "t1").Data.Alerts) == 1 }, 30*time.Second, 100*time.Millisecond)
	a := h.alerts(t, "t1").Data.Alerts[0]
	require.Equal(t, "firing", a.State)
	require.Equal(t, map[string]string{"alertname": "Lines", "app": "a", "severity": "page"}, a.Labels)
	require.Equal(t, "a has 3 lines", a.Annotations["summary"])
	require.Equal(t, "3e+00", a.Value)
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
}

// flakyEngine fails every query while fail is set.
type flakyEngine struct {
	ruler.Engine
	fail atomic.Bool
}

func (f *flakyEngine) Query(ctx context.Context, p logql.Params) logql.Query {
	if f.fail.Load() {
		return failingQuery{}
	}
	return f.Engine.Query(ctx, p)
}

type failingQuery struct{}

func (failingQuery) Exec(context.Context) (logqlmodel.Result, error) {
	return logqlmodel.Result{}, errors.New("clickhouse: connection refused")
}

func TestRules_EvaluationErrorReported(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	eng := &flakyEngine{Engine: engine(t, conn)}
	eng.fail.Store(true)
	h := startRuler(t, t.TempDir(), eng, nil)
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "err") }, 30*time.Second, 100*time.Millisecond)
	for _, r := range h.rules(t, "t1").Data.Groups[0].Rules {
		require.Contains(t, r.LastError, "connection refused")
	}

	eng.fail.Store(false)
	require.Eventually(t, func() bool { return h.evaluated(t, "t1", "ok") }, 30*time.Second, 100*time.Millisecond)
}

func TestRules_SurviveRestart(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	dir := t.TempDir()

	h1 := startRuler(t, dir, engine(t, conn), nil)
	code, _ := h1.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)
	h1.stop()

	h2 := startRuler(t, dir, engine(t, conn), nil)
	code, body := h2.do(t, http.MethodGet, "/loki/api/v1/rules/ns/g1", "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Eventually(t, func() bool { return len(h2.alerts(t, "t1").Data.Alerts) == 1 }, 30*time.Second, 100*time.Millisecond)
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/ruler/ -run TestRules -count=1`
Expected: FAIL to compile with `undefined: ruler.New` and `unknown field Rules in struct literal of type api.API`.

- [ ] **Step 3: Implement `internal/ruler/ruler.go`**

```go
package ruler

import (
	"context"
	"hash/fnv"

	"github.com/go-kit/log"
	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
)

// New builds Loki's ruler over e, as pkg/loki/modules.go initRulerStorage,
// initRuleEvaluator (local mode) and initRuler do, plus Loki's ruler HTTP API.
// The ruler is a dskit service the caller starts and stops. Only one ruler may
// run per process: Loki keeps its remote-write registry in a package variable.
func New(ctx context.Context, rc lokiruler.Config, sc rulestore.Config, e Engine, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*rulerbase.Ruler, *rulerbase.API, error) {
	store, err := rulerbase.NewRuleStore(ctx, sc, limits, lokiruler.GroupLoader{}, logger)
	if err != nil {
		return nil, nil, err
	}
	ev := lokiruler.NewEvaluatorWithJitter(&Evaluator{Engine: e}, rc.Evaluation.MaxJitter, fnv.New32a(), logger)
	r, err := lokiruler.NewRuler(rc, ev, reg, logger, store, limits, "lokihouse")
	if err != nil {
		return nil, nil, err
	}
	return r, rulerbase.NewAPI(r, store, logger), nil
}
```

- [ ] **Step 4: Implement `internal/api/rules.go`**

```go
package api

import (
	"net/http"

	gmux "github.com/gorilla/mux"
	"github.com/grafana/loki/v3/pkg/util/constants"
)

// ruleRoutes registers Loki's ruler API as pkg/loki/modules.go initRuler does:
// the Prometheus-compatible rules and alerts endpoints, the Loki rule-group
// routes and the legacy /api/prom ones, all tenant-wrapped. Loki's handlers
// read {namespace} and {groupName} with gorilla's mux.Vars.
func (a *API) ruleRoutes(mux *http.ServeMux) {
	h := func(f http.HandlerFunc) http.Handler {
		return a.tenant(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			vars := map[string]string{}
			for _, k := range []string{"namespace", "groupName"} {
				if v := r.PathValue(k); v != "" {
					vars[k] = v
				}
			}
			f(w, gmux.SetURLVars(r, vars))
		}))
	}
	mux.Handle("GET "+constants.PathPrometheusRules, h(a.Rules.PrometheusRules))
	mux.Handle("GET "+constants.PathPrometheusAlerts, h(a.Rules.PrometheusAlerts))
	for _, p := range []struct{ rules, ns, group string }{
		{constants.PathLokiRules, constants.PathLokiRulesNamespace, constants.PathLokiRulesNamespaceGroup},
		{constants.PathPromRules, constants.PathPromRulesNamespace, constants.PathPromRulesNamespaceGroup},
	} {
		mux.Handle("GET "+p.rules, h(a.Rules.ListRules))
		mux.Handle("GET "+p.ns, h(a.Rules.ListRules))
		mux.Handle("POST "+p.ns, h(a.Rules.CreateRuleGroup))
		mux.Handle("DELETE "+p.ns, h(a.Rules.DeleteNamespace))
		mux.Handle("GET "+p.group, h(a.Rules.GetRuleGroup))
		mux.Handle("DELETE "+p.group, h(a.Rules.DeleteRuleGroup))
	}
}
```

In `internal/api/api.go`, add the import `rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"`, add the field after `Tail http.Handler`:

```go
	Rules       *rulerbase.API // Loki's ruler API; nil = ruler disabled
```

and in `Router()`, after the `if a.Tail != nil { … }` block:

```go
	if a.Rules != nil {
		a.ruleRoutes(mux)
	}
```

- [ ] **Step 5: Run the ruler tests**

Run: `go test ./internal/ruler/ ./internal/api/ -count=1`
Expected: PASS. If `TestRules_InvalidGroupRejected/not yaml` returns a different message, check `ErrBadRuleGroup` in `pkg/ruler/base/api.go` at the pinned commit: it is `"unable to decoded rule group"` (Loki's typo).

- [ ] **Step 6: Wire the ruler into `cmd/lokihouse/main.go`**

Add the imports `"github.com/grafana/dskit/services"`, `"github.com/prometheus/client_golang/prometheus"` and `lhruler "github.com/bzed/lokihouse/internal/ruler"`. Before `addr := …`, add:

```go
	// Loki's ruler, when ruler_storage is configured (Loki's single-binary rule).
	if cfg.RulerEnabled() {
		r, rapi, err := lhruler.New(ctx, cfg.Ruler, cfg.RulerStorage, a.Queries, limits, log.With(logger, "component", "ruler"), prometheus.NewRegistry())
		if err != nil {
			return fmt.Errorf("ruler: %w", err)
		}
		if err := services.StartAndAwaitRunning(ctx, r); err != nil {
			return fmt.Errorf("ruler: %w", err)
		}
		a.Rules = rapi
	}
```

Run: `go build ./... && go vet ./...`
Expected: no output.

- [ ] **Step 7: Smoke-test the binary**

```bash
d=$(mktemp -d /tmp/lhruler.XXXXXX)
cat > $d/c.yaml <<EOF
auth_enabled: false
server: {http_listen_port: 13101}
clickhouse: {database: lh_smoke}
ruler: {poll_interval: 1s, rule_path: $d/rules-temp}
ruler_storage: {backend: filesystem, filesystem: {dir: $d/rules}}
EOF
go build -o $d/lokihouse ./cmd/lokihouse && ($d/lokihouse -config.file $d/c.yaml > $d/log 2>&1 &)
sleep 3
printf 'name: g\nrules:\n  - record: x:count\n    expr: sum(count_over_time({app="a"}[1m]))\n' | curl -s -w ' %{http_code}\n' --data-binary @- -H 'Content-Type: application/yaml' localhost:13101/loki/api/v1/rules/ns
curl -s localhost:13101/loki/api/v1/rules
pkill -f "$d/lokihouse"; rm -rf "$d"
```

Expected: `{"status":"success","data":null,"errorType":"","error":""} 202`, then the group as YAML under `ns:`. The log has no `panic`. Afterwards drop the smoke database: `clickhouse client -q 'DROP DATABASE IF EXISTS lh_smoke'`.

- [ ] **Step 8: Document config and divergences**

Append to `lokihouse.example.yaml`:

```yaml
# Loki's ruler (alerting and recording rules), with Loki's own config keys.
# It runs only when ruler_storage is set, and on one lokihouse instance only
# (no ruler ring; see docs/divergences.md).
# ruler:
#   rule_path: /var/lib/lokihouse/rules-temp   # default: rules-temp (relative)
#   alertmanager_url: http://alertmanager:9093
#   external_url: http://grafana:3000
#   remote_write:
#     enabled: true
#     clients:
#       prom: {url: http://prometheus:9090/api/v1/write}
#   wal: {dir: /var/lib/lokihouse/ruler-wal}   # wiped at every start
# ruler_storage:
#   backend: filesystem                        # local = read-only rule files
#   filesystem: {dir: /var/lib/lokihouse/rules}
```

Append to `docs/divergences.md`:

```markdown
## Ruler (Phase 4)
- **No ruler ring (sharding):** Loki distributes rule groups over a ring of rulers; lokihouse has no ring. `ruler.enable_sharding: true` and `ruler.evaluation.mode: remote` are rejected at startup, and `/ruler/ring` is not served. Every lokihouse instance with `ruler_storage` configured evaluates *all* rule groups, so configure it on exactly one instance; otherwise alerts and recording-rule samples are duplicated.
- **`ruler.rule_path` default:** `rules-temp` relative to the working directory instead of Loki's `/rules` (Loki's single binary uses `<common.path_prefix>/rules-temp`; lokihouse has no `path_prefix`).
- **Namespaces containing `/`:** Loki's router (gorilla mux on the decoded path) splits `/loki/api/v1/rules/a%2Fb` into namespace `a` and group `b`; lokihouse's `ServeMux` keeps `a/b` as one namespace. Other escaped characters (spaces, `%`, unicode) behave the same in both.
- **Ruler metrics:** collected in a private registry and not exposed (lokihouse has no `/metrics` endpoint before Phase 5).
```

- [ ] **Step 9: Run everything on the single node and the cluster**

Run: `make test && make test-cluster`
Expected: all packages `ok`.

- [ ] **Step 10: Commit**

```bash
git add internal/ruler/ruler.go internal/ruler/ruler_test.go internal/api/rules.go internal/api/api.go cmd/lokihouse/main.go lokihouse.example.yaml docs/divergences.md
git commit -m "feat(ruler): Loki ruler service, filesystem rule storage and rules API"
```

---

### Task 3: Recording-rule remote write, alert notification, WAL-dir guard

**Files:**
- Create: `internal/ruler/notify_test.go`, `internal/ruler/config_test.go`
- Modify: `internal/ruler/config.go`, `docs/divergences.md`

**Interfaces:**
- Consumes: `startRuler`, `(*harness).do`, `(*harness).alerts`, `group`, `engine`, `writeLines` (Tasks 1–2); `ruler.Validate`, `ruler.Defaults` (Task 1).
- Produces: `ruler.Validate` additionally rejects unsafe `ruler.wal.dir` values when `ruler.remote_write.enabled` is true (unexported `checkWALDir`, `within`).

Remote write and notification need no new production code: `ruler.New` already runs Loki's WAL registry and notifier. This task pins their behaviour on lokihouse's engine with real receivers, and adds the WAL-dir guard. The guard is needed because `pkg/ruler/registry.go` `newWALRegistry` runs `os.RemoveAll(config.WAL.Dir)` at every startup when remote write is enabled.

- [ ] **Step 1: Write the failing WAL-guard test**

Create `internal/ruler/config_test.go`:

```go
package ruler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate_WALDir(t *testing.T) {
	base := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		wal, rules, rulePath string
		ok                   bool
	}{
		"sibling dirs":            {wal: filepath.Join(base, "ruler-wal"), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "rules-temp"), ok: true},
		"default relative":        {wal: "ruler-wal", rules: filepath.Join(base, "rules"), rulePath: "rules-temp", ok: true},
		"empty":                   {wal: "", rules: filepath.Join(base, "rules"), rulePath: "rules-temp"},
		"working directory":       {wal: ".", rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"filesystem root":         {wal: "/", rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"parent of cwd":           {wal: filepath.Dir(cwd), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"parent of rule storage":  {wal: base, rules: filepath.Join(base, "rules"), rulePath: filepath.Join(t.TempDir(), "t")},
		"same as rule_path":       {wal: filepath.Join(base, "x"), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "x")},
	} {
		t.Run(name, func(t *testing.T) {
			rc, sc := Defaults()
			rc.RemoteWrite.Enabled = false
			rc.WAL.Dir, rc.RulePath = tc.wal, tc.rulePath
			sc.Backend, sc.Filesystem.Directory = "filesystem", tc.rules
			require.NoError(t, checkWALDir(rc, sc), "remote write disabled: Loki never wipes the WAL dir")

			rc.RemoteWrite.Enabled = true
			err := checkWALDir(rc, sc)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "ruler.wal.dir")
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/ruler/ -run TestValidate_WALDir -count=1`
Expected: FAIL to compile with `undefined: checkWALDir`.

- [ ] **Step 3: Implement the guard in `internal/ruler/config.go`**

Add the imports `"os"`, `"path/filepath"`, `"strings"`. At the end of `Validate`, replace `return nil` with `return checkWALDir(rc, sc)`, and add:

```go
// checkWALDir rejects ruler.wal.dir values that Loki's startup wipe
// (os.RemoveAll in pkg/ruler/registry.go newWALRegistry, run whenever remote
// write is enabled) would turn into data loss: empty, or a directory holding
// the working directory, the rule storage or rule_path.
func checkWALDir(rc lokiruler.Config, sc rulestore.Config) error {
	if !rc.RemoteWrite.Enabled {
		return nil
	}
	if rc.WAL.Dir == "" {
		return errors.New("ruler.wal.dir must be set when remote_write is enabled")
	}
	wal, err := filepath.Abs(rc.WAL.Dir)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	for _, p := range []string{cwd, sc.Filesystem.Directory, sc.Local.Directory, rc.RulePath} {
		if p == "" {
			continue
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return err
		}
		if within(abs, wal) {
			return fmt.Errorf("ruler.wal.dir %q contains %q, and Loki's ruler wipes the WAL directory at startup", rc.WAL.Dir, p)
		}
	}
	return nil
}

// within reports whether path is dir or lies below it.
func within(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
```

- [ ] **Step 4: Run the guard test**

Run: `go test ./internal/ruler/ -run TestValidate_WALDir -count=1 && go test ./internal/config/ -count=1`
Expected: PASS (the Task 1 config tests still pass: `ruler-wal` relative to the package directory contains none of the other paths).

- [ ] **Step 5: Write the remote-write and Alertmanager tests**

Create `internal/ruler/notify_test.go`:

```go
package ruler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v4"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// rwSink is a Prometheus remote-write receiver. The first failFirst requests
// get 503 (Prometheus retries 5xx).
type rwSink struct {
	*httptest.Server
	mu        sync.Mutex
	failFirst int
	requests  int
	samples   map[string]float64 // series labels -> latest value
	tenants   map[string]bool    // X-Scope-OrgID headers seen
}

func newRWSink(t *testing.T, failFirst int) *rwSink {
	s := &rwSink{failFirst: failFirst, samples: map[string]float64{}, tenants: map[string]bool{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.requests++
		if s.requests <= s.failFirst {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.tenants[r.Header.Get("X-Scope-OrgID")] = true
		for _, ts := range req.Timeseries {
			if n := len(ts.Samples); n > 0 {
				s.samples[seriesKey(ts.Labels)] = ts.Samples[n-1].Value
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(s.Close)
	return s
}

func seriesKey(ls []prompb.Label) string {
	m := map[string]string{}
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return fmt.Sprint(m)
}

func (s *rwSink) sample(key string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.samples[key]
	return v, ok
}

// withRemoteWrite enables Loki's ruler remote write to url, decoding the
// client from YAML exactly as config.Load does (so Prometheus defaults apply).
func withRemoteWrite(t *testing.T, url string) func(*lokiruler.Config) {
	return func(rc *lokiruler.Config) {
		src := fmt.Sprintf("enabled: true\nclients:\n  sink: {url: %q, queue_config: {batch_send_deadline: 100ms, min_backoff: 10ms, max_backoff: 100ms}}\n", url+"/api/v1/write")
		require.NoError(t, yaml.Unmarshal([]byte(src), &rc.RemoteWrite))
	}
}

func TestRemoteWrite_RecordingRuleSamples(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	sink := newRWSink(t, 0)
	h := startRuler(t, t.TempDir(), engine(t, conn), withRemoteWrite(t, sink.URL))
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	key := fmt.Sprint(map[string]string{"__name__": "app:lines:count", "app": "a"})
	require.Eventually(t, func() bool { _, ok := sink.sample(key); return ok }, 60*time.Second, 100*time.Millisecond)
	v, _ := sink.sample(key)
	require.Equal(t, 3.0, v)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Equal(t, map[string]bool{"t1": true}, sink.tenants, "Loki's add_org_id_header default sends the rule's tenant")
}

func TestRemoteWrite_RetriesAfterReceiverError(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	sink := newRWSink(t, 3)
	h := startRuler(t, t.TempDir(), engine(t, conn), withRemoteWrite(t, sink.URL))
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	key := fmt.Sprint(map[string]string{"__name__": "app:lines:count", "app": "a"})
	require.Eventually(t, func() bool { _, ok := sink.sample(key); return ok }, 60*time.Second, 100*time.Millisecond)
}

type amAlert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generatorURL"`
}

func TestAlertmanager_ReceivesFiringAlert(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	var mu sync.Mutex
	var got []amAlert
	am := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var as []amAlert
		if err := json.NewDecoder(r.Body).Decode(&as); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		got = append(got, as...)
		mu.Unlock()
	}))
	t.Cleanup(am.Close)

	h := startRuler(t, t.TempDir(), engine(t, conn), func(rc *lokiruler.Config) { rc.AlertmanagerURL = am.URL })
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) > 0 }, 60*time.Second, 100*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]string{"alertname": "Lines", "app": "a", "severity": "page"}, got[0].Labels)
	require.Equal(t, "a has 3 lines", got[0].Annotations["summary"])
}
```

- [ ] **Step 6: Run them**

Run: `go test ./internal/ruler/ -count=1 -v -run 'TestRemoteWrite|TestAlertmanager' 2>&1 | tail -20`
Expected: PASS. If a remote-write test times out, read the ruler log with `log.NewLogfmtLogger(os.Stderr)` in `startRuler` (temporarily). Look for `errNotReady` (the tenant WAL is not ready yet; it clears within a few evaluations) or a queue-manager error naming the URL.

- [ ] **Step 7: Record the guard as a divergence**

Append to the `## Ruler (Phase 4)` section of `docs/divergences.md`:

```markdown
- **Ruler WAL directory guard:** with remote write enabled, Loki wipes `ruler.wal.dir` (`os.RemoveAll`) at every start. lokihouse refuses to start when that directory is empty or contains the working directory, the rule storage directory or `rule_path`; Loki would delete them.
```

- [ ] **Step 8: Run everything on the single node and the cluster**

Run: `make test && make test-cluster`
Expected: all packages `ok`.

- [ ] **Step 9: Commit**

```bash
git add internal/ruler/config.go internal/ruler/config_test.go internal/ruler/notify_test.go docs/divergences.md
git commit -m "feat(ruler): guard the ruler WAL dir; pin remote write and Alertmanager notification"
```

---

### Task 4: Ruler oracle, Phase 4 status and verification

**Files:**
- Create: `test/oracle/ruler_test.go`, `docs/phase4-status.md`
- Modify: `test/oracle/loki.go`, `test/oracle/lokihouse.go`, `test/oracle/oracle_test.go`

**Interfaces:**
- Consumes: `Compare`, `CompareMut`, `PushBoth`, `Get` (existing oracle helpers); lokihouse's ruler config keys (Tasks 1–3).
- Produces: `StartLoki(t *testing.T, extra string) string` and `StartLokihouse(t *testing.T, extra string) string`, where `extra` is YAML appended to the generated config. Also `newRulerSinks`, `rulerYAML` and `rulerOracle(t, loki, house string, ls, hs *rulerSinks)`.

Facts checked against the pinned Loki binary while planning:
- With `common.storage.filesystem.rules_directory` set, Loki's ruler storage is the read-only `local` backend, and `POST /loki/api/v1/rules/…` fails. An explicit `ruler_storage: {backend: filesystem, filesystem: {dir: …}}` (no `rules_directory`) is writable.
- With `poll_interval: 1s` and a group `interval: 1s`, Loki evaluates new groups within about 2 s. It remote-writes with `X-Scope-OrgID: <tenant>` and reports `health`/`lastError`/`state`/`alerts` in `/prometheus/api/v1/rules`.
- A recording rule whose expression is a log query (`{app="api"}`) is *accepted*: Loki's `ValidateGroups` only parses it. Its evaluation then fails with `rule result is not a vector or scalar`.

- [ ] **Step 1: Give the harness an `extra` config parameter**

In `test/oracle/loki.go`:
- Change the signature to `func StartLoki(t *testing.T, extra string) string`.
- After the two `strings.ReplaceAll` lines, add `cfg += extra`.
- In `lokiConfigTemplate`, change `filesystem: {chunks_directory: ${DIR}/chunks, rules_directory: ${DIR}/rules}` to `filesystem: {chunks_directory: ${DIR}/chunks}`. The `rules_directory` only selected Loki's read-only rule store.

In `test/oracle/lokihouse.go`, change the signature to `func StartLokihouse(t *testing.T, extra string) string` and append `extra` to the generated config: change the `fmt.Sprintf(\`auth_enabled: …\`, …)` call to end with `) + extra`.

- [ ] **Step 2: Write the ruler oracle**

Create `test/oracle/ruler_test.go`:

```go
//go:build oracle

package oracle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage/remote"
)

// rulerSinks receive one store's recording-rule samples (Prometheus remote
// write) and alert notifications (Alertmanager API v2).
type rulerSinks struct {
	rw, am  *httptest.Server
	mu      sync.Mutex
	samples map[string]float64 // series labels -> latest value
	tenants map[string]bool    // X-Scope-OrgID headers seen on remote write
	alerts  map[string]amAlert // alert labels -> latest notification
}

type amAlert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generatorURL"`
}

func newRulerSinks(t *testing.T) *rulerSinks {
	s := &rulerSinks{samples: map[string]float64{}, tenants: map[string]bool{}, alerts: map[string]amAlert{}}
	s.rw = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.tenants[r.Header.Get("X-Scope-OrgID")] = true
		for _, ts := range req.Timeseries {
			m := map[string]string{}
			for _, l := range ts.Labels {
				m[l.Name] = l.Value
			}
			if n := len(ts.Samples); n > 0 {
				s.samples[fmt.Sprint(m)] = ts.Samples[n-1].Value
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.am = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var as []amAlert
		if err := json.NewDecoder(r.Body).Decode(&as); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, a := range as {
			s.alerts[fmt.Sprint(a.Labels)] = a
		}
	}))
	t.Cleanup(func() { s.rw.Close(); s.am.Close() })
	return s
}

// snapshot returns the sink state as JSON for Compare.
func (s *rulerSinks) snapshot() (samples, tenants, alerts []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	samples, _ = json.Marshal(s.samples)
	tenants, _ = json.Marshal(s.tenants)
	alerts, _ = json.Marshal(s.alerts)
	return
}

func (s *rulerSinks) counts() (series, alerts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.samples), len(s.alerts)
}

// rulerYAML is the ruler config both stores get (Loki's keys; lokihouse reads
// the same ones), with its own directories and sinks.
func rulerYAML(dir string, s *rulerSinks) string {
	return fmt.Sprintf(`
ruler:
  poll_interval: 1s
  evaluation_interval: 1s
  rule_path: %[1]s/rules-temp
  external_url: http://grafana.oracle
  alertmanager_url: %[2]s
  remote_write:
    enabled: true
    clients:
      oracle: {url: "%[3]s/api/v1/write", queue_config: {batch_send_deadline: 1s}}
  wal: {dir: %[1]s/ruler-wal}
ruler_storage:
  backend: filesystem
  filesystem: {dir: %[1]s/rules}
`, dir, s.am.URL, s.rw.URL)
}

// oracleGroup covers recording rules (plain, parser, bytes), a firing alert
// with templated annotations, a pending alert and an inactive one, over the
// tenant-a fixture (all of it lies inside [now-3h, now]).
const oracleGroup = `name: oracle
interval: 1s
rules:
  - record: app:lines:count
    expr: sum by (app) (count_over_time({app=~".+"}[3h]))
  - record: api:status:count
    expr: sum by (status) (count_over_time({app="api"} | json [3h]))
  - record: env:bytes
    expr: sum by (env) (bytes_over_time({app=~".+"}[3h]))
  - alert: ManyLines
    expr: sum by (app) (count_over_time({app=~".+"}[3h])) > 100
    labels: {severity: page}
    annotations: {summary: '{{ $labels.app }} has {{ $value }} lines'}
  - alert: ManyLinesPending
    expr: sum by (app) (count_over_time({app=~".+"}[3h])) > 100
    for: 1h
  - alert: Never
    expr: sum(count_over_time({app="api"}[3h])) > 1e9
`

// brokenGroup is accepted by Loki (the expression parses) but every
// evaluation fails: a log query is not a vector.
const brokenGroup = `name: broken
interval: 1s
rules:
  - record: logs:raw
    expr: '{app="api"}'
`

const tmpGroup = `name: tmp
interval: 1m
rules:
  - record: tmp:count
    expr: sum(count_over_time({app="web"}[1m]))
`

// send performs one ruler API call and returns status and body.
func send(t *testing.T, method, u, tenant, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, u, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", tenant)
	if body != "" {
		req.Header.Set("Content-Type", "application/yaml")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, u, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// normRules drops the evaluation timestamps and durations (each store runs its
// own schedule) and orders each rule's alerts by labels.
func normRules(v any) {
	switch t := v.(type) {
	case map[string]any:
		for _, k := range []string{"lastEvaluation", "evaluationTime", "activeAt"} {
			delete(t, k)
		}
		if as, ok := t["alerts"].([]any); ok {
			sort.SliceStable(as, func(i, j int) bool {
				return fmt.Sprint(as[i].(map[string]any)["labels"]) < fmt.Sprint(as[j].(map[string]any)["labels"])
			})
		}
		for _, c := range t {
			normRules(c)
		}
	case []any:
		for _, c := range t {
			normRules(c)
		}
	}
}

// waitRulesEvaluated polls until every rule of tenant has been evaluated and
// every rule outside group "broken" is healthy (the first evaluations can fail
// while the remote-write WAL starts).
func waitRulesEvaluated(t *testing.T, u, tenant string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var r struct {
			Data struct {
				Groups []struct {
					Name  string
					Rules []struct{ Health, LastEvaluation string }
				}
			}
		}
		_ = json.Unmarshal(Get(t, u+"/prometheus/api/v1/rules", tenant), &r)
		n := 0
		for _, g := range r.Data.Groups {
			for _, rule := range g.Rules {
				if !strings.HasPrefix(rule.LastEvaluation, "0001-") && (g.Name == "broken" || rule.Health == "ok") {
					n++
				}
			}
		}
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d of %d rules evaluated after %s", u, n, want, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func rulerOracle(t *testing.T, loki, house string, ls, hs *rulerSinks) {
	both := func(name, method, path, tenant, body string) {
		t.Helper()
		lc, lb := send(t, method, loki+path, tenant, body)
		hc, hb := send(t, method, house+path, tenant, body)
		if lc != hc {
			t.Errorf("%s: status loki=%d lokihouse=%d\nloki: %s\nlokihouse: %s", name, lc, hc, lb, hb)
			return
		}
		Compare(t, name, lb, hb)
	}

	// Rule-group CRUD, Loki's validation errors and tenant isolation.
	both("rules_create", "POST", "/loki/api/v1/rules/oracle%20ns", "tenant-a", oracleGroup)
	both("rules_create_broken", "POST", "/loki/api/v1/rules/oracle%20ns", "tenant-a", brokenGroup)
	both("rules_bad_expr", "POST", "/loki/api/v1/rules/bad", "tenant-a", "name: g\nrules:\n  - record: x\n    expr: sum(\n")
	both("rules_bad_record_name", "POST", "/loki/api/v1/rules/bad", "tenant-a", "name: g\nrules:\n  - record: 'not a name'\n    expr: sum(count_over_time({app=\"api\"}[1m]))\n")
	both("rules_bad_yaml", "POST", "/loki/api/v1/rules/bad", "tenant-a", "{{{")
	both("rules_list", "GET", "/loki/api/v1/rules", "tenant-a", "")
	both("rules_list_ns", "GET", "/loki/api/v1/rules/oracle%20ns", "tenant-a", "")
	both("rules_get_group", "GET", "/loki/api/v1/rules/oracle%20ns/oracle", "tenant-a", "")
	both("rules_get_missing", "GET", "/loki/api/v1/rules/oracle%20ns/missing", "tenant-a", "")
	both("rules_legacy_list", "GET", "/api/prom/rules", "tenant-a", "")
	both("rules_other_tenant", "GET", "/loki/api/v1/rules", "tenant-b", "")
	both("rules_tmp_create", "POST", "/loki/api/v1/rules/tmp", "tenant-a", tmpGroup)
	both("rules_tmp_delete_group", "DELETE", "/loki/api/v1/rules/tmp/tmp", "tenant-a", "")
	both("rules_tmp_get_deleted", "GET", "/loki/api/v1/rules/tmp/tmp", "tenant-a", "")
	both("rules_tmp_recreate", "POST", "/loki/api/v1/rules/tmp", "tenant-a", tmpGroup)
	both("rules_tmp_delete_ns", "DELETE", "/loki/api/v1/rules/tmp", "tenant-a", "")
	both("rules_tmp_list_deleted", "GET", "/loki/api/v1/rules/tmp", "tenant-a", "")

	// Evaluation state: 6 rules in "oracle" + 1 in "broken".
	for _, u := range []string{loki, house} {
		waitRulesEvaluated(t, u, "tenant-a", 7, 2*time.Minute)
	}
	for _, p := range []string{"/prometheus/api/v1/rules", "/prometheus/api/v1/alerts"} {
		CompareMut(t, "ruler"+p, Get(t, loki+p, "tenant-a"), Get(t, house+p, "tenant-a"), normRules)
	}

	// Recording-rule samples and Alertmanager notifications. The fixture has
	// 3 apps, 3 statuses and 2 envs: 8 series; ManyLines fires for 3 apps.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ln, la := ls.counts()
		hn, ha := hs.counts()
		if ln >= 8 && hn >= 8 && la >= 3 && ha >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sinks incomplete after 2m: loki %d series/%d alerts, lokihouse %d series/%d alerts", ln, la, hn, ha)
		}
		time.Sleep(500 * time.Millisecond)
	}
	lsamp, lten, lal := ls.snapshot()
	hsamp, hten, hal := hs.snapshot()
	Compare(t, "remote_write_samples", lsamp, hsamp)
	Compare(t, "remote_write_tenants", lten, hten)
	Compare(t, "alertmanager_alerts", lal, hal)
}
```

- [ ] **Step 3: Call it from `TestOracle`**

In `test/oracle/oracle_test.go`, replace the first two lines of `TestOracle` with:

```go
	lokiSinks, houseSinks := newRulerSinks(t), newRulerSinks(t)
	loki := StartLoki(t, rulerYAML(t.TempDir(), lokiSinks))
	house := StartLokihouse(t, rulerYAML(t.TempDir(), houseSinks))
```

and call the ruler oracle right after `phase3Endpoints(...)`, before `deletesOracle`:

```go
	// The ruler: rule CRUD, evaluation state, remote write and notifications
	// over tenant-a's fixture.
	rulerOracle(t, loki, house, lokiSinks, houseSinks)
```

- [ ] **Step 4: Run the oracle**

Run: `go vet -tags oracle ./test/oracle/ && make oracle`
Expected: `ok`. Any `rules_*`, `ruler/*`, `remote_write_*` or `alertmanager_alerts` diff is a real divergence. Fix it by routing through Loki's code (Global Constraints), or, if it is inherent, add it to `docs/divergences.md` and narrow the comparison with a mutator. Known flake: the `detected_labels` cases (see `docs/divergences.md`); re-run on that.

- [ ] **Step 5: Run all gates**

Run, in order: `make test`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`.
Expected: every one `ok`.

- [ ] **Step 6: Write `docs/phase4-status.md`**

Fill in the date and record the gate results from Step 5 exactly as they came out. Commit only when every gate is `ok`.

```markdown
# Phase 4 status

Phase 4 runs Loki's ruler inside lokihouse: rule groups in local filesystem
storage, Loki's rules API, alerting and recording rules evaluated with
lokihouse's engine (Tier A push-down, Tier B fallback, delete filtering),
Alertmanager notification and recording-rule remote write. Everything is
compared against real Loki (commit `fbc3f077`) in the oracle.

## Exit criteria

Run on <date>, on `main` after task 4.

| Gate | Command | Result |
| --- | --- | --- |
| Unit tests | `make test` | ok (all packages) |
| Cluster unit tests | `make test-cluster` | ok (4-node ClickHouse cluster) |
| Oracle (all push-down modes) | `make oracle-all` | ok (off / downstream / on) |
| Cluster oracle | `make oracle-cluster` | ok (Loki vs 4-node lokihouse) |

## Endpoints

All served by Loki's handlers (`pkg/ruler/base` `API`), registered as Loki's
`initRuler` does, when `ruler_storage` is configured.

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
```

- [ ] **Step 7: Commit**

```bash
git add test/oracle/ruler_test.go test/oracle/loki.go test/oracle/lokihouse.go test/oracle/oracle_test.go docs/phase4-status.md
git commit -m "test(oracle): ruler oracle; docs: phase 4 status"
```
