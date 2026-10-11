# Lokihouse Phase 5 (Hardening and Release) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make lokihouse a releasable service. It shuts down cleanly on signals and enforces lint, vulnerability and ≥ 85 % coverage gates. CI builds a hardened Debian trixie package with a systemd unit. It ships Markdown docs, a GitHub Pages site and a manpage. It enforces Loki's ingestion and query limits, answers multi-tenant queries, exposes Prometheus metrics and query stats, is benchmarked against Loki, and is checked for memory and goroutine leaks.

**Architecture:** The plan has two parts.
- **Part A (Tasks 1–8)** builds the safety net first:
  - an `AGENTS.md` contract and `make check` gates (gofmt/goimports, vet, golangci-lint, govulncheck, a coverage gate);
  - an `internal/server` package that owns the process lifecycle (signals, graceful HTTP shutdown, ruler and background-loop shutdown, sd_notify);
  - operator flags and config (`-version`, `-config.expand-env`, strict YAML, `password_file`, logging keys);
  - a lint-clean, 85 %-covered tree;
  - docs plus manpage, the Debian package, and GitHub Actions.
- **Part B (Tasks 9–14)** adds the spec's Phase 5 runtime hardening on top. Every Part B task already runs under the Part A gates.

All Loki behaviour still comes from Loki's code: the rate limiter strategy, limit checks, the multi-tenant querier, the stats context and metric names.

**Tech Stack:**
- Go 1.27.2 (`toolchain go1.27.2` in `go.mod`) and `github.com/grafana/loki/v3` @ `fbc3f0770b8c` (unchanged).
- New direct Go dependencies, all already in Loki's module graph at these versions: `github.com/coreos/go-systemd/v22` v22.7.0, `github.com/drone/envsubst` v1.0.3, `github.com/felixge/httpsnoop` (version from `go.sum`).
- Tools, run with `go run pkg@version` so CI and local runs match: golangci-lint v2.14.0, govulncheck v1.9.0.
- Docs: mkdocs 1.6.1 and mkdocs-material 9.7.7. Manpage: go-md2man (Debian/Ubuntu package).
- Packaging: Debian trixie, debhelper compat 13, systemd 257+ (`ImportCredential=`).
- ClickHouse 26.9.

**Spec:** `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md`:
- §6 rows "Multi-tenancy … `a|b` multi-tenant queries … 1 / 5", "Ingest rate/size limits … our own token bucket … 5 (rate)" and "Query stats in responses … Phase 5";
- §8 Phase 5 row "Hardening: limits/rate limiting, multi-tenant queries, query stats, timeouts, perf bench vs Loki, packaging";
- §5.1 "ClickHouse Cloud … Phase 5 verification", which is **deferred** (see "Deferred");
- §7 correctness strategy and the divergence register.

User requirements added for this phase (2026-10-11):
- Shutdown handling: signals, stop the ruler, stop the background loops.
- Proper Debian packaging for stable (trixie). The Go toolchain is downloaded with curl during the package build.
- `AGENTS.md` with safe and structured Go rules, enforced formatting and linting, and ≥ 85 % test coverage.
- GitHub Actions for test, build, format/lint checks and the Debian package.
- Markdown docs published as a GitHub Pages site with mkdocs, plus a manpage.
- systemd units with `DynamicUser` and hardening, and an example config.
- Hardening scope: limits and timeouts, multi-tenant queries, metrics and query stats, the performance benchmark against Loki, and memory/goroutine leak checks with pprof (valgrind does not apply to Go). The podman compose example from the spec row is **not** in scope.
- Debian: trixie, amd64 and arm64, release assets on tags.
- CI: unit tests, lint, govulncheck, coverage, deb and docs on every push and PR; the cluster tests and the oracle nightly and on manual dispatch.

## Global Constraints

All Phase 1–4 Global Constraints still apply (see the plans in `docs/superpowers/plans/`). In particular:
- Never bump the Loki commit.
- Reuse Loki's code instead of re-implementing it. Copied code keeps the attribution line `// Copied from grafana/loki@fbc3f077 <path>; modifications: <list>`.
- Every accepted difference from Loki goes into `docs/divergences.md` in the task that introduces it.

In addition:

- **Go toolchain:** `go.mod` keeps `go 1.27.1` and adds `toolchain go1.27.2`. govulncheck reports 11 reachable standard-library vulnerabilities in go1.27.1 (GO-2026-6599 … GO-2026-6617), all fixed in go1.27.2. Debian builds download exactly go1.27.2:
  - `go1.27.2.linux-amd64.tar.gz` SHA-256 `ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5`
  - `go1.27.2.linux-arm64.tar.gz` SHA-256 `94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8`
- **No build or scratch directories under `$HOME`.** Use `mktemp -d /tmp/…`, `/var/tmp`, or `debian/.build` inside the source tree. The Debian build sets `HOME`, `GOPATH`, `GOCACHE` and `GOMODCACHE` to `debian/.build/…`.
- **Pinned tool versions:** golangci-lint `v2.14.0`, govulncheck `v1.9.0`, mkdocs `1.6.1`, mkdocs-material `9.7.7`. Pin GitHub Actions by commit SHA with the tag in a comment:
  - `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`
  - `actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0`
  - `actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97 # v7.0.0`
  - `actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0`
  - `actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2`
  - `actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2`
  - `actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d # v6.0.0`
  - `actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9 # v5.0.0`
  - `actions/deploy-pages@368f82528645a54fb793d4d04e342629a3f51346 # v5.0.1`
  - `softprops/action-gh-release@efb35369e0ad2afab669f228072c1b0d510eae64 # v3.0.3`
- **Gates:** from the end of Task 4, `make check` passes at every commit: fmt-check, vet, lint, vuln, and the coverage gate at the current `COVERAGE_MIN`. From the end of Task 5, `COVERAGE_MIN` is 85 and never lowered.
- **Coverage definition:** statement coverage from `go test -race -covermode=atomic -coverpkg=<pkgs>` over every package in `go list ./...` except `internal/chstore/chtest` (a test helper) and `test/...` (oracle, build-tagged). It is computed by `scripts/coverage.sh`.
- **govulncheck:** zero findings, except IDs listed with a reason in `scripts/govulncheck-ignore.txt`. The only allowed entry is `GO-2026-5115`. It is a false positive: the advisory covers `github.com/grafana/loki/v3 < v3.6.4`, and the pinned pseudo-version `v3.0.0-20261010092436-fbc3f0770b8c` sorts below that. The commit is newer and already rejects namespace path traversal (`filepath.IsLocal` in `pkg/ruler/base/api.go` `parseNamespace`). `TestRules_PathTraversalRejected` (Task 5) proves it.
- **Config key names:** wherever Loki has a setting, lokihouse uses Loki's key, block and default:
  - `server.http_server_read_timeout`, `server.http_server_write_timeout`, `server.http_server_idle_timeout`, `server.graceful_shutdown_timeout`, `server.log_level`, `server.log_format`;
  - `querier.multi_tenant_queries_enabled`;
  - `limits_config.*`;
  - the `-config.file` and `-config.expand-env` flags.
- **Debian:** source format `3.0 (native)`, version `0.5.0`, architectures `amd64 arm64`, target `trixie`. Maintainer `Bernd Zeimetz <bernd@bzed.de>`. The package must install and run without a network. Only the *build* downloads Go and modules.
- **systemd:** `lokihouse.service` uses `DynamicUser=yes` and passes `systemd-analyze security --offline=true --threshold=15`. The threshold is 10× the displayed exposure level, so the level must be ≤ 1.5; the draft in Task 7 scores 1.1.
- **Shutdown order** (Task 2): stop accepting HTTP → drain in-flight requests (bounded by `graceful_shutdown_timeout`) → stop the ruler → stop the background loops → close ClickHouse. Signals are SIGINT and SIGTERM. A second signal kills the process immediately (default Go behaviour).
- Tests that need ClickHouse use `chtest.New` and must pass on the single node and on the 2×2 cluster (`make test`, `make test-cluster`). The oracle must pass in all push-down modes (`make oracle-all`) and on the cluster (`make oracle-cluster`) at the end of Tasks 9, 10 and 14.

## Review Focus

1. **Shutdown during a push.** SIGTERM arrives while a client is mid-upload to `/loki/api/v1/push`. The push still completes with 204 and its rows are in ClickHouse; new connections are refused. Owner: Task 2 (`TestRun_InFlightPushCompletes`).
2. **Long-lived tail connections.** A `/loki/api/v1/tail` WebSocket stays open and keeps delivering lines long after `server.http_server_write_timeout` (30 s by default). It is not cut when the timeout expires. Owner: Task 2 (`TestRun_TailOutlivesWriteTimeout`).
3. **Config typos.** A misspelled key (`http_listen_prot: 3100`) stops startup with an error that names the key, instead of being ignored. Owner: Task 3 (`TestLoad_UnknownKeyRejected`).
4. **Multi-tenant data leak.** With `X-Scope-OrgID: a|b`, a selector `{__tenant_id__="c"}` must return nothing from tenant `c`, and every series must carry the right `__tenant_id__`. With multi-tenant queries disabled, `a|b` must not reach any tenant's data. Owner: Task 10 (`TestMultiTenant_NoForeignTenant`, `TestMultiTenant_DisabledRejects`).
5. **Rate-limited push is all-or-nothing.** A push over the tenant's ingestion rate returns 429 with Loki's message and stores **no** rows (not even the valid streams). Other tenants are not affected. Owner: Task 9 (`TestRateLimit_RejectsWholePushAndIsolatesTenants`).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `AGENTS.md` (new), `CLAUDE.md` (new: `@AGENTS.md`) | Rules for safe, structured Go in this repo | 1 |
| `.golangci.yml` (new) | Linter and formatter set | 1, 4 |
| `scripts/coverage.sh`, `scripts/govulncheck.sh`, `scripts/govulncheck-ignore.txt` (new) | Coverage gate; govulncheck with an allowlist | 1 |
| `Makefile` (modify) | `fmt`, `fmt-check`, `vet`, `lint`, `vuln`, `cover`, `check`, version ldflags, `docs`, `man`, `deb`, `bench` | 1, 3, 6, 7, 12 |
| `go.mod`, `go.sum` (modify) | `toolchain go1.27.2`; new direct deps | 1, 2, 3, 11 |
| `internal/server/server.go`, `server_test.go` (new) | Wiring, lifecycle, graceful shutdown, sd_notify | 2, 9, 10, 11 |
| `cmd/lokihouse/main.go`, `main_test.go` | Flags, signals, exit codes, manpage drift test | 2, 3, 6 |
| `internal/config/config.go`, `config_test.go`, `docs_test.go` | Server and logging keys, strict YAML, env expansion, `password_file`, `querier` block, docs drift test | 2, 3, 6, 10 |
| `internal/logging/logging.go`, `logging_test.go` (new) | go-kit logger from `server.log_level`/`log_format` | 3 |
| `internal/api/misc.go` (modify) | buildinfo from `prometheus/common/version` | 3 |
| `internal/deletes/processor.go` (modify) | No error log on shutdown | 2 |
| `internal/ingest/distcopy/*_test.go` (new, copied from Loki) | Coverage of copied distributor code | 5 |
| assorted `*_test.go` | Coverage to ≥ 85 % | 5 |
| `README.md`, `mkdocs.yml`, `docs/index.md`, `docs/getting-started.md`, `docs/configuration.md`, `docs/operations.md`, `docs/api.md`, `docs/architecture.md`, `docs/development.md`, `docs/requirements.txt`, `docs/man/lokihouse.1.md` (new) | Documentation, site and manpage | 6 (extended by 9–13) |
| `debian/*` (new) | Debian native package, systemd unit, packaged config, smoke test | 7 |
| `.github/workflows/ci.yml`, `deb.yml`, `nightly.yml`, `release.yml`, `pages.yml`, `.github/dependabot.yml` (new); `test/cluster/up.sh` (modify) | CI | 8 |
| `internal/ingest/ratelimit.go` (new), `internal/ingest/handler.go`, `internal/ingest/distcopy/process.go` (modify) | Ingestion rate limit; Prepare returns sizes | 9 |
| `internal/api/limits.go`, `limits_test.go` (new), `internal/api/query.go` (modify) | Loki query limits on query/query_range | 9 |
| `internal/multitenant/querier.go`, `querier_test.go` (new) | Loki's MultiTenantQuerier (logs, samples, labels, series) | 10 |
| `internal/pushdown/engine.go` (modify) | Multi-tenant queries go to Tier B | 10 |
| `internal/api/api.go`, `metadata.go` (modify) | `Meta` querier for labels and series; `/metrics`; instrumentation | 10, 11 |
| `internal/api/instrument.go` (new) | Loki's HTTP request metrics | 11 |
| `internal/chstore/stats.go` (new), `chstore` and `pushdown/tiera.go` query call sites (modify) | ClickHouse progress → Loki stats | 11 |
| `internal/ruler/ruler.go` (modify) | Metrics namespace `loki` | 11 |
| `test/oracle/bench_test.go` (new), `docs/benchmark.md` (new) | Benchmark against Loki | 12 |
| `test/oracle/oracle_test.go`, `test/oracle/multitenant_test.go` (new) | Multi-tenant oracle | 10 |
| `internal/*/main_test.go` (goleak), `test/oracle/soak_test.go`, `internal/api/pprof_test.go` (new); `server.register_instrumentation` | Leak checks: goleak, pprof endpoints, soak test | 13 |
| `docs/phase5-status.md` (new), `docs/divergences.md`, `docs/roadmap.md` (new) | Status, divergences, deferred work | 9–14 |

---

# Part A: quality gates, lifecycle and release engineering

### Task 1: Quality gates and AGENTS.md

**Files:**
- Create: `AGENTS.md`, `CLAUDE.md`, `.golangci.yml`, `scripts/coverage.sh`, `scripts/govulncheck.sh`, `scripts/govulncheck-ignore.txt`
- Modify: `Makefile`, `go.mod`, `.gitignore`

**Interfaces:**
- Produces these make targets: `make fmt` (rewrites), `make fmt-check`, `make vet`, `make lint`, `make vuln`, `make cover` (honours `COVERAGE_MIN`) and `make check` (all of them). Later tasks and CI call only these targets.
- Produces `scripts/coverage.sh`. Env: `COVERAGE_MIN` (default 85), `COVERAGE_OUT` (default `coverage.out`). Exit code 1 when below the minimum.

- [ ] **Step 1: Pin the toolchain**

Run (from the repo root):
```bash
go mod edit -toolchain=go1.27.2
go version   # GOTOOLCHAIN=auto downloads go1.27.2 on first use
```
Expected: `go version go1.27.2 linux/amd64`. Commit `go.mod` with this task.

- [ ] **Step 2: Write `AGENTS.md`**

```markdown
# AGENTS.md: rules for working on lokihouse

These rules apply to every change, by humans and by coding agents. `make check`
enforces the mechanical ones; reviewers enforce the rest.

## Before you push

- `make check` passes: gofmt/goimports, `go vet`, golangci-lint (`.golangci.yml`),
  govulncheck, unit tests with the race detector, and total coverage ≥ 85 %.
- ClickHouse-backed tests need a server:
  `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`.
- Changes to query, ingest, ruler or SQL code also pass `make test-cluster`
  and the oracle (`make oracle-all`, `make oracle-cluster`).

## Loki compatibility

- lokihouse imports Loki at one pinned commit (`Makefile` `LOKI_COMMIT`). Never
  bump it in passing.
- Reuse Loki's code; do not re-implement Loki behaviour. Code copied from Loki
  starts with `// Copied from grafana/loki@<commit> <path>; modifications: <list>`.
- Every observable difference from Loki is recorded in `docs/divergences.md`
  with its reason, in the same change.
- Wherever Loki has a configuration key, flag, metric name or error message,
  use Loki's exactly.

## Structure

- `cmd/lokihouse` only parses flags and signals; wiring and lifecycle live in
  `internal/server`. Packages under `internal/` own one responsibility each and
  do not import `cmd` or `test`.
- Keep files focused. A function that no longer fits on a screen is a sign to
  split it.
- No package-level mutable state except Prometheus collectors and values Loki
  forces on us (document those where they are declared).
- No new third-party dependency unless it is already in Loki's module graph at
  the version Loki uses, or the change explains why it is needed.

## Safe Go

- **Errors:** handle every error. Wrap with context (`fmt.Errorf("open %s: %w", p, err)`);
  compare with `errors.Is`/`errors.As`, never by string. Ignoring an error needs
  `_ =` plus a comment saying why it is safe. Close errors of anything written
  to (files, ClickHouse batches, HTTP response bodies being written) are checked.
- **No panics** in request, ingest or background paths. `panic` is only for
  programmer errors at init time.
- **Contexts:** every function that does I/O takes a `context.Context` first and
  passes it on. HTTP requests are built with `http.NewRequestWithContext`.
  Goroutines stop when their context ends and are waited for on shutdown.
- **Concurrency:** shared state is guarded by a mutex or atomics and documented.
  Tests run with `-race`. Never call `t.Parallel()` in ruler tests (Loki keeps
  ruler state in package variables).
- **Untrusted input** (HTTP bodies, headers, query strings, rule files, config):
  bound sizes, validate before use, never build SQL by string concatenation
  of user input (use ClickHouse parameters), never build file paths from user input
  without `filepath.IsLocal` or an equivalent check.
- **Integer conversions** are range-checked (`gosec` G115). **Files** are created
  with `0o600`/`0o750` unless a wider mode is required and commented.
- **Secrets** never appear in logs, errors or metrics labels. Config supports
  `password_file` for credentials.
- **Timeouts:** every network server and client has explicit timeouts.

## Tests

- New behaviour comes with tests written first (red, then green).
- Total statement coverage stays ≥ 85 % (`make cover`); a change must not lower
  it. Test behaviour through public APIs; avoid testing private helpers in
  isolation unless they hold real logic.
- Tests are deterministic: no sleeps to wait for state; poll with
  `require.Eventually` and a generous timeout.
- Tests never write outside `t.TempDir()` and never use `$HOME`.

## Lint suppressions

`//nolint:<linter> // <reason>` only, always with the specific linter and a
reason (`nolintlint` enforces this). Prefer fixing the code.

## Commits

One logical change per commit, conventional prefixes (`feat:`, `fix:`,
`test:`, `docs:`, `build:`, `ci:`, `refactor:`), with `make check` passing.
```

- [ ] **Step 3: Write `CLAUDE.md`**

```markdown
@AGENTS.md
```

- [ ] **Step 4: Write `.golangci.yml`**

```yaml
version: "2"
run:
  build-tags: [oracle, bench]
  timeout: 15m
linters:
  default: standard # errcheck, govet, ineffassign, staticcheck, unused
  enable:
    - bodyclose
    - contextcheck
    - copyloopvar
    - errorlint
    - gocritic
    - gosec
    - misspell
    - nilerr
    - noctx
    - nolintlint
    - revive
    - unconvert
  settings:
    errcheck:
      exclude-functions:
        # go-kit logger errors are not actionable (Loki ignores them as well).
        - (github.com/go-kit/log.Logger).Log
    nolintlint:
      require-explanation: true
      require-specific: true
    revive:
      rules:
        - name: blank-imports
        - name: context-as-argument
        - name: context-keys-type
        - name: dot-imports
        - name: error-return
        - name: error-strings
        - name: errorf
        - name: exported
        - name: increment-decrement
        - name: indent-error-flow
        - name: package-comments
        - name: range
        - name: receiver-naming
        - name: redefines-builtin-id
        - name: superfluous-else
        - name: time-naming
        - name: unexported-return
        - name: unreachable-code
        - name: var-naming
  exclusions:
    generated: strict
    presets: [std-error-handling]
    rules:
      # Tests: math/rand for reproducible fixtures, requests without context are fine.
      - path: _test\.go
        linters: [gosec, noctx]
      # The oracle harness launches Loki, writes temp files and fetches URLs it builds.
      - path: ^test/
        linters: [gosec, noctx]
      # Copied verbatim from Loki's distributor; keep the diff to upstream minimal.
      - path: ^internal/ingest/distcopy/
        linters: [gosec, revive, gocritic]
formatters:
  enable: [gofmt, goimports]
  settings:
    goimports:
      local-prefixes: [github.com/bzed/lokihouse]
```

- [ ] **Step 5: Write `scripts/coverage.sh`**

```sh
#!/bin/sh
# Total statement coverage gate (AGENTS.md: >= 85 %).
# Env: COVERAGE_MIN (default 85), COVERAGE_OUT (default coverage.out).
set -eu
min=${COVERAGE_MIN:-85}
out=${COVERAGE_OUT:-coverage.out}
pkgs=$(go list ./... | grep -v -e '/internal/chstore/chtest$' -e '/test/')
coverpkg=$(printf '%s\n' $pkgs | paste -sd, -)
# shellcheck disable=SC2086 # word splitting of the package list is intended
go test -race -count=1 -covermode=atomic -coverpkg="$coverpkg" -coverprofile="$out" $pkgs
total=$(go tool cover -func="$out" | awk '/^total:/ { sub("%", "", $3); print $3 }')
echo "total coverage: ${total}% (minimum ${min}%)"
if ! awk -v t="$total" -v m="$min" 'BEGIN { exit !(t + 0 >= m + 0) }'; then
	echo "coverage ${total}% is below the minimum ${min}%" >&2
	exit 1
fi
```

- [ ] **Step 6: Write `scripts/govulncheck.sh` and its allowlist**

`scripts/govulncheck-ignore.txt`:
```text
# One OSV ID per line, each with a reason. Reviewed entries only.
# GO-2026-5115: false positive. The advisory covers grafana/loki/v3 < v3.6.4;
# the pinned pseudo-version v3.0.0-20261010092436-fbc3f0770b8c sorts below it,
# but the commit is newer and rejects namespace path traversal
# (pkg/ruler/base/api.go parseNamespace: filepath.IsLocal).
# Proven by internal/ruler TestRules_PathTraversalRejected.
GO-2026-5115
```

`scripts/govulncheck.sh`:
```sh
#!/bin/sh
# govulncheck with a reviewed allowlist (scripts/govulncheck-ignore.txt).
# Fails on any called vulnerability that is not allowlisted.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ignore=$(grep -v -e '^#' -e '^$' "$here/govulncheck-ignore.txt" || true)
json=$(mktemp /tmp/govulncheck.XXXXXX)
trap 'rm -f "$json"' EXIT
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 -format json ./... > "$json"
# A finding whose first trace frame has a function is a called (reachable) one.
found=$(python3 - "$json" <<'EOF'
import json, sys
ids = set()
dec = json.JSONDecoder()
buf = open(sys.argv[1]).read()
i = 0
while i < len(buf):
    while i < len(buf) and buf[i].isspace():
        i += 1
    if i >= len(buf):
        break
    obj, i = dec.raw_decode(buf, i)
    f = obj.get("finding")
    if f and f.get("trace") and f["trace"][0].get("function"):
        ids.add(f["osv"])
print("\n".join(sorted(ids)))
EOF
)
bad=0
for id in $found; do
	if printf '%s\n' "$ignore" | grep -qx "$id"; then
		echo "allowlisted: $id"
	else
		echo "VULNERABLE: $id (see https://pkg.go.dev/vuln/$id)" >&2
		bad=1
	fi
done
exit $bad
```

Run `chmod +x scripts/*.sh`.

- [ ] **Step 7: Add the Makefile targets**

Add to `Makefile` below the existing `build`/`test` targets. Keep every existing target.
```make
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
# Raised to 85 at the end of Task 5; never lowered afterwards.
COVERAGE_MIN ?= 75

.PHONY: fmt fmt-check vet lint vuln cover check
fmt:
	$(GOLANGCI_LINT) fmt
fmt-check:
	@out=$$($(GOLANGCI_LINT) fmt --diff); if [ -n "$$out" ]; then echo "$$out"; echo "run: make fmt" >&2; exit 1; fi
vet:
	go vet ./...
	go vet -tags oracle,bench ./test/...
lint:
	$(GOLANGCI_LINT) run ./...
vuln:
	sh scripts/govulncheck.sh
cover:
	COVERAGE_MIN=$(COVERAGE_MIN) sh scripts/coverage.sh
check: fmt-check vet lint vuln cover
```

Add `coverage.out` and `site/` to `.gitignore`.

- [ ] **Step 8: Verify the gates run**

Run each target and record the result. **Lint failures are expected** at this point; Task 4 fixes them.
```bash
make fmt-check; echo "fmt-check exit $?"
make vet && echo vet ok
make lint | tail -3            # expect ~160 issues (fixed in Task 4)
make vuln                      # expect: only "allowlisted: GO-2026-5115" (go1.27.2 fixes the stdlib ones)
COVERAGE_MIN=101 make cover; echo "exit $?"   # expect exit 2 from make (script exit 1): gate works
make cover                      # expect: total coverage ~75% (minimum 75%), exit 0
```
If `make vuln` reports a standard-library ID, the toolchain pin did not take effect: check `go version` again.

- [ ] **Step 9: Commit**

```bash
git add AGENTS.md CLAUDE.md .golangci.yml scripts Makefile go.mod .gitignore
git commit -m "build: AGENTS.md, lint/vuln/coverage gates, go1.27.2 toolchain"
```

---

### Task 2: Process lifecycle and graceful shutdown

**Files:**
- Create: `internal/server/server.go`, `internal/server/server_test.go`
- Modify: `cmd/lokihouse/main.go`, `internal/config/config.go`, `internal/config/config_test.go`, `internal/deletes/processor.go`, `internal/deletes/processor_test.go`, `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `config.Load(path string) (*config.Config, error)`, `lhruler.New(...)`, `deletes.Processor`, `api.API`, `pushdown.Engine` (existing).
- Produces:
  - `server.Options{Logger log.Logger; Listening func(net.Addr)}`.
  - `server.Run(ctx context.Context, cfg *config.Config, opts server.Options) error`. It blocks until `ctx` ends or the HTTP server fails, then shuts down in the order given in the Global Constraints.
  - New `config.Config.Server` fields (Loki keys and dskit defaults): `HTTPServerReadTimeout` (30s), `HTTPServerReadHeaderTimeout` (0), `HTTPServerWriteTimeout` (30s), `HTTPServerIdleTimeout` (120s), `GracefulShutdownTimeout` (30s), all `time.Duration`.
  - Log lines: `msg="stopped" component=<http|ruler|deletes|clickhouse>` for each stage, which tests assert on.

- [ ] **Step 1: Write the failing config test**

Append to `internal/config/config_test.go`:
```go
func TestLoad_ServerTimeoutDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	// dskit server defaults, as in Loki.
	require.Equal(t, 30*time.Second, c.Server.HTTPServerReadTimeout)
	require.Equal(t, time.Duration(0), c.Server.HTTPServerReadHeaderTimeout)
	require.Equal(t, 30*time.Second, c.Server.HTTPServerWriteTimeout)
	require.Equal(t, 120*time.Second, c.Server.HTTPServerIdleTimeout)
	require.Equal(t, 30*time.Second, c.Server.GracefulShutdownTimeout)
}

func TestLoad_ServerTimeouts(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`server:
  http_server_read_timeout: 1s
  http_server_read_header_timeout: 2s
  http_server_write_timeout: 3s
  http_server_idle_timeout: 4s
  graceful_shutdown_timeout: 5s
`), 0o600))
	c, err := Load(p)
	require.NoError(t, err)
	require.Equal(t, []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second},
		[]time.Duration{c.Server.HTTPServerReadTimeout, c.Server.HTTPServerReadHeaderTimeout, c.Server.HTTPServerWriteTimeout, c.Server.HTTPServerIdleTimeout, c.Server.GracefulShutdownTimeout})
}
```

Run: `go test ./internal/config/ -run TestLoad_Server -count=1`
Expected: compile FAIL (`c.Server.HTTPServerReadTimeout undefined`).

- [ ] **Step 2: Add the server keys**

In `internal/config/config.go`, replace the `Server` struct with:
```go
	Server struct {
		HTTPListenAddress           string        `yaml:"http_listen_address"`
		HTTPListenPort              int           `yaml:"http_listen_port"`
		HTTPServerReadTimeout       time.Duration `yaml:"http_server_read_timeout"`        // Loki/dskit default 30s
		HTTPServerReadHeaderTimeout time.Duration `yaml:"http_server_read_header_timeout"` // 0 = use the read timeout
		HTTPServerWriteTimeout      time.Duration `yaml:"http_server_write_timeout"`       // Loki/dskit default 30s
		HTTPServerIdleTimeout       time.Duration `yaml:"http_server_idle_timeout"`        // Loki/dskit default 120s
		GracefulShutdownTimeout     time.Duration `yaml:"graceful_shutdown_timeout"`       // Loki/dskit default 30s
	} `yaml:"server"`
```
In `Load`, next to `c.Server.HTTPListenPort = 3100`:
```go
	c.Server.HTTPServerReadTimeout = 30 * time.Second
	c.Server.HTTPServerWriteTimeout = 30 * time.Second
	c.Server.HTTPServerIdleTimeout = 120 * time.Second
	c.Server.GracefulShutdownTimeout = 30 * time.Second
```
Run: `go test ./internal/config/ -count=1`. Expected: PASS.

- [ ] **Step 3: Write the failing deletes-processor test**

On shutdown, the processor's in-flight `ProcessDue` fails with `context canceled`. That must not be logged as an error. Append to `internal/deletes/processor_test.go`. It reuses the file's `newProcessor` helper; read its signature first.
```go
func TestProcessor_RunQuietOnShutdown(t *testing.T) {
	p, _, _, _ := newProcessor(t, time.Hour, time.Hour, time.Now(), time.Now())
	var buf bytes.Buffer
	p.Logger = log.NewLogfmtLogger(&buf)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutdown already requested: ProcessDue fails with context.Canceled
	p.Run(ctx)
	require.Empty(t, buf.String())
}
```
Run: `go test ./internal/deletes/ -run TestProcessor_RunQuietOnShutdown -count=1`
Expected: FAIL (the error is logged). If it passes because `ProcessDue` happens not to touch ClickHouse before checking `ctx`, keep the test anyway and go on to Step 4.

- [ ] **Step 4: Silence the shutdown error**

In `internal/deletes/processor.go` `Run`, change the error branch to:
```go
		if err := p.ProcessDue(ctx, now()); err != nil && ctx.Err() == nil {
```
Run: `go test ./internal/deletes/ -count=1`. Expected: PASS.

- [ ] **Step 5: Write the failing server tests**

Add the dependency: `go get github.com/coreos/go-systemd/v22@v22.7.0`.

Create `internal/server/server_test.go`:
```go
package server_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/server"
)

const defaultServer = "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, graceful_shutdown_timeout: 10s}\n"

// testConfig writes a config for a fresh ClickHouse database (dropped when
// the test ends) with the given server block and extra top-level YAML.
func testConfig(t *testing.T, serverYAML, extra string) *config.Config {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_srv_" + hex.EncodeToString(b)
	topo := chtest.Topology()
	o := chtest.Options("default")
	y := "auth_enabled: true\n" + serverYAML + fmt.Sprintf(`clickhouse:
  addr: [%q]
  database: %s
  username: %q
  password: %q
  cluster: %q
  replicated: %t
`, o.Addr[0], db, o.Auth.Username, o.Auth.Password, topo.Cluster, topo.Replicated) + extra
	p := filepath.Join(t.TempDir(), "lokihouse.yaml")
	require.NoError(t, os.WriteFile(p, []byte(y), 0o600))
	cfg, err := config.Load(p)
	require.NoError(t, err)
	t.Cleanup(func() {
		admin, err := clickhouse.Open(chtest.Options("default"))
		if err == nil {
			_ = chstore.DropDatabase(context.Background(), admin, db, topo)
			_ = admin.Close()
		}
	})
	return cfg
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuffer) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type running struct {
	addr   string // host:port
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// start runs server.Run in the background and waits until it listens.
func start(t *testing.T, cfg *config.Config, logger log.Logger) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrc := make(chan net.Addr, 1)
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() {
		r.done <- server.Run(ctx, cfg, server.Options{Logger: logger, Listening: func(a net.Addr) { addrc <- a }})
	}()
	select {
	case a := <-addrc:
		r.addr = a.String()
	case err := <-r.done:
		t.Fatalf("Run returned before listening: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("server did not start")
	}
	t.Cleanup(func() { _ = r.stop() })
	return r
}

// stop cancels Run's context and returns Run's result (once).
func (r *running) stop() error {
	r.once.Do(func() {
		r.cancel()
		select {
		case r.err = <-r.done:
		case <-time.After(60 * time.Second):
			r.err = fmt.Errorf("Run did not return within 60s")
		}
	})
	return r.err
}

func pushBody(ts time.Time, line string) string {
	return fmt.Sprintf(`{"streams":[{"stream":{"app":"a"},"values":[[%q,%q]]}]}`, fmt.Sprint(ts.UnixNano()), line)
}

func push(t *testing.T, addr, tenant, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/loki/api/v1/push", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenant)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func countRows(t *testing.T, cfg *config.Config, tenant string) uint64 {
	t.Helper()
	conn, err := clickhouse.Open(chtest.Options(cfg.ClickHouse.Database))
	require.NoError(t, err)
	defer conn.Close()
	var n uint64
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count() FROM logs WHERE tenant = ?", tenant).Scan(&n))
	return n
}

func TestRun_PushQueryShutdown(t *testing.T) {
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(time.Now(), "hello")))
	require.Equal(t, uint64(1), countRows(t, cfg, "t1"))

	require.NoError(t, r.stop())
	_, err := net.DialTimeout("tcp", r.addr, time.Second)
	require.Error(t, err, "listener must be closed after Run returns")
}

func TestRun_InFlightPushCompletes(t *testing.T) {
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())

	body := pushBody(time.Now(), "in flight")
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+r.addr+"/loki/api/v1/push", pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", "t1")
	req.ContentLength = int64(len(body))
	type result struct {
		code int
		err  error
	}
	resc := make(chan result, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			resc <- result{err: err}
			return
		}
		defer resp.Body.Close()
		resc <- result{code: resp.StatusCode}
	}()
	_, err = pw.Write([]byte(body[:10])) // the request is now in flight
	require.NoError(t, err)

	stopped := make(chan error, 1)
	go func() { stopped <- r.stop() }()
	// Shutdown has begun once new connections are refused.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", r.addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
		}
		return err != nil
	}, 10*time.Second, 20*time.Millisecond)

	_, err = pw.Write([]byte(body[10:]))
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	res := <-resc
	require.NoError(t, res.err)
	require.Equal(t, http.StatusNoContent, res.code)
	require.NoError(t, <-stopped)
	require.Equal(t, uint64(1), countRows(t, cfg, "t1"))
}

func TestRun_StopsRulerAndLoops(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, defaultServer, fmt.Sprintf(`ruler:
  rule_path: %s
ruler_storage:
  backend: filesystem
  filesystem: {dir: %s}
`, filepath.Join(dir, "rules-temp"), filepath.Join(dir, "rules")))
	var logs syncBuffer
	r := start(t, cfg, log.NewLogfmtLogger(&logs))
	require.NoError(t, r.stop())
	out := logs.String()
	for _, c := range []string{"http", "ruler", "deletes", "clickhouse"} {
		require.Contains(t, out, `msg=stopped component=`+c)
	}
	// Order: HTTP before ruler before background loops before ClickHouse.
	require.Less(t, strings.Index(out, "msg=stopped component=http"), strings.Index(out, "msg=stopped component=ruler"))
	require.Less(t, strings.Index(out, "msg=stopped component=ruler"), strings.Index(out, "msg=stopped component=deletes"))
	require.Less(t, strings.Index(out, "msg=stopped component=deletes"), strings.Index(out, "msg=stopped component=clickhouse"))
}

func TestRun_ListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	cfg := testConfig(t, fmt.Sprintf("server: {http_listen_address: 127.0.0.1, http_listen_port: %d}\n", port), "")
	err = server.Run(t.Context(), cfg, server.Options{Logger: log.NewNopLogger()})
	require.ErrorContains(t, err, "address already in use")
}

func TestRun_ReadTimeoutClosesSlowClients(t *testing.T) {
	cfg := testConfig(t, "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, http_server_read_timeout: 300ms}\n", "")
	r := start(t, cfg, log.NewNopLogger())
	c, err := net.Dial("tcp", r.addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Write([]byte("GET /ready HTTP/1.1\r\nHost: x\r\n")) // headers never finish
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadAll(c) // the server closes the connection after the read timeout
	require.NoError(t, err, "connection must be closed by the server, not by our 5s deadline")
}

func TestRun_TailOutlivesWriteTimeout(t *testing.T) {
	cfg := testConfig(t, "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, http_server_write_timeout: 500ms}\n", "")
	r := start(t, cfg, log.NewNopLogger())
	u := url.URL{Scheme: "ws", Host: r.addr, Path: "/loki/api/v1/tail", RawQuery: url.Values{"query": {`{app="a"}`}}.Encode()}
	ws, resp, err := websocket.DefaultDialer.DialContext(t.Context(), u.String(), http.Header{"X-Scope-OrgID": {"t1"}})
	require.NoError(t, err)
	defer resp.Body.Close()
	defer ws.Close()

	time.Sleep(1500 * time.Millisecond) // 3× the write timeout
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(time.Now(), "late line")))
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(msg), "late line")
}

func TestRun_NotifiesSystemd(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "notify.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()
	t.Setenv("NOTIFY_SOCKET", sock)

	read := func() string {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(30*time.Second)))
		b := make([]byte, 256)
		n, err := conn.Read(b)
		require.NoError(t, err)
		return string(b[:n])
	}
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())
	require.Equal(t, "READY=1", read())
	require.NoError(t, r.stop())
	require.Equal(t, "STOPPING=1", read())
}
```

Run: `go test ./internal/server/ -count=1`
Expected: compile FAIL (`package server` not found).

- [ ] **Step 6: Implement `internal/server/server.go`**

Move the wiring from `cmd/lokihouse/main.go` `run` into this file; it is a pure move except for the lifecycle code.
```go
// Package server wires lokihouse's components together and runs them until
// the context ends, then shuts them down in order: HTTP (draining in-flight
// requests), the ruler, the background loops and the ClickHouse connection.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/tail"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/patterns"
	"github.com/bzed/lokihouse/internal/pushdown"
	lhruler "github.com/bzed/lokihouse/internal/ruler"
	lhtail "github.com/bzed/lokihouse/internal/tail"
)

// Options are the process-level hooks Run needs besides the config.
type Options struct {
	Logger log.Logger
	// Listening, when set, is called with the bound HTTP address before
	// systemd is notified (tests listen on port 0).
	Listening func(net.Addr)
}

// Run starts lokihouse and blocks until ctx is done or the HTTP server fails.
// In-flight requests run on a context that is not cancelled by ctx; they get
// server.graceful_shutdown_timeout to finish.
func Run(ctx context.Context, cfg *config.Config, opts Options) error {
	logger := opts.Logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	conn, err := openClickHouse(ctx, cfg)
	if err != nil {
		return err
	}
	closeConn := func() {
		if err := conn.Close(); err != nil {
			level.Warn(logger).Log("msg", "closing clickhouse", "err", err)
		}
		level.Info(logger).Log("msg", "stopped", "component", "clickhouse")
	}

	limits, err := validation.NewOverrides(cfg.LimitsConfig, nil)
	if err != nil {
		closeConn()
		return err
	}
	a, store, q := newAPI(cfg, conn, limits, logger)

	// Background loops run on their own context, cancelled after HTTP and the
	// ruler have stopped, and are waited for before ClickHouse is closed.
	bg, stopBG := context.WithCancel(context.WithoutCancel(ctx))
	var loops sync.WaitGroup
	loops.Go(func() {
		(&deletes.Processor{
			Store: store, Conn: conn, DB: cfg.ClickHouse.Database, Topo: cfg.Topology(),
			CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod,
			Interval:     cfg.Compactor.CompactionInterval,
			Logger:       logger,
		}).Run(bg)
	})
	stopLoops := func() {
		stopBG()
		loops.Wait()
		level.Info(logger).Log("msg", "stopped", "component", "deletes")
	}
	_ = q // used by later tasks (multi-tenant, metrics)

	var r *rulerService
	if cfg.RulerEnabled() {
		r, err = startRuler(ctx, cfg, a, limits, logger)
		if err != nil {
			stopLoops()
			closeConn()
			return err
		}
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Server.HTTPListenAddress, strconv.Itoa(cfg.Server.HTTPListenPort)))
	if err != nil {
		r.stop(logger)
		stopLoops()
		closeConn()
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           a.Router(),
		ReadTimeout:       cfg.Server.HTTPServerReadTimeout,
		ReadHeaderTimeout: cfg.Server.HTTPServerReadHeaderTimeout,
		WriteTimeout:      cfg.Server.HTTPServerWriteTimeout,
		IdleTimeout:       cfg.Server.HTTPServerIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	if opts.Listening != nil {
		opts.Listening(ln.Addr())
	}
	level.Info(logger).Log("msg", "lokihouse listening", "addr", ln.Addr().String())
	sdNotify(logger, daemon.SdNotifyReady)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served:
	}

	level.Info(logger).Log("msg", "shutting down")
	sdNotify(logger, daemon.SdNotifyStopping)
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.GracefulShutdownTimeout)
	defer cancel()
	var errs []error
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("http server: %w", serveErr))
	}
	if err := srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		_ = srv.Close() // force-close what did not drain; the error above is reported
	}
	level.Info(logger).Log("msg", "stopped", "component", "http")
	if err := r.stopCtx(sctx, logger); err != nil {
		errs = append(errs, err)
	}
	stopLoops()
	closeConn()
	return errors.Join(errs...)
}

func openClickHouse(ctx context.Context, cfg *config.Config) (driver.Conn, error) {
	opts := func(db string) *clickhouse.Options {
		return &clickhouse.Options{
			Addr:     cfg.ClickHouse.Addr,
			Auth:     clickhouse.Auth{Database: db, Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password},
			Settings: cfg.CHSettings(),
		}
	}
	admin, err := clickhouse.Open(opts("default"))
	if err != nil {
		return nil, err
	}
	if err := chstore.Migrate(ctx, admin, cfg.ClickHouse.Database, cfg.Topology()); err != nil {
		_ = admin.Close() // the migration error is the one to report
		return nil, err
	}
	if err := admin.Close(); err != nil {
		return nil, fmt.Errorf("close admin connection: %w", err)
	}
	return clickhouse.Open(opts(cfg.ClickHouse.Database))
}

// newAPI builds the HTTP API over conn (moved unchanged from cmd/lokihouse).
func newAPI(cfg *config.Config, conn driver.Conn, limits *validation.Overrides, logger log.Logger) (*api.API, *deletes.Store, *chstore.Querier) {
	store := &deletes.Store{Conn: conn}
	q := &chstore.Querier{Conn: conn, Deletes: store}
	a := &api.API{
		Queries: &pushdown.Engine{
			Mode:   pushdown.Mode(cfg.Query.Pushdown),
			TierB:  logql.NewEngine(logql.EngineOpts{}, q, limits, logger),
			TierA:  &pushdown.TierA{Conn: conn, Limits: limits, Deletes: store},
			Limits: limits,
			Logger: logger,
		},
		Q:           q,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: limits, Logger: logger},
		Limits:      limits,
		Patterns:    &patterns.Querier{Q: q, Limits: limits, Logger: logger},
		Deletes:     &deletes.Handler{Store: store, Limits: limits, MaxInterval: cfg.Compactor.DeleteMaxInterval, CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod},
		AuthEnabled: cfg.AuthEnabled,
	}
	// tail.MaxDuration is Loki's -querier.tail-max-duration default (1h).
	tq := tail.NewQuerier(&lhtail.Ingester{Q: q}, q, store, limits, time.Hour, tail.NewMetrics(nil), logger)
	// loghttp.ParseTailQuery reads the query via r.Form, which the net/http
	// server does not populate automatically; parse it first.
	a.Tail = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm() // ParseTailQuery reports a malformed query itself
		tq.TailHandler(w, r)
	})
	return a, store, q
}

// rulerService is Loki's ruler; a nil *rulerService is a disabled ruler.
type rulerService struct{ svc services.Service }

func startRuler(ctx context.Context, cfg *config.Config, a *api.API, limits *validation.Overrides, logger log.Logger) (*rulerService, error) {
	r, rapi, err := lhruler.New(ctx, cfg.Ruler, cfg.RulerStorage, a.Queries, limits, log.With(logger, "component", "ruler"), prometheus.NewRegistry())
	if err != nil {
		return nil, fmt.Errorf("ruler: %w", err)
	}
	if err := services.StartAndAwaitRunning(ctx, r); err != nil {
		return nil, fmt.Errorf("ruler: %w", err)
	}
	// Loki serves the rules API only with ruler.enable_api (default true).
	if cfg.Ruler.EnableAPI {
		a.Rules = rapi
	}
	return &rulerService{svc: r}, nil
}

func (r *rulerService) stop(logger log.Logger) { _ = r.stopCtx(context.Background(), logger) }

func (r *rulerService) stopCtx(ctx context.Context, logger log.Logger) error {
	if r == nil {
		return nil
	}
	if err := services.StopAndAwaitTerminated(ctx, r.svc); err != nil {
		return fmt.Errorf("stop ruler: %w", err)
	}
	level.Info(logger).Log("msg", "stopped", "component", "ruler")
	return nil
}

// sdNotify tells systemd about state changes (Type=notify); a no-op when
// NOTIFY_SOCKET is unset.
func sdNotify(logger log.Logger, state string) {
	if _, err := daemon.SdNotify(false, state); err != nil {
		level.Warn(logger).Log("msg", "sd_notify failed", "state", state, "err", err)
	}
}
```
`sync.WaitGroup.Go` exists since Go 1.25. Remove the `_ = q` line as soon as a later task uses `q`.

- [ ] **Step 7: Make `cmd/lokihouse/main.go` thin**

```go
// Command lokihouse serves Loki's HTTP API on top of ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/server"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is main without os.Exit, for tests. Exit codes: 0 ok, 1 runtime or
// config error, 2 usage error.
func run(args []string, _ io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("lokihouse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config.file", "lokihouse.yaml", "configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	logger := log.NewLogfmtLogger(log.NewSyncWriter(stderr))
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // restore default handling: a second signal kills the process
	}()
	if err := server.Run(ctx, cfg, server.Options{Logger: logger}); err != nil {
		level.Error(logger).Log("err", err)
		return 1
	}
	return 0
}
```

- [ ] **Step 8: Run the tests**

```bash
go build ./... && go test ./internal/server/ ./internal/config/ ./internal/deletes/ -count=1 -race
```
Expected: PASS. If `TestRun_TailOutlivesWriteTimeout` fails with a read error near 500 ms, the hijacked tail connection kept the server's write deadline. Gorilla clears deadlines only when `Upgrader.HandshakeTimeout == 0`. Fix it in `newAPI`'s tail wrapper, before calling `tq.TailHandler`:
```go
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Time{}) // the tail connection is long-lived; see TestRun_TailOutlivesWriteTimeout
		_ = rc.SetReadDeadline(time.Time{})
```
Then run the test again.

- [ ] **Step 9: Run on the cluster and commit**

```bash
make test && make test-cluster
git add cmd internal go.mod go.sum
git commit -m "feat(server): graceful shutdown on SIGINT/SIGTERM, server timeouts, sd_notify"
```

---

### Task 3: Operator flags and configuration

**Files:**
- Create: `internal/logging/logging.go`, `internal/logging/logging_test.go`, `cmd/lokihouse/main_test.go`
- Modify: `cmd/lokihouse/main.go`, `internal/config/config.go`, `internal/config/config_test.go`, `internal/server/server_test.go` (the `config.Load` call), `test/oracle/lokihouse.go` (only if it calls `config.Load`), `internal/api/misc.go`, `Makefile`, `lokihouse.example.yaml`, `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `server.Run`, `server.Options` (Task 2).
- Produces:
  - `config.Load(path string, expandEnv bool) (*config.Config, error)` (signature change). YAML is strict: unknown keys are an error.
  - `config.Config.ClickHouse.PasswordFile string` (`password_file`), mutually exclusive with `password`.
  - `config.Config.Server.LogLevel dskitlog.Level` (`log_level`, default `info`) and `Server.LogFormat string` (`log_format`, `logfmt`|`json`, default `logfmt`).
  - `logging.New(w io.Writer, lvl dskitlog.Level, format string) (log.Logger, error)`.
  - Flags: `-config.file` (default `lokihouse.yaml`), `-config.expand-env` (default false, Loki's semantics via `github.com/drone/envsubst`), `-version`.
  - Build info: `github.com/prometheus/common/version` variables set by `-ldflags` (Makefile `LDFLAGS`); `/loki/api/v1/status/buildinfo` reports them.

- [ ] **Step 1: Write the failing config tests**

Append to `internal/config/config_test.go`. Change the existing `Load(p)` calls in this file to `Load(p, false)`.
```go
func TestLoad_UnknownKeyRejected(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, key string }{
		"top level": {"auth_enabld: false\n", "auth_enabld"},
		"nested":    {"server:\n  http_listen_prot: 3100\n", "http_listen_prot"},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte(tc.yaml), 0o600))
			_, err := Load(p, false)
			require.ErrorContains(t, err, tc.key)
		})
	}
}

func TestLoad_ExpandEnv(t *testing.T) {
	t.Setenv("LH_TEST_DB", "fromenv")
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  database: ${LH_TEST_DB}\n  username: ${LH_TEST_USER:-reader}\n"), 0o600))

	c, err := Load(p, true)
	require.NoError(t, err)
	require.Equal(t, "fromenv", c.ClickHouse.Database)
	require.Equal(t, "reader", c.ClickHouse.Username)

	c, err = Load(p, false) // without the flag the text is taken literally, as in Loki
	require.NoError(t, err)
	require.Equal(t, "${LH_TEST_DB}", c.ClickHouse.Database)
}

func TestLoad_PasswordFile(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte("s3cret\n"), 0o600))
	p := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+pw+"\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "s3cret", c.ClickHouse.Password, "trailing newline is stripped")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password: x\n  password_file: "+pw+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password and password_file")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+filepath.Join(dir, "missing")+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password_file")
}

func TestLoad_Logging(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "info", c.Server.LogLevel.String())
	require.Equal(t, "logfmt", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_level: debug, log_format: json}\n"), 0o600))
	c, err = Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "debug", c.Server.LogLevel.String())
	require.Equal(t, "json", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_format: xml}\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "log_format")
}

func TestLoad_ExampleConfig(t *testing.T) {
	_, err := Load(filepath.Join("..", "..", "lokihouse.example.yaml"), false)
	require.NoError(t, err)
}
```
Run: `go test ./internal/config/ -count=1`. Expected: compile FAIL (`Load` takes 1 argument).

- [ ] **Step 2: Implement strict loading, env expansion, password_file and logging keys**

`go get github.com/drone/envsubst@v1.0.3`.

In `internal/config/config.go`:
- add the fields `PasswordFile string \`yaml:"password_file"\`` (to the `ClickHouse` struct), `LogLevel dskitlog.Level \`yaml:"log_level"\`` and `LogFormat string \`yaml:"log_format"\`` (to `Server`), importing `dskitlog "github.com/grafana/dskit/log"`;
- set defaults in `Load`: `c.Server.LogFormat = "logfmt"` and `_ = c.Server.LogLevel.Set("info")`. `"info"` is a valid level, so `Set` cannot fail here;
- replace the read and decode part of `Load`:
```go
// Load reads the YAML config at path. With expandEnv, ${VAR} and
// ${VAR:-default} are expanded first (Loki's -config.expand-env). Unknown
// keys are an error, as in Loki.
func Load(path string, expandEnv bool) (*Config, error) {
	// … defaults unchanged …
	b, err := os.ReadFile(path) //nolint:gosec // G304: the config path is the operator's -config.file
	if err != nil {
		return nil, err
	}
	if expandEnv {
		s, err := envsubst.EvalEnv(string(b))
		if err != nil {
			return nil, fmt.Errorf("expand environment variables in %s: %w", path, err)
		}
		b = []byte(s)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.loadPasswordFile(); err != nil {
		return nil, err
	}
	if c.Server.LogFormat != "logfmt" && c.Server.LogFormat != "json" {
		return nil, fmt.Errorf("server.log_format: invalid value %q (want logfmt or json)", c.Server.LogFormat)
	}
	// … existing validation unchanged …
}

func (c *Config) loadPasswordFile() error {
	f := c.ClickHouse.PasswordFile
	if f == "" {
		return nil
	}
	if c.ClickHouse.Password != "" {
		return errors.New("clickhouse: set only one of password and password_file")
	}
	b, err := os.ReadFile(f) //nolint:gosec // G304: path from the operator's config
	if err != nil {
		return fmt.Errorf("clickhouse.password_file: %w", err)
	}
	c.ClickHouse.Password = strings.TrimRight(string(b), "\r\n")
	return nil
}
```
Check the existing `Load` for an `io.EOF` case. An empty file must still load: the old code decoded with `dec.Decode(c)`, so keep the same EOF tolerance. If `go.yaml.in/yaml/v4` names the strict switch differently, use its equivalent. In v3/v4 it is `(*Decoder).KnownFields(bool)`. Test it with `TestLoad_UnknownKeyRejected`.

Update every caller to the new signature (`grep -rn "config.Load(" --include=*.go .` excluding `.cache`), passing `false`.

Run: `go test ./internal/config/ ./internal/server/ -count=1`. Expected: PASS.

- [ ] **Step 3: Write the failing logging test**

`internal/logging/logging_test.go`:
```go
package logging_test

import (
	"bytes"
	"testing"

	dskitlog "github.com/grafana/dskit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/logging"
)

func TestNew_LevelAndFormat(t *testing.T) {
	var lvl dskitlog.Level
	require.NoError(t, lvl.Set("warn"))
	var buf bytes.Buffer
	l, err := logging.New(&buf, lvl, "json")
	require.NoError(t, err)
	level.Info(l).Log("msg", "hidden")
	level.Warn(l).Log("msg", "shown")
	require.NotContains(t, buf.String(), "hidden")
	require.Contains(t, buf.String(), `"msg":"shown"`)

	_, err = logging.New(&buf, lvl, "xml")
	require.Error(t, err)
}
```
Run: `go test ./internal/logging/ -count=1`. Expected: compile FAIL.

- [ ] **Step 4: Implement `internal/logging/logging.go`**

```go
// Package logging builds lokihouse's go-kit logger from Loki's server.log_level
// and server.log_format keys.
package logging

import (
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dskitlog "github.com/grafana/dskit/log"
)

// New returns a goroutine-safe logger writing to w in format ("logfmt" or
// "json"), dropping messages below lvl, with a UTC timestamp and caller.
func New(w io.Writer, lvl dskitlog.Level, format string) (log.Logger, error) {
	sw := log.NewSyncWriter(w)
	var l log.Logger
	switch format {
	case "logfmt":
		l = log.NewLogfmtLogger(sw)
	case "json":
		l = log.NewJSONLogger(sw)
	default:
		return nil, fmt.Errorf("unknown log format %q (want logfmt or json)", format)
	}
	l = level.NewFilter(l, lvl.Option)
	return log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5)), nil
}
```
Run the test. Expected: PASS. If the caller depth is wrong, `caller=` points into go-kit. Check one log line by hand and adjust the depth: 5 is correct with `level.NewFilter`, but verify.

- [ ] **Step 5: Write the failing `main` tests**

`cmd/lokihouse/main_test.go`:
```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun_Version(t *testing.T) {
	var out, errb bytes.Buffer
	require.Equal(t, 0, run([]string{"-version"}, &out, &errb))
	require.Contains(t, out.String(), "lokihouse, version")
}

func TestRun_UsageError(t *testing.T) {
	var out, errb bytes.Buffer
	require.Equal(t, 2, run([]string{"-no-such-flag"}, &out, &errb))
	require.Contains(t, errb.String(), "no-such-flag")
}

func TestRun_ConfigError(t *testing.T) {
	var out, errb bytes.Buffer
	p := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(p, []byte("nonsense_key: 1\n"), 0o600))
	require.Equal(t, 1, run([]string{"-config.file", p}, &out, &errb))
	require.Contains(t, errb.String(), "nonsense_key")
}
```
Run: `go test ./cmd/lokihouse/ -count=1`. Expected: FAIL (`-version` is unknown).

- [ ] **Step 6: Implement the flags, logging and version**

In `cmd/lokihouse/main.go` `run`:
```go
	path := fs.String("config.file", "lokihouse.yaml", "configuration file to load")
	expandEnv := fs.Bool("config.expand-env", false, "expand ${VAR} and ${VAR:-default} in the configuration file")
	showVersion := fs.Bool("version", false, "print version information and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.Print("lokihouse"))
		return 0
	}
	cfg, err := config.Load(*path, *expandEnv)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
	logger, err := logging.New(stderr, cfg.Server.LogLevel, cfg.Server.LogFormat)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
```
(rename the unused `_ io.Writer` parameter to `stdout`; import `github.com/prometheus/common/version` and `internal/logging`).

In `internal/api/misc.go`, replace the `Version` variable and `buildInfo` with Loki's buildinfo fields taken from `prometheus/common/version`:
```go
func buildInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{ // a failed write means the client went away
		"version": version.Version, "revision": version.Revision, "branch": version.Branch,
		"buildUser": version.BuildUser, "buildDate": version.BuildDate, "goVersion": version.GoVersion,
	})
}
```

In the `Makefile`, add the ldflags and use them in `build`:
```make
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')
REVISION   ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null)
BRANCH     ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILD_DATE ?= $(shell date -u -d "@$${SOURCE_DATE_EPOCH:-$$(date +%s)}" +%Y-%m-%dT%H:%M:%SZ)
PV := github.com/prometheus/common/version
LDFLAGS := -X $(PV).Version=$(VERSION) -X $(PV).Revision=$(REVISION) -X $(PV).Branch=$(BRANCH) -X $(PV).BuildDate=$(BUILD_DATE) -X $(PV).BuildUser=lokihouse
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/lokihouse ./cmd/lokihouse
```

Add to `lokihouse.example.yaml` under `server:`, commented out:
```yaml
  # http_listen_address: 0.0.0.0
  # log_level: info                # debug | info | warn | error
  # log_format: logfmt             # logfmt | json
  # http_server_read_timeout: 30s
  # http_server_write_timeout: 30s # long queries need more (query_timeout is 1m)
  # http_server_idle_timeout: 120s
  # graceful_shutdown_timeout: 30s
```
and under `clickhouse:`:
```yaml
  # password_file: /run/credentials/lokihouse.service/lokihouse.clickhouse-password
```

- [ ] **Step 7: Run and commit**

```bash
go test ./cmd/... ./internal/config/ ./internal/logging/ ./internal/server/ ./internal/api/ -count=1
make build && ./bin/lokihouse -version
git add cmd internal Makefile lokihouse.example.yaml go.mod go.sum test
git commit -m "feat: -version, -config.expand-env, strict config, password_file, log level/format"
```

---

### Task 4: Lint-clean tree

**Files:**
- Modify: any Go file golangci-lint reports. The baseline is ~160 findings: revive 42 (27 `exported`, 9 `package-comments`, 6 `unused-parameter`, the last disabled by the rule list), gosec 38, errcheck 34, noctx 13, plus staticcheck, unused, gocritic and others.

**Interfaces:**
- Consumes: `make lint`, `make fmt` (Task 1).
- Produces: `make fmt-check vet lint vuln` all pass. Behaviour is unchanged: `make test` and the oracle stay green.

- [ ] **Step 1: Format**

```bash
make fmt && git diff --stat
make fmt-check
```
Expected: `fmt-check` passes. Commit: `git commit -am "style: goimports with local prefix"`.

- [ ] **Step 2: Get the findings list**

```bash
make lint > /tmp/lint.txt 2>&1; grep -oE '\([a-z]+\)$' /tmp/lint.txt | sort | uniq -c | sort -rn
```

- [ ] **Step 3: Fix the findings, one linter per commit, using these rules**

| Linter / rule | Fix |
|---|---|
| errcheck | Handle the error. In tests use `require.NoError`. `defer x.Close()` on read-only resources is allowed by the `std-error-handling` preset. On writers, close explicitly and check the error. |
| revive `exported`, `package-comments` | Write a real doc comment starting with the identifier name: say what it does and what callers rely on. Never write "X is X". |
| noctx | `http.NewRequestWithContext(ctx, …)`; in tests `t.Context()`. |
| gosec G115 (integer conversion) | Range-check before converting. Example in `internal/ingest/handler.go` `retentionDays`: the value is already clamped by `min(..., math.MaxUint16)`. Make that explicit with `if d < 0 { d = 0 }` and keep the conversion; gosec accepts the guarded form. If not, use `//nolint:gosec // G115: clamped to [0, MaxUint16] above`. |
| gosec G304 (file path from variable) | Operator-supplied paths (config file, rule dirs): `//nolint:gosec // G304: path from operator config`. Paths from request input must be validated instead. |
| gosec G114 | Already fixed by Task 2 (`http.Server` with timeouts). |
| gosec G301/G306 in non-test code | `0o750` / `0o600`. |
| bodyclose | Close `resp.Body` on every path. |
| errorlint | `errors.Is`/`errors.As`, `%w`. |
| staticcheck / unused | Apply the fix or delete the dead code. Check with `git grep` that it is really unused (build tags `oracle`, `bench`). |
| gocritic | Apply the suggestion. |

After each linter: `make lint` (that linter's count is 0), `make test`, commit with `refactor(lint): <linter>`.

- [ ] **Step 4: Verify the whole gate except coverage**

```bash
make fmt-check vet lint vuln
make test && make test-cluster
make oracle-all
```
Expected: all pass. The oracle proves the cleanup changed no behaviour. Commit any remaining fix.

---

### Task 5: Coverage to ≥ 85 %

**Files:**
- Create:
  - `internal/ingest/distcopy/field_detection_test.go` (copied from Loki)
  - `internal/ingest/distcopy/validator_test.go` (copied from Loki)
  - `internal/ingest/distcopy/prepare_test.go`
  - `internal/api/metadata_test.go`, `internal/api/query_test.go`
  - `internal/deletes/list_test.go`
  - `tools/extract-corpus/main_test.go`
  - `internal/ruler/traversal_test.go`
- Modify: `tools/extract-corpus/main.go` (extract `run`), `Makefile` (`COVERAGE_MIN ?= 85`)

**Interfaces:**
- Consumes: `make cover`.
- Produces: total coverage ≥ 85 % and `COVERAGE_MIN ?= 85` in the `Makefile`.

The baseline is 2554 statements with 636 uncovered, so 253 more must be covered. Task 2's server tests already cover most of the former `cmd/lokihouse` code. Uncovered statements per package before this phase:

| Package | Statements | Uncovered |
|---|---:|---:|
| internal/ingest/distcopy | 522 | 233 |
| internal/chstore | 795 | 99 |
| internal/deletes | 278 | 70 |
| internal/api | 215 | 62 |
| cmd/lokihouse (now internal/server) | 55 | 55 |
| tools/extract-corpus | 68 | 47 |

- [ ] **Step 1: Measure**

```bash
COVERAGE_MIN=0 make cover | tail -1
go tool cover -func=coverage.out | sort -k3 -n | head -60
```

- [ ] **Step 2: Copy Loki's field-detection and validator tests**

Copy `$(go list -m -f '{{.Dir}}' github.com/grafana/loki/v3)/pkg/distributor/field_detection_test.go` to `internal/ingest/distcopy/field_detection_test.go`. Keep the tests that call only functions lokihouse copied:
- `Test_detectLogLevelFromLogEntry`
- `Test_detectLogLevelFromLogEntryWithCustomLabels`
- `Test_detectLogLevelFromOTLPSeverityNumber`
- `Test_DetectGenericFields_Enabled`
- `Test_DetectGenericFields`
- `TestGetLevelUsingJsonParser`
- `Test_detectLevelFromLogLine`
- `Test_detectLevelFromLogLine_earliestPosition`

Drop the tests that need Loki's `Distributor` (`TestDistributor_SharedFieldDetection`, `Test_DetectLogLevels`). Put the header at the top:
```go
// Copied from grafana/loki@fbc3f077 pkg/distributor/field_detection_test.go
// (the tests of the functions in field_detection.go); modifications: package
// distcopy; Distributor-based tests dropped; validationContext built with
// lokihouse's copy.
```
Adapt the `validationContext{...}` literals to the fields of lokihouse's copy (`internal/ingest/distcopy/validator.go`). Do the same for `pkg/distributor/validator_test.go`, keeping the `ShouldBlockIngestion` tests.

Run: `go test ./internal/ingest/distcopy/ -count=1 -cover`. Expected: PASS, package coverage ≥ 80 %.

- [ ] **Step 3: Test `Prepare`'s branches black-box**

`internal/ingest/distcopy/prepare_test.go`:
```go
package distcopy_test

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/ingest/distcopy"
)

func overrides(t *testing.T, mut func(*validation.Limits)) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("l", flag.PanicOnError))
	mut(&l)
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func req(labels string, entries ...logproto.Entry) *logproto.InternalPushRequest {
	r := &logproto.PushRequest{Streams: []logproto.Stream{{Labels: labels, Entries: entries}}}
	return logproto.NewInternalPushRequest(r) // adapt to the constructor push.ParseRequest uses at the pinned commit
}

func TestPrepare_TruncatesLongLines(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.MaxLineSize = 5; l.MaxLineSizeTruncate = true })
	now := time.Now()
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "0123456789"}), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries[0].Line, 5+len("...")) // Loki appends its truncation marker; assert Loki's exact marker
}

func TestPrepare_MissingEnforcedLabels(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.EnforcedLabels = []string{"team"} })
	now := time.Now()
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "x"}), now)
	require.Empty(t, out)
	require.ErrorContains(t, err, "missing required labels")
}

func TestPrepare_IncrementsDuplicateTimestamps(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.IncrementDuplicateTimestamp = true })
	now := time.Now().Truncate(time.Second)
	out, err := distcopy.Prepare(context.Background(), o, "t",
		req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "1"}, logproto.Entry{Timestamp: now, Line: "2"}), now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Nanosecond), out[0].Entries[1].Timestamp)
}

func TestPrepare_NormalisesStructuredMetadataNames(t *testing.T) {
	o := overrides(t, func(*validation.Limits) {})
	now := time.Now()
	e := logproto.Entry{Timestamp: now, Line: "x", StructuredMetadata: []logproto.LabelAdapter{{Name: "trace-id", Value: "1"}}}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Equal(t, "trace_id", out[0].Entries[0].StructuredMetadata[0].Name)
}
```
`logproto.InternalPushRequest` and the truncation marker vary between Loki commits. Before running, read `internal/ingest/handler.go` and `push.ParseRequest`'s return type to build the request correctly. Read Loki's `truncateLines` for the exact marker and assert it exactly. Run with `-cover`. Expected: PASS.

- [ ] **Step 4: API handlers**

`internal/api/metadata_test.go` and `internal/api/query_test.go`, using `newServerConn` and `do` from `api_test.go`:
```go
package api_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pushOne(t *testing.T, base, tenant string, ts time.Time) {
	t.Helper()
	body := fmt.Sprintf(`{"streams":[{"stream":{"app":"a","env":"dev"},"values":[[%q,"hello"]]}]}`, strconv.FormatInt(ts.UnixNano(), 10))
	code, out := do(t, http.MethodPost, base+"/loki/api/v1/push", tenant, body)
	require.Equal(t, http.StatusNoContent, code, out)
}

func TestAPI_SeriesAndLabelValues(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)
	q := url.Values{"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)}}

	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/label/env/values?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, `{"status":"success","data":["dev"]}`, body)

	q.Set("match[]", `{app="a"}`)
	code, body = do(t, http.MethodGet, srv.URL+"/loki/api/v1/series?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.JSONEq(t, `{"status":"success","data":[{"app":"a","env":"dev"}]}`, body)

	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/series?start=x", "t1", "")
	require.Equal(t, http.StatusBadRequest, code)
}

func TestAPI_InstantQuery(t *testing.T) {
	srv := newServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now)
	q := url.Values{"query": {`count_over_time({app="a"}[5m])`}, "time": {strconv.FormatInt(now.Add(time.Second).UnixNano(), 10)}}
	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"resultType":"vector"`)

	q.Set("query", `{app="a"}`) // a log query is not a valid instant query (Loki)
	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?"+q.Encode(), "t1", "")
	require.Equal(t, http.StatusBadRequest, code)

	code, _ = do(t, http.MethodGet, srv.URL+"/loki/api/v1/query?query=%7B", "t1", "")
	require.Equal(t, http.StatusBadRequest, code)
}

func TestAPI_BuildInfo(t *testing.T) {
	srv := newServer(t, false)
	code, body := do(t, http.MethodGet, srv.URL+"/loki/api/v1/status/buildinfo", "", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, `"goVersion"`)
}
```
Check the exact `series` and `label` JSON against Loki's marshalling. The existing oracle compares these endpoints, so copy the shape the oracle accepts.

- [ ] **Step 5: Deletes listing and the ruler path-traversal proof**

`internal/deletes/list_test.go`: cover `Handler.List` branches with `newHandler` and `doTenant` from `handler_test.go`:
- empty list → `[]`;
- after `Add`, the request appears with Loki's JSON fields (`request_id`, `start_time`, `end_time`, `query`, `status`, `created_at`);
- another tenant sees `[]`.

`internal/ruler/traversal_test.go` (the govulncheck allowlist proof):
```go
func TestRules_PathTraversalRejected(t *testing.T) {
	h := startRuler(t, t.TempDir(), engine(t, mustConn(t)), nil)
	for _, p := range []string{
		"/loki/api/v1/rules/..%2F..%2Fetc",
		"/loki/api/v1/rules/ns/..%2F..%2Fpasswd",
	} {
		code, body := h.do(t, http.MethodPost, p, "t1", group)
		require.NotEqual(t, http.StatusAccepted, code, "%s: %s", p, body)
	}
}
```
`mustConn` is `func mustConn(t *testing.T) driver.Conn { c, _ := chtest.New(t); return c }`. If a helper with the same purpose already exists in `ruler_test.go`, use it instead.

- [ ] **Step 6: `tools/extract-corpus`**

Refactor `main` into `run(root string, w io.Writer) error` (main calls it with `flag` and `os.Stdout`). Add `main_test.go`. It builds a fake Loki tree in `t.TempDir()`:
- `pkg/logql/syntax/parser_test.go` with a Go string literal containing `{app="a"} |= "x"`;
- `pkg/logql/engine_test.go` with `count_over_time({app="a"}[5m])`;
- `docs/sources/query/q.md` with a ```` ```logql ```` fence containing `rate({app="b"}[1m])`.

It asserts the output contains the normalised forms of all three and the header line, and that a `[30d]` range query is dropped (`maxRange`).

- [ ] **Step 7: Raise the gate and verify**

Set `COVERAGE_MIN ?= 85` in the `Makefile`.
```bash
make check
make test-cluster
```
Expected: `total coverage: ≥85.0% (minimum 85%)`. If it falls short, `go tool cover -func=coverage.out | sort -k3 -n | head -40` shows the next-largest uncovered functions. Add behaviour tests for them (not line-hitting tests) until the gate passes.

- [ ] **Step 8: Commit**

```bash
git add -A internal tools Makefile
git commit -m "test: coverage to 85% (Loki distributor tests, API, deletes, extract-corpus)"
```

---

### Task 6: Documentation site, README and manpage

**Files:**
- Create: `README.md`, `mkdocs.yml`, `docs/requirements.txt`, `docs/index.md`, `docs/getting-started.md`, `docs/configuration.md`, `docs/operations.md`, `docs/api.md`, `docs/architecture.md`, `docs/development.md`, `docs/man/lokihouse.1.md`, `internal/config/docs_test.go`
- Modify: `cmd/lokihouse/main_test.go`, `Makefile`, `docs/phase*-status.md` (links only)

**Interfaces:**
- Consumes: the flags (Task 3), the config keys (Tasks 2–3), `docs/divergences.md`.
- Produces:
  - `make docs` → `site/` via `mkdocs build --strict`;
  - `make man` → `bin/lokihouse.1` via `go-md2man`;
  - drift tests: every flag appears in the manpage, and every lokihouse-owned config key appears in `docs/configuration.md`.

- [ ] **Step 1: Write the failing drift tests**

Append to `cmd/lokihouse/main_test.go` (refactor `run` so the flag set comes from `newFlagSet(stderr io.Writer) (*flag.FlagSet, *flags)`):
```go
func TestManpageDocumentsEveryFlag(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "man", "lokihouse.1.md"))
	require.NoError(t, err)
	fs, _ := newFlagSet(io.Discard)
	fs.VisitAll(func(f *flag.Flag) {
		require.Contains(t, string(b), "**-"+f.Name+"**", "flag -%s missing from docs/man/lokihouse.1.md", f.Name)
	})
}
```
`internal/config/docs_test.go`:
```go
package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// lokiOwned are blocks whose keys are Loki's own; the docs link to Loki's
// reference instead of listing every key.
var lokiOwned = map[string]bool{"ruler": true, "ruler_storage": true, "limits_config": true}

func TestConfigurationDocsCoverEveryKey(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "configuration.md"))
	require.NoError(t, err)
	doc := string(b)
	var walk func(prefix string, t reflect.Type)
	walk = func(prefix string, typ reflect.Type) {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			key, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if key == "" || key == "-" {
				continue
			}
			full := prefix + key
			require.Contains(t, doc, "`"+full+"`", "config key %s missing from docs/configuration.md", full)
			if f.Type.Kind() == reflect.Struct && !lokiOwned[key] && f.Type.PkgPath() == "" {
				walk(full+".", f.Type)
			}
		}
	}
	walk("", reflect.TypeOf(Config{}))
}
```
Only anonymous inline structs (`PkgPath() == ""`) are walked. Named types like `dskitlog.Level` and `time.Duration` are leaves.

Run: `go test ./cmd/lokihouse/ ./internal/config/ -run 'Manpage|Docs' -count=1`. Expected: FAIL (files missing).

- [ ] **Step 2: Write the manpage `docs/man/lokihouse.1.md` (go-md2man format)**

```markdown
% LOKIHOUSE(1) lokihouse | User Commands
% Bernd Zeimetz
% October 2026

# NAME

lokihouse - Loki-compatible log store on ClickHouse

# SYNOPSIS

**lokihouse** [**-config.file** *path*] [**-config.expand-env**] [**-version**]

# DESCRIPTION

**lokihouse** serves Grafana Loki's HTTP API (push, LogQL queries, labels,
series, tail, index statistics, deletes and the ruler) and stores the logs in
ClickHouse. Clients such as Grafana, Promtail, Alloy and logcli talk to it as
they would to Loki.

On start it creates the database and tables it needs, then listens on
*server.http_listen_address*:*server.http_listen_port* (default port 3100).

# OPTIONS

**-config.file** *path*
:   YAML configuration file (default *lokihouse.yaml*). Unknown keys are an error.

**-config.expand-env**
:   Expand *${VAR}* and *${VAR:-default}* in the configuration file before
    parsing it, as Loki does.

**-version**
:   Print version, revision, branch, build date and Go version, then exit.

# SIGNALS

**SIGTERM**, **SIGINT**
:   Stop accepting connections, let in-flight requests finish (at most
    *server.graceful_shutdown_timeout*, default 30s), stop the ruler and the
    background loops, close the ClickHouse connection and exit 0. A second
    signal terminates immediately.

# EXIT STATUS

**0** clean shutdown; **1** configuration or runtime error; **2** invalid
command line.

# FILES

*/etc/lokihouse/lokihouse.yaml*
:   Configuration used by the Debian package's *lokihouse.service*.

*/var/lib/lokihouse*
:   Working directory of the service: temporary rule files and the ruler WAL.

# ENVIRONMENT

**NOTIFY_SOCKET**
:   Set by systemd for *Type=notify* services; lokihouse reports readiness and
    shutdown on it.

**CREDENTIALS_DIRECTORY**
:   Set by systemd when credentials are imported; reference it from the
    configuration with **-config.expand-env**, for example
    *password_file: ${CREDENTIALS_DIRECTORY}/lokihouse.clickhouse-password*.

# SEE ALSO

Documentation: <https://bzed.github.io/lokihouse/>; Loki configuration
reference for the *ruler*, *ruler_storage* and *limits_config* blocks:
<https://grafana.com/docs/loki/latest/configure/>.
```

- [ ] **Step 3: Write the docs pages**

`docs/index.md`: what lokihouse is (from spec §1, in user terms), the feature list (spec §6 in user terms), status (phases), and links to the other pages. `README.md`: a short version of `index.md` with a quick start (podman ClickHouse + `make build` + `./bin/lokihouse -config.file lokihouse.example.yaml`), and links to the site, `AGENTS.md` and `LICENSE` (AGPL-3.0).

`docs/getting-started.md`:
- install the `.deb` from a GitHub release (`apt install ./lokihouse_0.5.0_amd64.deb`);
- or build from source (`make build`);
- start ClickHouse (podman command);
- edit `/etc/lokihouse/lokihouse.yaml`;
- `systemctl enable --now lokihouse`;
- push a line with curl, query it with curl and `logcli`;
- add the data source to Grafana (type Loki, URL `http://host:3100`).

`docs/configuration.md`: one section per block, each key in backticks with its full dotted name (as the drift test expects), type, default and meaning:
- `auth_enabled`
- `server.*`: every key from Tasks 2–3
- `clickhouse.*`: `addr`, `database`, `username`, `password`, `password_file`, `cluster`, `replicated`, `replica_path`, `replica_name`, `settings`
- `query.pushdown`
- `compactor.*`
- `ruler`, `ruler_storage`, `limits_config`: these link to Loki's reference and list the lokihouse-specific notes (rule_path default, unsupported sharding, WAL guard)
- the `-config.expand-env` and `password_file` patterns.

`docs/operations.md`:
- systemd service (unit, DynamicUser, state dir, credentials via `systemd-creds`)
- shutdown behaviour
- readiness (`/ready`, sd_notify)
- logs (journald, `log_level`)
- ClickHouse cluster mode (from spec §5.1)
- retention, deletes
- running the ruler on exactly one instance
- upgrades (config conffile, migrations are idempotent)

Sections added later: "Limits" (Task 9), "Multi-tenant queries" (Task 10), "Metrics" (Task 11).

`docs/api.md`: the endpoint table, each endpoint with its Loki doc link and lokihouse notes (from the phase status docs).

`docs/architecture.md`: spec §4 (diagram, Tier A/B) and §5 (schema), rewritten for readers.

`docs/development.md`: make targets, test prerequisites, oracle, cluster, `AGENTS.md`, release process (Task 8).

`docs/requirements.txt`:
```text
mkdocs==1.6.1
mkdocs-material==9.7.7
```
`mkdocs.yml`:
```yaml
site_name: lokihouse
site_description: Loki-compatible log store on ClickHouse
site_url: https://bzed.github.io/lokihouse/
repo_url: https://github.com/bzed/lokihouse
edit_uri: edit/main/docs/
docs_dir: docs
strict: true
exclude_docs: |
  superpowers/
theme:
  name: material
  features: [navigation.sections, content.code.copy, search.highlight]
  palette:
    - media: "(prefers-color-scheme: light)"
      scheme: default
      toggle: {icon: material/brightness-7, name: Dark mode}
    - media: "(prefers-color-scheme: dark)"
      scheme: slate
      toggle: {icon: material/brightness-4, name: Light mode}
markdown_extensions: [admonition, tables, toc, attr_list, pymdownx.superfences, pymdownx.highlight, def_list]
nav:
  - Home: index.md
  - Getting started: getting-started.md
  - Configuration: configuration.md
  - Operations: operations.md
  - API: api.md
  - Architecture: architecture.md
  - Differences from Loki: divergences.md
  - Manual page: man/lokihouse.1.md
  - Development:
      - development.md
      - Benchmark: benchmark.md
      - Phase 1: phase1-status.md
      - Phase 2: phase2-status.md
      - Phase 3: phase3-status.md
      - Phase 4: phase4-status.md
```
Task 12 adds `benchmark.md`. Until then, create it with one line: "Results are added in Phase 5 Task 12." In the phase status pages, rewrite any link into `docs/superpowers/` as an absolute GitHub URL (`https://github.com/bzed/lokihouse/blob/main/docs/superpowers/...`), so the strict build does not fail on excluded pages.

- [ ] **Step 4: Makefile targets**

```make
.PHONY: docs man
docs:
	python3 -m venv /tmp/lokihouse-docs-venv
	/tmp/lokihouse-docs-venv/bin/pip install -q -r docs/requirements.txt
	/tmp/lokihouse-docs-venv/bin/mkdocs build --strict --site-dir site
man:
	mkdir -p bin
	go-md2man -in docs/man/lokihouse.1.md -out bin/lokihouse.1
```

- [ ] **Step 5: Verify**

```bash
go test ./cmd/lokihouse/ ./internal/config/ -count=1
make docs && make man && man -l bin/lokihouse.1 | head -20
make check
```
Expected: tests PASS, the strict mkdocs build has no warnings, and the manpage renders.

- [ ] **Step 6: Commit**

```bash
git add README.md mkdocs.yml docs cmd internal/config Makefile
git commit -m "docs: mkdocs site, README, manpage; flag/config drift tests"
```

---

### Task 7: Debian package

**Files:**
- Create:
  - `debian/changelog`, `debian/control`, `debian/copyright`, `debian/rules`
  - `debian/source/format`, `debian/source/options`
  - `debian/lokihouse.service`, `debian/lokihouse.yaml`
  - `debian/lokihouse.install`, `debian/lokihouse.manpages`, `debian/lokihouse.docs`, `debian/lokihouse.dirs`
  - `debian/README.Debian`
  - `debian/tests/control`, `debian/tests/smoke`
- Modify: `Makefile` (`deb` target), `.gitignore`

**Interfaces:**
- Consumes: the `Makefile` `LDFLAGS` variables (Task 3), `docs/man/lokihouse.1.md` (Task 6), `lokihouse.example.yaml`.
- Produces:
  - `dpkg-buildpackage -us -uc -b` → `../lokihouse_<ver>_<arch>.deb`;
  - `debian/tests/smoke`, run in CI after installing the package (Task 8);
  - `make deb`.

- [ ] **Step 1: Write the smoke test first**

`debian/tests/control`:
```
Tests: smoke
Depends: lokihouse, systemd
Restrictions: needs-root, superficial
```
`debian/tests/smoke`:
```sh
#!/bin/sh
# Installed-package checks: binary, manpage, config, unit validity and hardening.
set -eu
lokihouse -version | grep -q '^lokihouse, version '
test -f /etc/lokihouse/lokihouse.yaml
dpkg-query -W -f='${Conffiles}\n' lokihouse | grep -q '/etc/lokihouse/lokihouse.yaml'
test -f /usr/share/man/man1/lokihouse.1.gz
test -f /usr/share/doc/lokihouse/examples/lokihouse.example.yaml
unit=/usr/lib/systemd/system/lokihouse.service
test -f "$unit"
systemd-analyze verify --man=no "$unit"
systemd-analyze security --offline=true --threshold=15 --no-pager "$unit"
# Not enabled on install: there is no ClickHouse to talk to yet.
if [ -d /run/systemd/system ]; then
	! systemctl is-enabled --quiet lokihouse.service
fi
# The packaged config parses (no ClickHouse needed for parsing).
lokihouse -config.file /etc/lokihouse/lokihouse.yaml -config.expand-env -version >/dev/null
```
The last line only proves that flags parse. Config parsing is covered by `TestLoad_PackagedConfig` in Step 2.

- [ ] **Step 2: Test that the packaged config loads**

Append to `internal/config/config_test.go`:
```go
func TestLoad_PackagedConfig(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", t.TempDir())
	c, err := Load(filepath.Join("..", "..", "debian", "lokihouse.yaml"), true)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", c.Server.HTTPListenAddress, "the package listens on localhost by default")
}
```
Run: expected FAIL (file missing).

- [ ] **Step 3: Write `debian/lokihouse.yaml`**

```yaml
# lokihouse configuration (Debian package). All options:
#   man lokihouse, /usr/share/doc/lokihouse/examples/lokihouse.example.yaml,
#   https://bzed.github.io/lokihouse/configuration/
# The service runs with -config.expand-env, so ${VAR} is expanded.
auth_enabled: false              # true: clients must send X-Scope-OrgID
server:
  http_listen_address: 127.0.0.1 # only local clients; use 0.0.0.0 to listen everywhere
  http_listen_port: 3100
  log_level: info
clickhouse:
  addr: ["localhost:9000"]
  database: lokihouse
  username: default
  # Keep the password out of this file: store it as a systemd credential
  #   systemd-creds encrypt --name=lokihouse.clickhouse-password - \
  #     /etc/credstore.encrypted/lokihouse.clickhouse-password
  # and uncomment:
  # password_file: ${CREDENTIALS_DIRECTORY}/lokihouse.clickhouse-password
limits_config: {}
```
Run the Step 2 test. Expected: PASS.

- [ ] **Step 4: Write the unit `debian/lokihouse.service`**

```ini
[Unit]
Description=lokihouse - Loki-compatible log store on ClickHouse
Documentation=man:lokihouse(1) https://bzed.github.io/lokihouse/
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
NotifyAccess=main
ExecStart=/usr/bin/lokihouse -config.file=/etc/lokihouse/lokihouse.yaml -config.expand-env=true
Restart=on-failure
RestartSec=5s
# graceful_shutdown_timeout (30s) plus ruler and ClickHouse shutdown.
TimeoutStopSec=45s
DynamicUser=yes
StateDirectory=lokihouse
StateDirectoryMode=0750
WorkingDirectory=/var/lib/lokihouse
ConfigurationDirectory=lokihouse
# Credentials named lokihouse.* from /etc/credstore{,.encrypted} (optional).
ImportCredential=lokihouse.*
UMask=0077
LimitNOFILE=65536
CapabilityBoundingSet=
AmbientCapabilities=
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
PrivateUsers=yes
PrivateIPC=yes
ProtectSystem=strict
ProtectHome=yes
ProtectHostname=yes
ProtectClock=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectProc=invisible
ProcSubset=pid
# AF_UNIX is needed for sd_notify.
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
RemoveIPC=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native
SystemCallFilter=@system-service
SystemCallFilter=~@privileged @resources
SystemCallErrorNumber=EPERM
DevicePolicy=closed
KeyringMode=private

[Install]
WantedBy=multi-user.target
```
Check it locally: `systemd-analyze security --offline=true --threshold=15 --no-pager debian/lokihouse.service`. Expected: `Overall exposure level … 1.1 OK`, exit 0.

- [ ] **Step 5: Write the packaging files**

`debian/source/format`: `3.0 (native)`

`debian/source/options`:
```
tar-ignore = .cache
tar-ignore = bin
tar-ignore = site
tar-ignore = coverage.out
tar-ignore = debian/.build
```
`debian/changelog`:
```
lokihouse (0.5.0) trixie; urgency=medium

  * Phase 5: graceful shutdown, Debian packaging, docs, CI, limits,
    multi-tenant queries, metrics and query stats.

 -- Bernd Zeimetz <bernd@bzed.de>  Sun, 11 Oct 2026 12:00:00 +0200
```
`debian/control`:
```
Source: lokihouse
Section: net
Priority: optional
Maintainer: Bernd Zeimetz <bernd@bzed.de>
Build-Depends: debhelper-compat (= 13),
               ca-certificates,
               curl,
               git,
               go-md2man,
Standards-Version: 4.7.2
Homepage: https://github.com/bzed/lokihouse
Vcs-Browser: https://github.com/bzed/lokihouse
Vcs-Git: https://github.com/bzed/lokihouse.git
Rules-Requires-Root: no

Package: lokihouse
Architecture: amd64 arm64
Depends: ${misc:Depends}, ${shlibs:Depends}
Suggests: clickhouse-server
Description: Loki-compatible log store on ClickHouse
 lokihouse serves the HTTP API of Grafana Loki (push, LogQL queries, labels,
 series, tail, index statistics, deletes and the ruler) and stores the logs in
 ClickHouse, so Grafana, Promtail, Alloy and logcli use it like Loki.
 .
 The package ships a hardened systemd service using a dynamic user. It is not
 enabled on installation: configure /etc/lokihouse/lokihouse.yaml first.
```
`debian/rules`:
```make
#!/usr/bin/make -f
# The Go toolchain and the Go modules are downloaded during the build (pinned
# and checksummed: SHA-256 below, go.sum for modules). The build therefore
# needs network access; the installed package does not.

export DH_VERBOSE = 1
include /usr/share/dpkg/pkg-info.mk
include /usr/share/dpkg/architecture.mk

GO_VERSION := 1.27.2
GO_SHA256_amd64 := ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5
GO_SHA256_arm64 := 94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8
GO_SHA256 := $(GO_SHA256_$(DEB_HOST_ARCH))
ifeq ($(GO_SHA256),)
$(error unsupported architecture $(DEB_HOST_ARCH): add the go$(GO_VERSION) checksum to debian/rules)
endif

B := $(CURDIR)/debian/.build
GO := $(B)/go/bin/go
export HOME := $(B)/home
export GOPATH := $(B)/gopath
export GOCACHE := $(B)/gocache
export GOMODCACHE := $(B)/gomodcache
export GOTOOLCHAIN := local
export GOTELEMETRY := off
export GOFLAGS := -trimpath -mod=readonly -buildvcs=false
export CGO_ENABLED := 0

REVISION := $(shell git rev-parse --short=12 HEAD 2>/dev/null)
BUILD_DATE := $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" +%Y-%m-%dT%H:%M:%SZ)
PV := github.com/prometheus/common/version
LDFLAGS := -X $(PV).Version=$(DEB_VERSION_UPSTREAM) -X $(PV).Revision=$(REVISION) \
	-X $(PV).Branch=debian -X $(PV).BuildDate=$(BUILD_DATE) -X $(PV).BuildUser=debian

%:
	dh $@

override_dh_auto_configure:
	mkdir -p $(B) $(HOME)
	curl -fsSL --proto '=https' --tlsv1.2 -o $(B)/go.tar.gz \
		https://go.dev/dl/go$(GO_VERSION).linux-$(DEB_HOST_ARCH).tar.gz
	echo "$(GO_SHA256)  $(B)/go.tar.gz" | sha256sum -c -
	tar -C $(B) -xzf $(B)/go.tar.gz
	$(GO) mod download

override_dh_auto_build:
	$(GO) build -buildmode=pie -ldflags "$(LDFLAGS)" -o $(B)/lokihouse ./cmd/lokihouse
	go-md2man -in docs/man/lokihouse.1.md -out $(B)/lokihouse.1
	$(GO) version -m $(B)/lokihouse > $(B)/MODULES.txt

override_dh_auto_test:
ifeq (,$(filter nocheck,$(DEB_BUILD_OPTIONS)))
	# Unit tests need a ClickHouse server (CI runs them separately); check
	# the binary and the packages that need no database.
	$(B)/lokihouse -version
	$(GO) test -count=1 ./internal/config/ ./internal/logging/ ./cmd/lokihouse/
endif

override_dh_auto_install:

override_dh_installsystemd:
	dh_installsystemd --no-enable

# dwz does not handle Go's DWARF.
override_dh_dwz:

override_dh_auto_clean:
	chmod -R u+w $(B) 2>/dev/null || true
	rm -rf $(B)
```
`debian/lokihouse.install`:
```
debian/.build/lokihouse usr/bin
debian/lokihouse.yaml etc/lokihouse
lokihouse.example.yaml usr/share/doc/lokihouse/examples
```
`debian/lokihouse.manpages`: `debian/.build/lokihouse.1`

`debian/lokihouse.docs`:
```
README.md
docs/divergences.md
debian/.build/MODULES.txt
```
`debian/lokihouse.dirs`: `etc/lokihouse`

`debian/README.Debian`:
- ClickHouse is not packaged in Debian. Install it from ClickHouse's repository or point `clickhouse.addr` at a remote server.
- The service is not enabled on install. Edit `/etc/lokihouse/lokihouse.yaml`, then `systemctl enable --now lokihouse`.
- The service runs as a dynamic user with state in `/var/lib/lokihouse`. Point `ruler_storage.filesystem.dir` at `/var/lib/lokihouse/rules`.
- Put the ClickHouse password in a credential with the `systemd-creds` example from the config.
- The default listen address is 127.0.0.1.
- The ruler must run on one instance only.

`debian/copyright` (machine-readable, `Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/`):
- `Files: *`: `Copyright: 2026 Bernd Zeimetz <bernd@bzed.de>`, `License: AGPL-3.0`. Check `LICENSE`, and use `AGPL-3.0+` if the project says "or later".
- A `Files:` stanza for every file that carries the `Copied from grafana/loki` header (`git grep -l "Copied from grafana/loki"`): `Copyright: 2015-2026 Grafana Labs`, `License: AGPL-3.0`.
- The AGPL-3.0 license paragraph, referencing `/usr/share/common-licenses/AGPL-3` only if that file exists on trixie. If it does not, include the full text.
- A comment that the binary statically embeds the Go modules listed in `/usr/share/doc/lokihouse/MODULES.txt`, each under its own licence.

`chmod +x debian/rules debian/tests/smoke`. Add `debian/.build/`, `debian/lokihouse/`, `debian/.debhelper/`, `debian/debhelper-build-stamp`, `debian/files` and `debian/*.substvars` to `.gitignore`.

- [ ] **Step 6: Build, lint and test the package in a trixie container**

Add the `Makefile` target:
```make
.PHONY: deb
# Builds the .deb for the host architecture in a debian:trixie container (podman).
deb:
	out=$$(mktemp -d /tmp/lokihouse-deb.XXXXXX); \
	podman run --rm -v "$(CURDIR):/src:ro,Z" -v "$$out:/out:Z" docker.io/library/debian:trixie sh -euc '\
		apt-get update -qq && apt-get install -y -qq --no-install-recommends devscripts equivs build-essential lintian git rsync >/dev/null; \
		rsync -a --exclude .cache --exclude bin --exclude site /src/ /build/; cd /build; \
		git config --global --add safe.directory /build; \
		mk-build-deps -i -r -t "apt-get -y -qq --no-install-recommends" debian/control >/dev/null; \
		dpkg-buildpackage -us -uc -b; \
		lintian --fail-on error,warning --info ../lokihouse_*.deb; \
		apt-get install -y -qq systemd ../lokihouse_*.deb >/dev/null && sh debian/tests/smoke; \
		cp ../lokihouse_*.deb ../lokihouse_*.buildinfo ../lokihouse_*.changes /out/'; \
	echo "packages in $$out"; ls -l "$$out"
```
Run: `make deb`. Expected: the build, lintian and the smoke test all succeed, and the `.deb` is in `/tmp/lokihouse-deb.*`.

Lintian warnings: fix each one. If a warning is inherent to a statically linked Go binary built with a downloaded toolchain, override it in `debian/lokihouse.lintian-overrides` with a `#` comment line naming the reason. Every override needs that reason.

- [ ] **Step 7: Commit**

```bash
git add debian Makefile .gitignore internal/config/config_test.go
git commit -m "build(debian): native trixie package, hardened DynamicUser unit, packaged config"
```

---

### Task 8: GitHub Actions

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/deb.yml`, `.github/workflows/nightly.yml`, `.github/workflows/release.yml`, `.github/workflows/pages.yml`, `.github/dependabot.yml`
- Modify: `test/cluster/up.sh`, `docs/development.md` (CI and release sections)

**Interfaces:**
- Consumes: `make fmt-check vet lint vuln cover build docs man` (Tasks 1–6), `debian/` (Task 7), `test/cluster/up.sh`.
- Produces:
  - `deb.yml`: a reusable workflow (`workflow_call`, input `snapshot: boolean`) that uploads artifacts `deb-amd64` and `deb-arm64`.
  - The release process: bump `debian/changelog` → tag `v<version>` → `release.yml` publishes the `.deb` files and `SHA256SUMS`.

- [ ] **Step 1: `test/cluster/up.sh` without a host `clickhouse-client`**

GitHub runners have podman but no `clickhouse-client`. Change the readiness loop to run the client inside the node:
```sh
  until podman exec "ch$i" clickhouse-client -q "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do
```
Run `make cluster-down && make cluster-up`. Expected: `cluster lokihouse up`.

- [ ] **Step 2: `ci.yml`**

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
permissions:
  contents: read
concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true
jobs:
  checks:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - run: make fmt-check vet lint vuln
  test:
    runs-on: ubuntu-24.04
    services:
      clickhouse:
        image: docker.io/clickhouse/clickhouse-server:26.9
        env:
          CLICKHOUSE_SKIP_USER_SETUP: "1"
        ports: ["9000:9000"]
        options: >-
          --ulimit nofile=262144:262144
          --health-cmd "clickhouse-client -q 'SELECT 1'"
          --health-interval 5s --health-retries 30
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - run: make cover
      - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: coverage
          path: coverage.out
  build:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - run: make build && ./bin/lokihouse -version
  docs:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97 # v7.0.0
        with:
          python-version: "3.13"
      - run: sudo apt-get update && sudo apt-get install -y go-md2man
      - run: make docs man
  deb:
    uses: ./.github/workflows/deb.yml
    with:
      snapshot: true
```

- [ ] **Step 3: `deb.yml` (reusable)**

```yaml
name: deb
on:
  workflow_call:
    inputs:
      snapshot:
        type: boolean
        required: true
permissions:
  contents: read
jobs:
  deb:
    strategy:
      fail-fast: false
      matrix:
        include:
          - { arch: amd64, runner: ubuntu-24.04 }
          - { arch: arm64, runner: ubuntu-24.04-arm }
    runs-on: ${{ matrix.runner }}
    container: docker.io/library/debian:trixie
    steps:
      - run: apt-get update && apt-get install -y --no-install-recommends git ca-certificates
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0
      - run: git config --global --add safe.directory "$GITHUB_WORKSPACE"
      - run: apt-get install -y --no-install-recommends devscripts equivs build-essential lintian systemd
      - run: mk-build-deps -i -r -t 'apt-get -y --no-install-recommends' debian/control
      - if: inputs.snapshot
        env:
          DEBEMAIL: bernd@bzed.de
          DEBFULLNAME: Bernd Zeimetz
        run: |
          v=$(dpkg-parsechangelog -SVersion)
          dch --newversion "${v}+git$(date -u +%Y%m%d%H%M%S).$(git rev-parse --short=8 HEAD)" \
            --distribution trixie --force-distribution "CI snapshot build."
      - run: dpkg-buildpackage -us -uc -b
      - run: lintian --fail-on error,warning --info ../lokihouse_*.deb
      - run: apt-get install -y ../lokihouse_*.deb && sh debian/tests/smoke
      - run: mkdir out && cp ../lokihouse_*.deb ../lokihouse_*.buildinfo ../lokihouse_*.changes out/
      - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: deb-${{ matrix.arch }}
          path: out/
```

- [ ] **Step 4: `nightly.yml`**

```yaml
name: nightly
on:
  schedule:
    - cron: "17 3 * * *"
  workflow_dispatch:
permissions:
  contents: read
jobs:
  cluster:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - run: make test-cluster
  oracle:
    runs-on: ubuntu-24.04
    timeout-minutes: 180
    services:
      clickhouse:
        image: docker.io/clickhouse/clickhouse-server:26.9
        env:
          CLICKHOUSE_SKIP_USER_SETUP: "1"
        ports: ["9000:9000"]
        options: >-
          --ulimit nofile=262144:262144
          --health-cmd "clickhouse-client -q 'SELECT 1'"
          --health-interval 5s --health-retries 30
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - uses: actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: test/oracle/.cache
          key: loki-${{ hashFiles('Makefile') }}
      - run: make oracle-all
  oracle-cluster:
    runs-on: ubuntu-24.04
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
      - uses: actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: test/oracle/.cache
          key: loki-${{ hashFiles('Makefile') }}
      - run: make oracle-cluster
```
The cache key uses the `Makefile` because it holds `LOKI_COMMIT`. Check `test/oracle/loki.go` `lokiBin` for the directory it builds into and point `path:` at it.

- [ ] **Step 5: `release.yml` and `pages.yml`**

`release.yml`:
```yaml
name: release
on:
  push:
    tags: ["v*"]
permissions:
  contents: read
jobs:
  check-version:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - run: |
          v=$(dpkg-parsechangelog -SVersion)
          test "v$v" = "$GITHUB_REF_NAME" || { echo "tag $GITHUB_REF_NAME does not match debian/changelog $v" >&2; exit 1; }
  deb:
    needs: check-version
    uses: ./.github/workflows/deb.yml
    with:
      snapshot: false
  publish:
    needs: deb
    runs-on: ubuntu-24.04
    permissions:
      contents: write
    steps:
      - uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
        with:
          pattern: deb-*
          merge-multiple: true
          path: dist
      - run: cd dist && sha256sum *.deb > SHA256SUMS
      - uses: softprops/action-gh-release@efb35369e0ad2afab669f228072c1b0d510eae64 # v3.0.3
        with:
          files: |
            dist/*.deb
            dist/SHA256SUMS
          generate_release_notes: true
```
`pages.yml`:
```yaml
name: pages
on:
  push:
    branches: [main]
    paths: ["docs/**", "mkdocs.yml"]
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: pages
  cancel-in-progress: false
jobs:
  build:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97 # v7.0.0
        with:
          python-version: "3.13"
      - uses: actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d # v6.0.0
      - run: make docs
      - uses: actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9 # v5.0.0
        with:
          path: site
  deploy:
    needs: build
    runs-on: ubuntu-24.04
    permissions:
      pages: write
      id-token: write
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@368f82528645a54fb793d4d04e342629a3f51346 # v5.0.1
```
`.github/dependabot.yml`: the Go modules are tied to Loki's pinned commit, so Dependabot covers only Actions and the docs toolchain.
```yaml
version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    schedule: { interval: weekly }
  - package-ecosystem: pip
    directory: /docs
    schedule: { interval: monthly }
```

- [ ] **Step 6: Validate the workflows locally**

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/*.yml
```
If `v1.7.12` is not a published actionlint version, use the latest release (`gh api repos/rhysd/actionlint/releases/latest --jq .tag_name`) and pin that. Expected: no findings.

Document in `docs/development.md`:
- CI jobs and when they run;
- the release steps:
  1. `dch -v <x.y.z>`, set the distribution to `trixie`;
  2. commit;
  3. `git tag -s v<x.y.z>`;
  4. push the tag.
- the one-time repository settings the maintainer makes (not the implementer): Settings → Pages → Source "GitHub Actions"; branch protection requiring `checks`, `test`, `build`, `docs` and `deb`.

- [ ] **Step 7: Commit**

```bash
git add .github test/cluster/up.sh docs/development.md
git commit -m "ci: checks, tests, coverage, deb (amd64/arm64), docs, nightly cluster/oracle, release, pages"
```
Pushing is the maintainer's decision. Do not push or tag as part of this task.

---

# Part B: runtime hardening

### Task 9: Ingestion rate limit, query limits and timeouts

**Files:**
- Create: `internal/ingest/ratelimit.go`, `internal/ingest/ratelimit_test.go`, `internal/api/limits.go`, `internal/api/limits_test.go`
- Modify: `internal/ingest/handler.go`, `internal/ingest/distcopy/process.go`, `internal/ingest/distcopy/prepare_test.go` (Task 5's tests: `Prepare` now returns `Result`; use `res.Streams`), `internal/api/query.go`, `internal/server/server.go`, `docs/operations.md`, `docs/divergences.md`

**Interfaces:**
- Consumes: `distcopy.Prepare` (existing), `validation.Overrides`, `dskit/limiter`.
- Produces:
  - `distcopy.Prepare(ctx, limits, tenantID, req, now) (distcopy.Result, error)` with `type Result struct { Streams []logproto.Stream; Bytes, Lines int }`. Bytes and lines of the kept entries, counted as Loki's rate limiter counts them (`streamStats.unexpandedSize`, `entriesKept`).
  - `distcopy.ReportDiscarded(ctx, limits, tenantID, reason string, streams []logproto.Stream, now time.Time)`: records Loki's discarded samples and bytes metrics for each stream.
  - `ingest.NewRateLimiter(limits *validation.Overrides) *limiter.RateLimiter`, plus field `ingest.Handler.RateLimiter *limiter.RateLimiter` (nil = no limit).
  - `(*api.API).checkQuery(ctx context.Context, expr syntax.Expr, start, end time.Time, limit uint32) (newStart time.Time, empty bool, err error)`.

- [ ] **Step 1: Write the failing rate-limit tests**

`internal/ingest/ratelimit_test.go` (package `ingest_test`; uses `push` and `itoa` from `handler_test.go`):
```go
// rateLimited returns a handler whose tenants may ingest 104 bytes/s with a
// 104-byte burst (0.0001 MB), and its ClickHouse connection.
func rateLimited(t *testing.T) (*ingest.Handler, driver.Conn) {
	t.Helper()
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.IngestionRateMB, l.IngestionBurstSizeMB = 0.0001, 0.0001
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger(), RateLimiter: ingest.NewRateLimiter(o)}, conn
}

func rows(t *testing.T, conn driver.Conn, tenant string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant = ?", tenant).Scan(&n))
	return n
}

func TestRateLimit_RejectsWholePushAndIsolatesTenants(t *testing.T) {
	h, conn := rateLimited(t)
	ts := itoa(time.Now().UnixNano())
	small := `{"streams":[{"stream":{"app":"ok"},"values":[["` + ts + `","small"]]}]}`
	// Two streams: the first fits on its own, both together do not. The whole push is rejected.
	both := `{"streams":[{"stream":{"app":"ok"},"values":[["` + ts + `","small"]]},` +
		`{"stream":{"app":"big"},"values":[["` + ts + `","` + strings.Repeat("x", 200) + `"]]}]}`

	rec := push(t, h, "t1", both)
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "ingestion rate limit exceeded for user t1 (limit: 104 bytes/sec) while attempting to ingest '2' lines totaling '205' bytes")
	require.Equal(t, uint64(0), rows(t, conn, "t1"), "nothing is stored from a rejected push")

	rec = push(t, h, "t2", small)
	require.Equal(t, http.StatusNoContent, rec.Code, "other tenants have their own bucket")
	rec = push(t, h, "t1", small)
	require.Equal(t, http.StatusNoContent, rec.Code, "a rejected push consumes no tokens")
}

func TestRateLimit_DisabledWithoutLimiter(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()} // RateLimiter nil
	rec := push(t, h, "t1", `{"streams":[{"stream":{"app":"a"},"values":[["`+itoa(time.Now().UnixNano())+`","`+strings.Repeat("x", 100<<10)+`"]]}]}`)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}
```
The byte count `205` is `len("small") + 200`. Loki's `unexpandedSize` counts the line plus structured-metadata sizes. If the test reveals a different count, take it from Loki's counting rule; never adjust Loki's rule to match the test.

Run: `go test ./internal/ingest/ -run RateLimit -count=1`. Expected: compile FAIL.

- [ ] **Step 2: Implement**

In `internal/ingest/distcopy/process.go`:
- `Prepare` returns `Result`: accumulate `stats.unexpandedSize` and `stats.entriesKept` from `processStreamEntries` for every kept stream.
- Extend the attribution header: "modifications: … Prepare returns the kept bytes/lines for rate limiting (Loki's rlBuckets accounting, tenant bucket only) …".
- Add `ReportDiscarded`, which calls `p.validator.reportDiscardedDataWithTracker(ctx, reason, vctx, lbs, "", "", bytes, lines, "")` for each stream (labels parsed with `syntax.ParseLabels`).

`internal/ingest/ratelimit.go`:
```go
package ingest

import (
	"github.com/grafana/dskit/limiter"
	"github.com/grafana/loki/v3/pkg/validation"
)

// Copied from grafana/loki@fbc3f077 pkg/distributor/ingestion_rate_strategy.go
// (localStrategy); modifications: tenant-only keys (lokihouse has no policies),
// also used for Loki's "global" strategy (lokihouse has no distributor ring:
// one instance holds the whole limit, see docs/divergences.md).
type localStrategy struct{ limits *validation.Overrides }

func (s localStrategy) Limit(tenant string) float64 { return s.limits.IngestionRateBytes(tenant) }
func (s localStrategy) Burst(tenant string) int    { return s.limits.IngestionBurstSizeBytes(tenant) }

// NewRateLimiter returns Loki's per-tenant ingestion rate limiter
// (ingestion_rate_mb, ingestion_burst_size_mb), recalculated every 10s like
// the distributor's.
func NewRateLimiter(limits *validation.Overrides) *limiter.RateLimiter {
	return limiter.NewRateLimiter(localStrategy{limits: limits}, 10*time.Second)
}
```
In `handler.go` `ServeHTTP`, after `Prepare`, follow Loki's order (`pkg/distributor/distributor.go` Push):
```go
	res, verr := distcopy.Prepare(r.Context(), h.Limits, tenantID, req, now)
	if len(res.Streams) == 0 {
		// … existing: verr → 400, else 204 …
	}
	if h.RateLimiter != nil && !h.RateLimiter.AllowN(now, tenantID, res.Bytes) {
		distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, validation.RateLimited, res.Streams, now)
		err := fmt.Errorf(validation.RateLimitedErrorMsg, tenantID, int(h.RateLimiter.Limit(now, tenantID)), res.Lines, res.Bytes)
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	// … write, then verr → 400, else 204 (unchanged) …
```
`AllowN` consumes tokens only when it returns true, which matches Loki's reserve-then-cancel. Wire `RateLimiter: ingest.NewRateLimiter(limits)` in `server.newAPI`.

Run the tests. Expected: PASS.

- [ ] **Step 3: Write the failing query-limit tests**

`internal/api/limits_test.go`:
```go
func TestQueryLimits(t *testing.T) {
	srv, _ := newServerConnWithLimits(t, true, func(l *validation.Limits) {
		l.MaxQueryLength = model.Duration(time.Hour)
		l.MaxQueryLookback = model.Duration(48 * time.Hour)
		l.MaxEntriesLimitPerQuery = 100
		l.MaxStreamsMatchersPerQuery = 2
		l.RequiredLabels = []string{"app"}
	})
	now := time.Now()
	get := func(path string, v url.Values) (int, string) {
		return do(t, http.MethodGet, srv.URL+path+"?"+v.Encode(), "t1", "")
	}
	rng := func(q string, start, end time.Time, limit int) url.Values {
		return url.Values{"query": {q}, "start": {ns(start)}, "end": {ns(end)}, "limit": {strconv.Itoa(limit)}}
	}

	code, body := get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-2*time.Hour), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "the query time range exceeds the limit (query length: 2h0m0s, limit: 1h)")

	code, body = get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-time.Minute), now, 1000))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "max entries limit per query exceeded, limit > max_entries_limit_per_query (1000 > 100)")

	code, body = get("/loki/api/v1/query_range", rng(`{app="a", env="b", pod="c"}`, now.Add(-time.Minute), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "max streams matchers per query exceeded, matchers-count > limit (3 > 2)")

	code, body = get("/loki/api/v1/query_range", rng(`{env="b"}`, now.Add(-time.Minute), now, 10))
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "stream selector is missing required matchers [app]")

	// Entirely before max_query_lookback: Loki answers with an empty result.
	code, body = get("/loki/api/v1/query_range", rng(`{app="a"}`, now.Add(-72*time.Hour), now.Add(-71*time.Hour), 10))
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"result":[]`)

	// Metric queries get the matcher checks too.
	code, _ = get("/loki/api/v1/query_range", url.Values{"query": {`count_over_time({env="b"}[1m])`}, "start": {ns(now.Add(-time.Minute))}, "end": {ns(now)}, "step": {"60"}})
	require.Equal(t, http.StatusBadRequest, code)
}

func TestQueryTimeout(t *testing.T) {
	srv, _ := newServerConnWithLimits(t, true, func(l *validation.Limits) { l.QueryTimeout = model.Duration(time.Nanosecond) })
	now := time.Now()
	code, _ := do(t, http.MethodGet, srv.URL+"/loki/api/v1/query_range?"+url.Values{
		"query": {`{app="a"}`}, "start": {ns(now.Add(-time.Hour))}, "end": {ns(now)},
	}.Encode(), "t1", "")
	require.Equal(t, http.StatusGatewayTimeout, code, "Loki maps a query timeout to 504")
}
```
`ns` is `func ns(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }`; define it in `api_test.go` if it does not exist. Run: expected FAIL.

- [ ] **Step 4: Implement `internal/api/limits.go`**

Copy the following from Loki @ `fbc3f077`, with one attribution header listing all sources:
- `validateMaxEntriesLimits` and `validateMatchers` from `pkg/querier/queryrange/limits.go`, with their error templates: `requiredLabelsErrTmpl`, `requiredNumberLabelsErrTmpl`, and `validation.ErrMaxEntriesLimit` from `pkg/util/validation`;
- the matcher and entry-limit part of `pkg/querier/queryrange/roundtrip.go` `Do`;
- the lookback and length part of `limitsMiddleware.Do`;
- the matcher-count check of `pkg/querier/limits/validation.go` `ValidateQueryRequest`.
```go
// checkQuery applies Loki's query limits to expr over [start, end] with the
// entry limit: required labels and entry limits (query-frontend roundtrip),
// max_query_lookback and max_query_length (limits middleware) and
// max_streams_matchers_per_query (querier). It returns the start clamped to
// the lookback, and empty=true when the whole range lies before it (Loki
// answers with an empty result).
func (a *API) checkQuery(ctx context.Context, expr syntax.Expr, start, end time.Time, limit uint32) (time.Time, bool, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return start, false, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	var groups [][]*labels.Matcher
	switch e := expr.(type) {
	case syntax.SampleExpr:
		gs, _ := e.MatcherGroups() // Loki ignores the error here too ("handled later")
		for _, g := range gs {
			groups = append(groups, g.Matchers)
		}
	case syntax.LogSelectorExpr:
		if err := validateMaxEntriesLimits(ctx, limit, a.Limits, tenants); err != nil {
			return start, false, err
		}
		groups = append(groups, e.Matchers())
	}
	for _, m := range groups {
		if err := validateMatchers(ctx, a.Limits, tenants, m); err != nil {
			return start, false, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
		}
		for _, id := range tenants {
			if maxMatchers := a.Limits.MaxStreamsMatchersPerQuery(ctx, id); len(m) > maxMatchers {
				return start, false, httpgrpc.Errorf(http.StatusBadRequest,
					"max streams matchers per query exceeded, matchers-count > limit (%d > %d)", len(m), maxMatchers)
			}
		}
	}
	lookback := validation.SmallestPositiveNonZeroDurationPerTenant(tenants, func(id string) time.Duration { return a.Limits.MaxQueryLookback(ctx, id) })
	if lookback > 0 {
		minStart := time.Now().Add(-lookback)
		if end.Before(minStart) {
			return start, true, nil
		}
		if start.Before(minStart) {
			start = minStart
		}
	}
	length := validation.SmallestPositiveNonZeroDurationPerTenant(tenants, func(id string) time.Duration { return a.Limits.MaxQueryLength(ctx, id) })
	if length > 0 {
		if l := timestamp.Time(end.UnixMilli()).Sub(timestamp.Time(start.UnixMilli())); l > length {
			return start, false, fmt.Errorf("%w: %w", logqlmodel.ErrMaxQueryLength,
				httpgrpc.Errorf(http.StatusBadRequest, util_validation.ErrQueryTooLong, l, model.Duration(length)))
		}
	}
	return start, false, nil
}

// emptyResult is Loki's NewEmptyResponse for a query: no streams, or an empty
// vector or matrix.
func emptyResult(expr syntax.Expr, instant bool) parser.Value {
	if _, ok := expr.(syntax.SampleExpr); ok {
		if instant {
			return promql.Vector{}
		}
		return promql.Matrix{}
	}
	return logqlmodel.Streams{}
}
```
In `query.go`, `rangeQuery` and `instantQuery` call `checkQuery` after `NewLiteralParams`, which reports parse errors exactly as before:
- on error, `serverutil.WriteError(err, w)`;
- on `empty`, write `emptyResult` with `marshal.WriteQueryResponseJSON(v, nil, stats.Result{}, w, flags)`;
- if the start changed, rebuild the params with `logql.NewLiteralParams` and the new start.

`validateMaxEntriesLimits` and `validateMatchers` take the tenant list as a parameter in the copy. That modification goes into the attribution line.

The query timeout is already applied by Loki's engine `Exec` (`limits.QueryTimeout`). Run `TestQueryTimeout`. If the status is not 504, `serverutil.WriteError` gets a wrapped error that it cannot map. Check how `exec` wraps it and pass the engine's error through unchanged.

Run: `go test ./internal/api/ ./internal/ingest/ -count=1`. Expected: PASS.

- [ ] **Step 5: Docs, divergences and gates**

- `docs/operations.md`, new section "Limits": which `limits_config` keys lokihouse enforces and where:
  - ingestion: `ingestion_rate_mb`, `ingestion_burst_size_mb` and all existing validation limits;
  - query: `max_query_length`, `max_query_lookback`, `max_entries_limit_per_query`, `max_streams_matchers_per_query`, `required_labels`, `minimum_labels_number`, `query_timeout`, `max_query_series`, `max_query_range`.
- `docs/divergences.md`, section "Hardening (Phase 5)":
  - `ingestion_rate_strategy: global` behaves like `local`: no distributor ring, so with several lokihouse instances behind a load balancer each enforces the full limit;
  - per-policy rate limits (`policy_stream_mapping`) are not supported.

```bash
make check && make test-cluster && make oracle-all && make oracle-cluster
git add internal docs
git commit -m "feat: Loki ingestion rate limit and query limits"
```

---

### Task 10: Multi-tenant queries

**Files:**
- Create: `internal/multitenant/querier.go`, `internal/multitenant/querier_test.go`, `test/oracle/multitenant_test.go`
- Modify:
  - `internal/config/config.go`, `internal/config/config_test.go`
  - `internal/pushdown/engine.go`, `internal/pushdown/engine_test.go`
  - `internal/api/api.go`, `internal/api/metadata.go`
  - `internal/server/server.go`
  - `test/oracle/oracle_test.go`, `test/oracle/loki.go` (config), `test/oracle/lokihouse.go` (config)
  - `docs/configuration.md`, `docs/operations.md`, `docs/divergences.md`

**Interfaces:**
- Consumes: `chstore.Querier` (`SelectLogs`, `SelectSamples`, `Label`, `Series`), `checkQuery` (Task 9, already multi-tenant aware).
- Produces:
  - `config.Config.Querier.MultiTenantQueriesEnabled bool` (`querier.multi_tenant_queries_enabled`, default false).
  - `multitenant.Store` interface (`logql.Querier` plus `Label` and `Series`) and `multitenant.Querier{Next Store}`, which implements `Store`.
  - `api.API.Meta api.MetaQuerier` (`interface{ Label(...); Series(...) }`); when nil, `a.Q` is used.

- [ ] **Step 1: Write the failing tests**

`internal/multitenant/querier_test.go` tests through the HTTP API with a real ClickHouse. Build the API as `server.newAPI` will after Step 3: push to `t1` and `t2`, then:
```go
func TestMultiTenant_QueryLabelsSeries(t *testing.T) {
	srv := newMTServer(t, true) // multi_tenant_queries_enabled
	now := time.Now()
	pushOne(t, srv.URL, "t1", now, `{app="a"}`, "one")
	pushOne(t, srv.URL, "t2", now, `{app="a"}`, "two")

	body := query(t, srv.URL, "t1|t2", `{app="a"}`, now)
	require.Contains(t, body, `"__tenant_id__":"t1"`)
	require.Contains(t, body, `"__tenant_id__":"t2"`)

	body = query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__="t2"}`, now)
	require.NotContains(t, body, `"t1"`)
	require.Contains(t, body, "two")

	body = query(t, srv.URL, "t1|t2", `sum by (__tenant_id__) (count_over_time({app="a"}[5m]))`, now)
	require.Contains(t, body, `"__tenant_id__":"t1"`)

	require.Contains(t, labels(t, srv.URL, "t1|t2", now), "__tenant_id__")
	require.Contains(t, series(t, srv.URL, "t1|t2", `{app="a"}`, now), `"__tenant_id__":"t2"`)
}

func TestMultiTenant_NoForeignTenant(t *testing.T) {
	srv := newMTServer(t, true)
	now := time.Now()
	pushOne(t, srv.URL, "c", now, `{app="a"}`, "secret")
	body := query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__="c"}`, now)
	require.NotContains(t, body, "secret")
	body = query(t, srv.URL, "t1|t2", `{app="a", __tenant_id__=~".+"}`, now)
	require.NotContains(t, body, "secret")
}

func TestMultiTenant_DisabledRejects(t *testing.T) {
	srv := newMTServer(t, false)
	now := time.Now()
	pushOne(t, srv.URL, "t1", now, `{app="a"}`, "one")
	code, body := queryStatus(t, srv.URL, "t1|t2", `{app="a"}`, now)
	require.NotEqual(t, http.StatusOK, code)
	require.Contains(t, body, "multiple org IDs present")
}

func TestMultiTenant_PushdownModes(t *testing.T) {
	for _, mode := range []string{"on", "downstream", "off"} {
		t.Run(mode, func(t *testing.T) {
			srv := newMTServerMode(t, mode)
			now := time.Now()
			pushOne(t, srv.URL, "t1", now, `{app="a"}`, "one")
			pushOne(t, srv.URL, "t2", now, `{app="a"}`, "two")
			body := query(t, srv.URL, "t1|t2", `sum by (__tenant_id__) (count_over_time({app="a"}[5m]))`, now)
			require.Contains(t, body, `"__tenant_id__":"t1"`)
			require.Contains(t, body, `"__tenant_id__":"t2"`)
		})
	}
}
```
Write the helpers (`newMTServer`, `newMTServerMode`, `pushOne`, `query`, `queryStatus`, `labels`, `series`) in the same file. Build them like `internal/api/api_test.go` `newServerConnWithLimits`, plus the `pushdown.Engine` from `server.newAPI`. Run: expected compile FAIL.

- [ ] **Step 2: Implement `internal/multitenant/querier.go`**

Copy from `pkg/querier/multi_tenant_querier.go` @ `fbc3f077`:
- `SelectLogs`, `SelectSamples`, `Label`, `Series`;
- `removeTenantSelector`, `replaceMatchers`, `filterValuesByMatchers`, `sliceToSet`, `relabel`/`relabel.relabel`, and the `defaultTenantLabel` and `retainedLabel` constants.

Use Loki's exported `querier.NewTenantEntryIterator` and `querier.NewTenantSampleIterator` instead of copying them. Header:
```go
// Copied from grafana/loki@fbc3f077 pkg/querier/multi_tenant_querier.go
// (SelectLogs, SelectSamples, Label, Series and their helpers); modifications:
// wraps lokihouse's Store instead of Loki's querier.Querier; store-chunk
// overrides (partitionChunkRefsByTenant) removed: lokihouse has no chunks.
```
```go
// Store is what lokihouse's ClickHouse querier offers per tenant.
type Store interface {
	logql.Querier
	Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)
	Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error)
}

// Querier answers queries whose X-Scope-OrgID names several tenants
// ("a|b"): one sub-query per tenant, each result labelled __tenant_id__.
// Single-tenant requests pass straight through to Next.
type Querier struct{ Next Store }
```

- [ ] **Step 3: Wire it in**

- `config.go`: add `Querier struct { MultiTenantQueriesEnabled bool \`yaml:"multi_tenant_queries_enabled"\` } \`yaml:"querier"\``, with a config test for default false and set true. Add the key to `docs/configuration.md` (the drift test requires it).
- `pushdown/engine.go` `Query`, at the top:
```go
	// Tier A SQL is single-tenant; multi-tenant queries run on Tier B, whose
	// querier fans out per tenant (internal/multitenant).
	if ids, err := tenant.TenantIDs(ctx); err == nil && len(ids) > 1 {
		return e.TierB.Query(p)
	}
```
  with an `engine_test.go` case asserting that `TierALeaves` does not increase for a two-tenant context.
- `server.newAPI`: when `cfg.Querier.MultiTenantQueriesEnabled`, build the Tier B engine over `&multitenant.Querier{Next: q}` and set `a.Meta` to the same value. Otherwise keep `q` and leave `a.Meta` nil.
- `api/metadata.go`: call `a.meta().Label` and `a.meta().Series`, where `meta()` returns `a.Meta` if set, else `a.Q`.
- Endpoints not covered by the copy (index stats, volume, detected_*, patterns, tail, deletes, rules, push) keep their single-tenant behaviour and reject `a|b` with the single-tenant error.

Run the Step 1 tests. Expected: PASS.

- [ ] **Step 3b: Oracle comparison**

Enable the key in both configs:
- in `test/oracle/loki.go` `lokiConfigTemplate`, add `multi_tenant_queries_enabled: true` to the existing `querier:` block, or add the block if there is none;
- in `test/oracle/lokihouse.go`'s config, add `querier: {multi_tenant_queries_enabled: true}`.

Create `test/oracle/multitenant_test.go` with `multiTenantOracle(t, loki, house string, start, end time.Time)`, called from `TestOracle` after the existing endpoint checks. It compares Loki and lokihouse with `Compare` for tenant header `tenant-a|tenant-b`:
- `query_range` `{app="api"}` limit 100;
- `query_range` `sum by (__tenant_id__, app) (count_over_time({app=~".+"}[5m]))` step 60;
- `query_range` `{app="api", __tenant_id__="tenant-b"}`;
- `labels`;
- `label/__tenant_id__/values`;
- `series` `match[]={app="api"}`.

Run `make oracle-all`. Expected: PASS. A mismatch is a bug to fix (or, after analysis, a divergence entry), never a test to loosen.

- [ ] **Step 4: Docs, divergences, gates, commit**

- `docs/operations.md`, section "Multi-tenant queries": the config key, the header syntax, the `__tenant_id__` label, and which endpoints support it.
- `docs/divergences.md`: Loki's multi-tenant querier also covers index stats, volume, detected fields and labels, and patterns; lokihouse answers those for one tenant only. Multi-tenant queries always run on Tier B (no SQL push-down).

```bash
make check && make test-cluster && make oracle-all && make oracle-cluster
git add internal test docs
git commit -m "feat: multi-tenant queries (querier.multi_tenant_queries_enabled)"
```

---

### Task 11: Metrics endpoint and query stats

**Files:**
- Create: `internal/api/instrument.go`, `internal/api/instrument_test.go`, `internal/chstore/stats.go`, `internal/chstore/stats_test.go`
- Modify:
  - `internal/api/api.go` (`Metrics` field, route, instrumentation)
  - `internal/ingest/handler.go` (received counters)
  - every ClickHouse query call site of `chstore.Querier` and `pushdown.TierA`
  - `internal/ruler/ruler.go`, `internal/server/server.go`, `internal/server/server_test.go`
  - `docs/operations.md`, `docs/divergences.md`

**Interfaces:**
- Consumes: `ingest` `Result.Bytes`/`Lines` (Task 9), the ruler registry (Task 2).
- Produces:
  - `GET /metrics` (no tenant): Prometheus default registry plus the ruler registry, through `api.API.Metrics http.Handler`;
  - metrics (Loki names):
    - `loki_request_duration_seconds{method,route,status_code,ws}` and `loki_inflight_requests{method,route}`;
    - `loki_distributor_bytes_received_total{tenant}`, `loki_distributor_lines_received_total{tenant}`;
    - Loki's existing `loki_discarded_samples_total` and `loki_discarded_bytes_total`;
    - `loki_ruler_*` (ruler namespace changed from `lokihouse` to Loki's `loki`);
    - `lokihouse_build_info`;
  - `chstore.WithQueryStats(ctx context.Context) context.Context`: adds ClickHouse progress (rows, bytes read) to Loki's stats context in `ctx`.

- [ ] **Step 1: Write the failing tests**

`internal/server/server_test.go`:
```go
func TestRun_Metrics(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, defaultServer, fmt.Sprintf("ruler_storage: {backend: filesystem, filesystem: {dir: %s}}\nruler: {rule_path: %s}\n",
		filepath.Join(dir, "rules"), filepath.Join(dir, "rules-temp")))
	r := start(t, cfg, log.NewNopLogger())
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(time.Now(), "hello")))
	require.Equal(t, http.StatusBadRequest, push(t, r.addr, "t1", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`))

	resp, err := http.Get("http://" + r.addr + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, _ := io.ReadAll(resp.Body)
	m := string(b)
	for _, want := range []string{
		`loki_request_duration_seconds_count{method="POST",route="loki_api_v1_push",status_code="204",ws="false"}`,
		`loki_distributor_lines_received_total{tenant="t1"} 1`,
		`loki_discarded_samples_total{`,
		`loki_ruler_sync_rules_total`,
		`lokihouse_build_info{`,
	} {
		require.Contains(t, m, want)
	}
}
```
Counter values accumulate across tests in one process because the collectors are global. If another server test in the package also pushes for `t1`, the exact `1` breaks: use a unique tenant name (`"metrics-t1"`) in this test.

`internal/chstore/stats_test.go`, through the API in all push-down modes. Reuse Task 10's `newMTServerMode` pattern or the `api_test.go` helpers:
```go
func TestQueryStats_ReportsClickHouseWork(t *testing.T) {
	for _, mode := range []string{"off", "downstream", "on"} {
		t.Run(mode, func(t *testing.T) {
			// push 10 lines for t1, then:
			body := queryRange(t, srv, "t1", `sum(count_over_time({app="a"}[5m]))`)
			var res struct {
				Data struct {
					Stats struct {
						Summary struct {
							TotalBytesProcessed int64 `json:"totalBytesProcessed"`
							TotalLinesProcessed int64 `json:"totalLinesProcessed"`
						} `json:"summary"`
					} `json:"stats"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal([]byte(body), &res))
			require.Positive(t, res.Data.Stats.Summary.TotalBytesProcessed)
			require.GreaterOrEqual(t, res.Data.Stats.Summary.TotalLinesProcessed, int64(10))
		})
	}
}
```
Run: expected FAIL.

- [ ] **Step 2: Implement metrics**

`internal/api/instrument.go`:
```go
package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/felixge/httpsnoop"
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Loki's (dskit server's) HTTP metrics, same names, labels and buckets.
var (
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loki",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100},
	}, []string{"method", "route", "status_code", "ws"})
	inflightRequests = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "loki",
		Name:      "inflight_requests",
		Help:      "Current number of inflight requests.",
	}, []string{"method", "route"})
)

// instrument records Loki's request metrics for every request mux serves.
// httpsnoop keeps http.Hijacker working for the tail WebSocket.
func instrument(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		route := routeName(pattern)
		ws := strconv.FormatBool(strings.EqualFold(r.Header.Get("Upgrade"), "websocket"))
		g := inflightRequests.WithLabelValues(r.Method, route)
		g.Inc()
		defer g.Dec()
		m := httpsnoop.CaptureMetrics(mux, w, r)
		requestDuration.WithLabelValues(r.Method, route, strconv.Itoa(m.Code), ws).Observe(m.Duration.Seconds())
	})
}

// routeName turns a ServeMux pattern ("GET /loki/api/v1/query") into dskit's
// route label ("loki_api_v1_query"); unmatched requests are "other".
func routeName(pattern string) string {
	if pattern == "" {
		return "other"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		pattern = path
	}
	return middleware.MakeLabelValue(pattern)
}
```
Check the bucket list against dskit's `server.go` (`instrument.DefBuckets` or the configured `HTTPServerRequestDurationBuckets` default) at the pinned dskit version and copy it exactly. Add an `instrument_test.go` table test for `routeName`: `"" → other`, `"GET /loki/api/v1/query" → loki_api_v1_query`, and `"GET /loki/api/v1/label/{name}/values"` → what `MakeLabelValue` returns.

In `api.Router()`: `if a.Metrics != nil { mux.Handle("GET /metrics", a.Metrics) }`, then `return instrument(mux)`.

In `ingest/handler.go`, after a successful write:
```go
	bytesReceived.WithLabelValues(tenantID).Add(float64(res.Bytes))
	linesReceived.WithLabelValues(tenantID).Add(float64(res.Lines))
```
Define both with `promauto` (`Namespace: "loki"`, names `distributor_bytes_received_total` and `distributor_lines_received_total`, Loki's help texts from `pkg/distributor/distributor.go`).

In `server.Run`:
- create `rulerReg := prometheus.NewRegistry()` and pass it to `lhruler.New`;
- register `versioncollector.NewCollector("lokihouse")` on the default registerer, ignoring `prometheus.AlreadyRegisteredError` (several `Run`s in one test process);
- set `a.Metrics = promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, rulerReg}, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})`.

In `internal/ruler/ruler.go`, change the `NewRuler` namespace argument from `"lokihouse"` to `"loki"` (Loki's `-metrics.namespace` default).

- [ ] **Step 3: Implement query stats**

`internal/chstore/stats.go`:
```go
package chstore

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
)

// WithQueryStats returns ctx with a ClickHouse progress callback that adds the
// rows and uncompressed bytes the server read to Loki's stats context in ctx
// (summary.totalLinesProcessed / totalBytesProcessed). ClickHouse reports
// progress as increments; existing query options in ctx are kept.
func WithQueryStats(ctx context.Context) context.Context {
	st := stats.FromContext(ctx)
	return clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) {
		st.AddDecompressedLines(int64(p.Rows))  //nolint:gosec // G115: row counts fit in int64
		st.AddDecompressedBytes(int64(p.Bytes)) //nolint:gosec // G115: byte counts fit in int64
	}))
}
```
Call `ctx = WithQueryStats(ctx)` before every `conn.Query`/`QueryRow`/`Select` in `chstore.Querier.SelectLogs` and `SelectSamples` and in `pushdown.TierA.Exec`. Find them with `grep -n "Conn.Query\|conn.Query\|\.Select(" internal/chstore/*.go internal/pushdown/tiera.go`. Metadata endpoints do not report stats in Loki, so skip them.

Check first that `clickhouse.Context` merges with options already in `ctx` (for example settings). Read `clickhouse-go/v2@v2.49.0/context.go` `Context`. If it replaces them, pass the existing options through.

If the Tier A stats are double-counted or missing (the downstream engine merges leaf `Result.Statistics`), make `TierA.Exec` record into a fresh `stats.NewContext` and return its `Result` in `logqlmodel.Result.Statistics`, which Loki's downstream evaluator merges. Choose whichever makes `TestQueryStats_ReportsClickHouseWork` pass with the values ≤ 2× the true row count in every mode.

Run the Step 1 tests. Expected: PASS.

- [ ] **Step 4: Docs, divergences, gates, commit**

- `docs/operations.md`, section "Metrics": the endpoint, the metric list, a scrape config example.
- `docs/divergences.md`:
  - `totalBytesProcessed`/`totalLinesProcessed` are ClickHouse's `read_bytes`/`read_rows` (granule-level, every column read), not Loki's decompressed chunk bytes and lines, so the values differ from Loki's for the same data;
  - the distributor counters carry only the `tenant` label (Loki also has `retention_hours`, `aggregated_metric`, `policy` and `format`);
  - `loki_ruler_*` names now match Loki's (the Phase 4 note "ruler metrics not exposed" is resolved; update that entry).

```bash
make check && make test-cluster && make oracle-all
git add internal docs go.mod go.sum
git commit -m "feat: /metrics (Loki request, distributor, ruler metrics) and ClickHouse query stats"
```

---

### Task 12: Benchmark against Loki

**Files:**
- Create: `test/oracle/bench_test.go`, `docs/benchmark.md` (replaces the Task 6 stub)
- Modify: `Makefile` (`bench`), `docs/development.md`

**Interfaces:**
- Consumes: `StartLoki`, `StartLokihouse`, `PushBoth`, `Get`, `Compare` (oracle harness).
- Produces:
  - `make bench`, which writes a Markdown results table to `$LOKIHOUSE_BENCH_OUT` (default `.cache/bench/results.md`);
  - `docs/benchmark.md` with the method, the environment and one recorded result.

- [ ] **Step 1: Write the benchmark**

`test/oracle/bench_test.go`:
```go
//go:build oracle && bench

package oracle

import (
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

// benchData pushes lines spread over streams and the last hour, in batches,
// to both stores. Deterministic (seeded).
func benchData(t *testing.T, urls []string, lines, streams int, end time.Time) {
	rnd := rand.New(rand.NewSource(7))
	const batch = 5000
	levels := []string{"info", "warn", "error", "debug"}
	for off := 0; off < lines; off += batch {
		var b strings.Builder
		b.WriteString(`{"streams":[`)
		for s := 0; s < streams; s++ {
			if s > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"stream":{"app":"app%d","env":"%s"},"values":[`, s%10, []string{"dev", "prod"}[s%2])
			first := true
			for i := off + s; i < off+batch && i < lines; i += streams {
				if !first {
					b.WriteString(",")
				}
				first = false
				ts := end.Add(-time.Hour).Add(time.Duration(i) * time.Hour / time.Duration(lines))
				fmt.Fprintf(&b, `[%q,"level=%s status=%d duration=%dms msg=\"request %d\""]`,
					strconv.FormatInt(ts.UnixNano(), 10), levels[rnd.Intn(4)], []int{200, 404, 500}[rnd.Intn(3)], rnd.Intn(900), i)
			}
			b.WriteString("]}")
		}
		b.WriteString("]}")
		PushBoth(t, urls, PushBatch{Tenant: "bench", Body: []byte(b.String())})
	}
}

var benchQueries = []struct{ name, path, query string }{
	{"log filter", "query_range", `{app="app1"} |= "status=500"`},
	{"logfmt filter", "query_range", `{env="prod"} | logfmt | status >= 500`},
	{"count_over_time", "query_range", `sum by (app) (count_over_time({env=~".+"}[5m]))`},
	{"rate regex", "query_range", `sum(rate({app=~"app[1-3]"} |~ "level=(warn|error)" [1m]))`},
	{"topk", "query_range", `topk(3, sum by (app) (rate({env="prod"}[5m])))`},
	{"unwrap quantile", "query_range", `quantile_over_time(0.95, {env="prod"} | logfmt | unwrap duration(duration) [5m]) by (app)`},
	{"bytes_over_time", "query_range", `sum(bytes_over_time({env="dev"}[10m]))`},
	{"labels", "labels", ``},
	{"series", "series", `{env="prod"}`},
}

func TestBench(t *testing.T) {
	lines := envInt("LOKIHOUSE_BENCH_LINES", 200000)
	streams := envInt("LOKIHOUSE_BENCH_STREAMS", 50)
	rounds := envInt("LOKIHOUSE_BENCH_ROUNDS", 5)
	loki := StartLoki(t, "")
	house := StartLokihouse(t, "")
	end := time.Now().Truncate(time.Minute)
	benchData(t, []string{loki, house}, lines, streams, end)
	// Make sure Loki has everything (the oracle's readiness wait, scaled).
	waitBenchQueryable(t, loki, lines, end)

	var out strings.Builder
	fmt.Fprintf(&out, "| query | Loki p50 | Loki p95 | lokihouse p50 | lokihouse p95 |\n|---|---:|---:|---:|---:|\n")
	for _, q := range benchQueries {
		u := func(base string) string {
			v := url.Values{"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			switch q.path {
			case "query_range":
				v.Set("query", q.query)
				v.Set("step", "60")
				v.Set("limit", "1000")
			case "series":
				v.Set("match[]", q.query)
			}
			return base + "/loki/api/v1/" + q.path + "?" + v.Encode()
		}
		lt, lb := timeIt(t, u(loki), rounds)
		ht, hb := timeIt(t, u(house), rounds)
		Compare(t, "bench "+q.name, lb, hb) // a benchmark of wrong answers is worthless
		fmt.Fprintf(&out, "| %s | %s | %s | %s | %s |\n", q.name, pct(lt, 50), pct(lt, 95), pct(ht, 50), pct(ht, 95))
	}
	p := os.Getenv("LOKIHOUSE_BENCH_OUT")
	if p == "" {
		p = filepath.Join("..", "..", ".cache", "bench", "results.md")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	header := fmt.Sprintf("%d lines, %d streams, %d rounds per query, %s\n\n", lines, streams, rounds, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(p, []byte(header+out.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("results in %s\n%s", p, out.String())
}

func timeIt(t *testing.T, u string, rounds int) ([]time.Duration, []byte) {
	var ds []time.Duration
	var body []byte
	for i := 0; i < rounds; i++ {
		start := time.Now()
		body = Get(t, u, "bench")
		ds = append(ds, time.Since(start))
	}
	return ds, body
}

func pct(ds []time.Duration, p int) string {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[(len(s)-1)*p/100].Round(time.Millisecond).String()
}

func waitBenchQueryable(t *testing.T, loki string, lines int, end time.Time) {
	v := url.Values{"query": {`sum(count_over_time({env=~".+"}[2h]))`}, "time": {strconv.FormatInt(end.Add(time.Minute).UnixNano(), 10)}}
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if strings.Contains(string(Get(t, loki+"/loki/api/v1/query?"+v.Encode(), "bench")), fmt.Sprintf(`"%d"`, lines)) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("loki did not report %d lines within 10m", lines)
}
```
`Get` fails on non-200/400 responses. If Loki rejects a query (limits), raise the limit in both configs through the `extra` argument instead of removing the query. Loki's default `max_query_series` and `max_entries_limit_per_query` should be fine for this data.

`Makefile`:
```make
.PHONY: bench
bench:
	go test -tags oracle,bench -run TestBench -count=1 -timeout 60m -v ./test/oracle/
```

- [ ] **Step 2: Run it and record the result**

```bash
make bench
```
Expected: PASS, with the table in `.cache/bench/results.md`. Write `docs/benchmark.md`:
- method: the same data and queries for both, results compared for equality, p50/p95 over N rounds;
- environment: CPU (`lscpu | grep 'Model name'`), RAM, ClickHouse 26.9 single node in podman, Loki single binary with filesystem storage, both on the same host;
- caveats: Loki serves recent data from ingester memory, and this measures one node, not scale-out;
- the result table.

Add the bench to `docs/development.md`.

- [ ] **Step 3: Commit**

```bash
make check
git add test/oracle/bench_test.go docs Makefile
git commit -m "test(bench): lokihouse vs Loki latency benchmark; docs: results"
```

---

### Task 13: Memory and goroutine leak checks

lokihouse is pure Go (`CGO_ENABLED=0`, garbage collected), so valgrind does not apply: it sees the Go runtime's own allocator, not leaks. The Go equivalents:
- **pprof** heap and goroutine profiles from a long-running, loaded process (a soak run);
- **goleak**, which fails a test that leaves goroutines behind;
- the race detector, which `make cover` already runs.

**Files:**
- Create: `internal/server/main_test.go`, `internal/tail/main_test.go`, `internal/ruler/main_test.go`, `internal/api/main_test.go`, `test/oracle/soak_test.go`, `internal/api/pprof_test.go`
- Modify:
  - `internal/config/config.go`, `internal/config/config_test.go`
  - `internal/api/api.go` (pprof routes; `/metrics` and pprof gated)
  - `internal/server/server.go`
  - `Makefile` (`soak`), `.github/workflows/nightly.yml` (soak job)
  - `docs/configuration.md`, `docs/operations.md` ("Profiling"), `docs/development.md`, `AGENTS.md` (leak rule)
  - `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `server.Run` (Task 2), `/metrics` (Task 11), `StartLokihouse(t, extra)` (oracle harness).
- Produces:
  - `config.Config.Server.RegisterInstrumentation bool` (`server.register_instrumentation`, Loki/dskit default true). When true, `GET /metrics` and `/debug/pprof/*` are served (Loki parity); when false, neither is.
  - `api.API.Pprof bool`.
  - `make soak`, which writes heap and goroutine profiles and a samples CSV to `$LOKIHOUSE_SOAK_OUT` (default `.cache/soak/`).

- [ ] **Step 1: Write the failing pprof and config tests**

`internal/api/pprof_test.go`:
```go
func TestPprofRoutes(t *testing.T) {
	a := &api.API{Pprof: true, AuthEnabled: true}
	srv := httptest.NewServer(a.Router())
	defer srv.Close()
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/heap?gc=1", "/debug/pprof/goroutine?debug=1"} {
		code, body := do(t, http.MethodGet, srv.URL+p, "", "")
		require.Equal(t, http.StatusOK, code, p)
		require.NotEmpty(t, body, p)
	}
	off := httptest.NewServer((&api.API{AuthEnabled: true}).Router())
	defer off.Close()
	code, _ := do(t, http.MethodGet, off.URL+"/debug/pprof/heap", "", "")
	require.Equal(t, http.StatusNotFound, code)
}
```
In `internal/config/config_test.go`, test that `register_instrumentation` defaults to `true` and that `false` is honoured. Run: expected FAIL.

- [ ] **Step 2: Implement the key and the routes**

Add `RegisterInstrumentation bool \`yaml:"register_instrumentation"\`` to `Server`, default `true`, and document it in `docs/configuration.md` (the drift test requires it).

In `api.Router()`, with `import "net/http/pprof"`:
```go
	if a.Pprof {
		// Loki (dskit server) serves the Go profiler on its HTTP port while
		// server.register_instrumentation is true; same here, without a tenant.
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
```
`pprof.Index` serves the named profiles (`heap`, `goroutine`, `allocs`, …) under `/debug/pprof/`. In `server.Run`, set `a.Pprof = cfg.Server.RegisterInstrumentation` and set `a.Metrics` only when it is true.

The CPU profile and trace endpoints run for up to their `seconds` parameter. `server.http_server_write_timeout` (30 s default) bounds them, as in Loki. Note this in `docs/operations.md` "Profiling", along with:
- how to take profiles: `go tool pprof http://host:3100/debug/pprof/heap`, and `-base` to diff two heap snapshots;
- the advice to keep port 3100 off untrusted networks or set `register_instrumentation: false`, since pprof exposes internals.

Run the tests. Expected: PASS.

- [ ] **Step 3: goleak in the packages that start goroutines**

`go get go.uber.org/goleak@v1.3.0` (Loki's version). For each of `internal/server`, `internal/tail`, `internal/ruler` and `internal/api`, add `main_test.go`:
```go
package server_test // tail_test / ruler_test / api_test respectively

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}
```
and, in the same file, `leakIgnores()` returning the `goleak.IgnoreTopFunction(...)` / `goleak.IgnoreAnyFunction(...)` options for goroutines that are not lokihouse's to stop. Each ignore gets a comment naming its owner, for example:
- the ClickHouse driver's connection-pool janitor, which `conn.Close` stops (if it remains, a test forgot to close a connection: fix the test, do not ignore it);
- Loki and Prometheus package-level workers started in `init`;
- `net/http` keep-alive connections of `http.DefaultClient`. Call `http.DefaultClient.CloseIdleConnections()` in a `t.Cleanup` of the helpers that use it instead of ignoring them.

Process: run `go test ./internal/server/ -count=1`. goleak prints every leaked goroutine with its stack. Classify each one:
1. A lokihouse goroutine (`github.com/bzed/lokihouse/...` in the stack) is a **bug**: fix the code so it stops on shutdown or context cancel, and keep the test.
2. A goroutine owned by a dependency we hold a handle to is a missing `Close`/`Stop` in lokihouse: fix it.
3. A process-lifetime goroutine of a dependency (started in `init`, or a global cache) gets an ignore with a comment.

Repeat until the four packages pass. Run with `-count=3` as well: a leak that only shows on the second run is a leak.

- [ ] **Step 4: Write the soak test**

`test/oracle/soak_test.go`:
```go
//go:build oracle && soak

package oracle

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// metricValue returns the value of an unlabelled metric from /metrics.
func metricValue(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[0] == name {
			v, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// saveProfile stores /debug/pprof/<name> (after a GC for heap) in dir.
func saveProfile(t *testing.T, base, name, dir, tag string) {
	t.Helper()
	q := ""
	if name == "heap" {
		q = "?gc=1"
	}
	resp, err := http.Get(base + "/debug/pprof/" + name + q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	f, err := os.Create(filepath.Join(dir, tag+"-"+name+".pb.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		t.Fatal(err)
	}
}

// TestSoak runs a mixed workload against lokihouse for LOKIHOUSE_SOAK_DURATION
// (default 10m) and checks that, after a warm-up, the post-GC heap and the
// goroutine count do not grow. Profiles from the end of the warm-up and from
// the end are kept for `go tool pprof -base`.
func TestSoak(t *testing.T) {
	dur, err := time.ParseDuration(envOr("LOKIHOUSE_SOAK_DURATION", "10m"))
	if err != nil {
		t.Fatal(err)
	}
	out := envOr("LOKIHOUSE_SOAK_OUT", filepath.Join("..", "..", ".cache", "soak"))
	if err := os.MkdirAll(out, 0o750); err != nil {
		t.Fatal(err)
	}
	rules := t.TempDir()
	house := StartLokihouse(t, fmt.Sprintf("ruler_storage: {backend: filesystem, filesystem: {dir: %s}}\nruler: {rule_path: %s, poll_interval: 5s, evaluation_interval: 5s}\n",
		filepath.Join(rules, "rules"), filepath.Join(rules, "rules-temp")))
	putRule(t, house)

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	var wg sync.WaitGroup
	for _, w := range []func(context.Context, *testing.T, string){soakPush, soakQuery, soakTail} {
		wg.Go(func() { w(ctx, t, house) })
	}

	type sample struct{ heap, goroutines float64 }
	var samples []sample
	warmup := dur / 5
	tick := time.NewTicker(dur / 40)
	defer tick.Stop()
	start := time.Now()
	warmSaved := false
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			saveProfile(t, house, "heap", t.TempDir(), "gc") // forces a GC before reading the gauges
			s := sample{metricValue(t, house, "go_memstats_heap_inuse_bytes"), metricValue(t, house, "go_goroutines")}
			if time.Since(start) >= warmup {
				if !warmSaved {
					saveProfile(t, house, "heap", out, "warm")
					saveProfile(t, house, "goroutine", out, "warm")
					warmSaved = true
				}
				samples = append(samples, s)
			}
		}
	}
	wg.Wait()
	saveProfile(t, house, "heap", out, "end")
	saveProfile(t, house, "goroutine", out, "end")

	csv := "heap_inuse_bytes,goroutines\n"
	for _, s := range samples {
		csv += fmt.Sprintf("%.0f,%.0f\n", s.heap, s.goroutines)
	}
	if err := os.WriteFile(filepath.Join(out, "samples.csv"), []byte(csv), 0o600); err != nil {
		t.Fatal(err)
	}
	if len(samples) < 10 {
		t.Fatalf("only %d samples; run longer", len(samples))
	}
	// Leak criteria (after warm-up): the last quarter's mean heap is at most
	// 20% above the first quarter's, and goroutines at most 20 above the
	// first sample (live tail connections come and go).
	q := len(samples) / 4
	mean := func(ss []sample) float64 {
		var m float64
		for _, s := range ss {
			m += s.heap
		}
		return m / float64(len(ss))
	}
	first, last := mean(samples[:q]), mean(samples[len(samples)-q:])
	if last > first*1.2 {
		t.Errorf("heap grew from %.1f MiB to %.1f MiB after warm-up; compare: go tool pprof -base %s %s",
			first/(1<<20), last/(1<<20), filepath.Join(out, "warm-heap.pb.gz"), filepath.Join(out, "end-heap.pb.gz"))
	}
	if g0, g1 := samples[0].goroutines, samples[len(samples)-1].goroutines; g1 > g0+20 {
		t.Errorf("goroutines grew from %.0f to %.0f; see %s", g0, g1, filepath.Join(out, "end-goroutine.pb.gz"))
	}
}

func putRule(t *testing.T, house string) {
	t.Helper()
	body := "name: soak\ninterval: 5s\nrules:\n  - record: soak:lines:rate1m\n    expr: sum(rate({app=\"soak\"}[1m]))\n"
	req, _ := http.NewRequest(http.MethodPost, house+"/loki/api/v1/rules/soak", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "soak")
	req.Header.Set("Content-Type", "application/yaml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create rule: %d", resp.StatusCode)
	}
}

// soakPush pushes 100 lines over 10 streams every 100ms.
func soakPush(ctx context.Context, t *testing.T, house string) {
	for i := 0; ctx.Err() == nil; i++ {
		var b strings.Builder
		b.WriteString(`{"streams":[`)
		for s := 0; s < 10; s++ {
			if s > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"stream":{"app":"soak","pod":"p%d"},"values":[`, s)
			for j := 0; j < 10; j++ {
				if j > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, `[%q,"level=info i=%d j=%d msg=\"soak line\""]`, strconv.FormatInt(time.Now().UnixNano(), 10), i, j)
			}
			b.WriteString("]}")
		}
		b.WriteString("]}")
		if err := do(ctx, http.MethodPost, house+"/loki/api/v1/push", "application/json", b.String(), http.StatusNoContent); err != nil {
			if ctx.Err() == nil {
				t.Errorf("push: %v", err)
			}
			return
		}
		sleepCtx(ctx, 100*time.Millisecond)
	}
}

// soakQuery runs log, metric and metadata queries in a loop.
func soakQuery(ctx context.Context, t *testing.T, house string) {
	qs := []string{
		`{app="soak"} |= "soak" | logfmt`,
		`sum by (pod) (count_over_time({app="soak"}[1m]))`,
		`topk(3, sum by (pod) (rate({app="soak"} | logfmt | level="info" [1m])))`,
	}
	for i := 0; ctx.Err() == nil; i++ {
		now := time.Now()
		v := url.Values{"query": {qs[i%len(qs)]}, "start": {strconv.FormatInt(now.Add(-5*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"15"}, "limit": {"500"}}
		for _, u := range []string{house + "/loki/api/v1/query_range?" + v.Encode(), house + "/loki/api/v1/labels"} {
			if err := do(ctx, http.MethodGet, u, "", "", http.StatusOK); err != nil {
				if ctx.Err() == nil {
					t.Errorf("query: %v", err)
				}
				return
			}
		}
		sleepCtx(ctx, 200*time.Millisecond)
	}
}

// soakTail opens a tail, reads for 5s, closes it, and repeats.
func soakTail(ctx context.Context, t *testing.T, house string) {
	u := "ws" + strings.TrimPrefix(house, "http") + "/loki/api/v1/tail?" + url.Values{"query": {`{app="soak"}`}}.Encode()
	for ctx.Err() == nil {
		c, resp, err := websocket.DefaultDialer.DialContext(ctx, u, http.Header{"X-Scope-OrgID": {"soak"}})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Errorf("tail dial: %v", err)
			return
		}
		resp.Body.Close()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				break
			}
		}
		_ = c.Close()
	}
}

// do sends one request as tenant "soak" and checks the status. Worker
// goroutines use it instead of PushBoth/Get, which call t.Fatal (only allowed
// on the test goroutine).
func do(ctx context.Context, method, u, contentType, body string, want int) error {
	req, err := http.NewRequestWithContext(ctx, method, u, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Scope-OrgID", "soak")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: status %d: %s", method, u, resp.StatusCode, b)
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
```
The workers report errors with `t.Errorf` and return (`t.Fatal` is only allowed on the test goroutine). Before writing the file, check with `grep -n "^func do(" test/oracle/*.go` that no other oracle file already defines `do`; if one does, rename this helper.

`StartLokihouse` must start lokihouse with `server.register_instrumentation` at its default (true) so `/metrics` and `/debug/pprof` are available. Check that its config does not disable it.

`Makefile`:
```make
.PHONY: soak
soak:
	go test -tags oracle,soak -run TestSoak -count=1 -timeout 90m -v ./test/oracle/
```

- [ ] **Step 5: Run the soak and fix what it finds**

```bash
LOKIHOUSE_SOAK_DURATION=10m make soak
```
Expected: PASS, with `samples.csv` and the `warm-*`/`end-*` profiles in `.cache/soak/`. If it fails:
```bash
go tool pprof -top -base .cache/soak/warm-heap.pb.gz bin/lokihouse .cache/soak/end-heap.pb.gz
go tool pprof -traces .cache/soak/end-goroutine.pb.gz | head -80
```
The growing allocation sites or goroutine stacks name the leak. Fix it in lokihouse and add a unit test that reproduces it: for example a goroutine count before and after N tail connections, or `goleak` in the package. Do not raise the thresholds to make the soak pass. Record the result (duration, heap first and last quarter, goroutines) in `docs/development.md`, section "Leak checks".

- [ ] **Step 6: Nightly soak in CI and the AGENTS.md rule**

Add a job to `.github/workflows/nightly.yml`, with the same ClickHouse service, checkout and setup-go steps as the `oracle` job:
```yaml
  soak:
    runs-on: ubuntu-24.04
    timeout-minutes: 60
    # services, checkout, setup-go: as in the oracle job
    steps:
      - run: LOKIHOUSE_SOAK_DURATION=30m make soak
      - if: always()
        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: soak-profiles
          path: .cache/soak/
```
Write the job out completely (do not leave the comment line). Validate with actionlint.

Add to `AGENTS.md` under "Safe Go":
```markdown
- **Leaks:** every goroutine has an owner that stops it. Packages that start
  goroutines run `goleak.VerifyTestMain`; an ignore needs a comment naming the
  dependency that owns the goroutine. Changes to long-lived paths (ingest,
  query, tail, ruler) are checked with `make soak` (pprof heap/goroutine
  profiles; valgrind does not apply to Go).
```

- [ ] **Step 7: Gates and commit**

```bash
make check && make test-cluster
git add internal test Makefile .github AGENTS.md docs go.mod go.sum
git commit -m "test: goleak, pprof endpoints (register_instrumentation), soak test for heap/goroutine leaks"
```

---

### Task 14: Phase 5 wrap-up

**Files:**
- Create: `docs/phase5-status.md`, `docs/roadmap.md`
- Modify: `docs/divergences.md`, `mkdocs.yml` (nav: Phase 5, Roadmap), `debian/changelog` (final entry text), `docs/superpowers/specs/2026-10-10-clickhouse-loki-design.md` (§5.1 Cloud row: "deferred, see docs/roadmap.md")

**Interfaces:**
- Consumes: everything above.
- Produces: the phase status with gate results, and the roadmap with deferred work.

- [ ] **Step 1: `docs/roadmap.md`**

```markdown
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
- **Per-policy ingestion limits** (`policy_stream_mapping`), multi-tenant
  support for the index, volume, detected and pattern endpoints: see
  `docs/divergences.md`.
```

- [ ] **Step 2: `docs/phase5-status.md`**

Use the Phase 4 status structure:
- **Exit criteria table:** run every gate on the final commit and paste the real results: `make check` (with the coverage figure), `make test-cluster`, `make oracle-all`, `make oracle-cluster`, `make deb` (amd64 here; arm64 from CI), `make docs`, `make bench`, `make soak`.
- **Feature list per task**, with the config keys.
- **Operations notes:** systemd, shutdown, limits, multi-tenant, metrics.
- **Known issues:** a link to `docs/divergences.md`.

- [ ] **Step 3: Final verification**

```bash
make check
make test-cluster
make oracle-all
make oracle-cluster
make deb
make docs
git status --short   # clean apart from the files of this task
```
Expected: everything passes. Record the results in `docs/phase5-status.md`, not from memory.

- [ ] **Step 4: Commit**

```bash
git add docs mkdocs.yml debian/changelog
git commit -m "docs: phase 5 status, roadmap (ClickHouse Cloud deferred)"
```
Tagging `v0.5.0` and pushing are the maintainer's decision. Do not tag or push as part of this task.

---

## Deferred

- ClickHouse Cloud verification (spec §5.1) → `docs/roadmap.md` (Task 14).
- podman compose example (spec §8 Phase 5 row): out of scope by decision on 2026-10-11.
