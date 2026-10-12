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
- **Leaks:** every goroutine has an owner that stops it. Packages that start
  goroutines run `goleak.VerifyTestMain`; an ignore needs a comment naming the
  dependency that owns the goroutine. Changes to long-lived paths (ingest,
  query, tail, ruler) are checked with `make soak` (pprof heap/goroutine
  profiles; valgrind does not apply to Go).
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
