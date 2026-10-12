# Development

## CI

GitHub Actions (`.github/workflows/`), all on `ubuntu-24.04`:

| Workflow | When | What |
| --- | --- | --- |
| `ci.yml` | push/PR to `main` | `checks` (fmt, vet, lint, vuln); `test` (`make cover` against a ClickHouse 26.9 service container, coverage artifact); `build` (binary + `-version`); `docs` (`make docs man`); `deb` (reusable `deb.yml`, snapshot version). |
| `deb.yml` | called by `ci.yml` and `release.yml` | Builds the `.deb` for amd64 and arm64 in a `debian:trixie` container: pinned Go toolchain (SHA-256 checked), `dpkg-buildpackage`, `lintian --fail-on error,warning`, install + `debian/tests/smoke`. |
| `nightly.yml` | daily 03:17 UTC, manual dispatch | `cluster` (`make test-cluster` on a 4-node podman cluster); `oracle` (`make oracle-all` with a Loki binary cache keyed on the Makefile, which pins `LOKI_COMMIT`); `oracle-cluster`; `soak` (30 min heap/goroutine soak, profiles uploaded). |
| `release.yml` | push of a `v*` tag | `check-version` (tag must equal the version in `debian/changelog`), `deb` (release version), `publish` (`.deb` + `SHA256SUMS` as a GitHub release). |
| `docs.yml` | push to `main` | `mkdocs build --strict` and deploy to GitHub Pages at <https://bzed.github.io/lokihouse/>. |

Release steps (maintainer):

1. `dch -v <x.y.z>` and set the distribution to `trixie`.
2. Commit.
3. `git tag -s v<x.y.z>`.
4. Push the tag.

One-time repository settings (maintainer, not the implementer): Settings →
Pages → Source "GitHub Actions"; branch protection requiring `checks`,
`test`, `build`, `docs` and `deb`.

## Local testing

- `make check` runs the full gate: gofmt/goimports, `go vet`, golangci-lint,
  govulncheck, the unit tests with the race detector, and the coverage floor.
- `make oracle` / `make oracle-all` run the lokihouse-vs-Loki oracle in one
  push-down mode (or all three). Both need a running ClickHouse
  (`podman run -d -p 9000:9000 docker.io/clickhouse/clickhouse-server:26.9`).
- `make bench` runs the latency benchmark against Loki (see
  [Benchmark](benchmark.md)); results land in `.cache/bench/results.md`.

## Leak checks

lokihouse is pure Go (`CGO_ENABLED=0`), so valgrind does not apply (it sees the
runtime's allocator, not leaks). The equivalents are pprof, goleak and the race
detector (which `make check` already runs).

- **goleak**: `internal/api`, `internal/server`, `internal/tail` and
  `internal/ruler` run `goleak.VerifyTestMain`, so any test that leaves a
  goroutine behind fails the package. Each `leakIgnores()` entry names the
  dependency that owns the goroutine.
- **pprof / soak**: `make soak` (build tag `soak`) runs a mixed push/query/tail
  workload against lokihouse for `LOKIHOUSE_SOAK_DURATION` (default 10m),
  samples `go_memstats_heap_inuse_bytes` and `go_goroutines` after a forced GC,
  and fails if post-warm-up heap grows >20% or goroutines grow >20. It writes
  `samples.csv` and `warm-*`/`end-*` profiles to `LOKIHOUSE_SOAK_OUT`
  (default `.cache/soak/`) for `go tool pprof -base`.
- **Profiling in production**: with `server.register_instrumentation: true`
  (the default) the Go profiler is served at `/debug/pprof/*` — see
  [Profiling](operations.md#profiling).

Recorded result (2026-10-12, 10m soak, 10 streams, push+query+tail+ruler,
AMD Ryzen 9 9900X): 32 post-warm-up samples, post-GC heap floor first-quarter
mean ~13.5 MiB, last-quarter mean ~15.6 MiB (no sustained growth, <20%);
goroutines flat at ~26–35. PASS.
