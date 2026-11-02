---
description: How Rúnakefli is built, tested and released - the CI workflows, release steps, the signed apt repository and its key, the container image, local testing and leak checks.
---

# Development

For people who build, test and release Rúnakefli. The rules every change has to
follow (error handling, tests, lint, changelog, commit style) are in
[`AGENTS.md`](https://github.com/bzed/runakefli/blob/main/AGENTS.md), and
[`CONTRIBUTING.md`](https://github.com/bzed/runakefli/blob/main/CONTRIBUTING.md)
says how to contribute.

## CI

GitHub Actions (`.github/workflows/`), all on `ubuntu-24.04`:

| Workflow | When | What |
| --- | --- | --- |
| `ci.yml` | push/PR to `main` | `checks` (fmt, vet, lint, vuln); `test` (`make cover` against a ClickHouse® 26.9 service container; coverage and JUnit report uploaded to Codecov with `CODECOV_TOKEN` and as an artifact); `build` (binary + `-version`); `docs` (`make docs man`); `deb` (reusable `deb.yml`, snapshot version). |
| `codeql.yml` | push/PR to `main`, weekly | CodeQL (security-and-quality) for Go and the workflows, config in `.github/codeql/`. |
| `deb.yml` | called by `ci.yml` and `release.yml` | Builds the `.deb` for amd64 and arm64 in a `debian:trixie` container: pinned Go toolchain (SHA-256 checked), `dpkg-buildpackage`, `lintian --fail-on error,warning`, install + `debian/tests/smoke`. |
| `nightly.yml` | daily 03:17 UTC, manual dispatch | `cluster` (`make test-cluster` on a 4-node podman cluster); `oracle` (`make oracle-all` with a Loki® binary cache keyed on the Makefile, which pins `LOKI_COMMIT`); `oracle-cluster`; `soak` (30 min heap/goroutine soak, profiles uploaded). |
| `release.yml` | push of a `v*` tag | `check-version` (tag must equal the version in `debian/changelog`), `deb` (release version), `docker` (pushes `ghcr.io/bzed/runakefli`), `publish` (`.deb` + `SHA256SUMS` as a GitHub release), `pages` (calls `docs.yml`: apt suite `trixie` and the docs). |
| `docs.yml` | push to `main`, called by `release.yml` | `build` (`mkdocs build --strict`); `deb` (snapshot, not in a release run); `apt` (reprepro, signed with `APT_GPG_PRIVATE_KEY`); `deploy` to GitHub Pages, then `s3` mirrors the site to <https://runakefli.app/> (docs at `/`, the repository at `/apt/`). |
| `docker.yml` | push to `main`, PRs touching the image, called by `release.yml` | `test` builds the image on native amd64 and arm64 runners and runs `scripts/docker-test.sh` against a ClickHouse container; with `push: true` (release only) `publish` pushes the multi-arch image with SBOM and provenance and signs it with cosign (keyless). |

Release steps (maintainer):

1. In `CHANGELOG.md`, move `## [Unreleased]` to `## [x.y.z] - YYYY-MM-DD`.
2. `dch -v <x.y.z>` and set the distribution to `trixie`.
3. Commit.
4. `git tag -s v<x.y.z>`.
5. Push the tag. `release.yml` fails if the tag, the top of `debian/changelog`
   and the `CHANGELOG.md` section disagree.

One-time repository settings (maintainer, not the implementer):

- Settings → Pages → Source "GitHub Actions"; the `github-pages` environment
  must allow deployments from `main` and from `v*` tags.
- The secret `APT_GPG_PRIVATE_KEY`, see below.
- The secrets `S3_ENDPOINT` (host only), `S3_BUCKET`, `S3_ACCESS_KEY_ID` and
  `S3_SECRET_ACCESS_KEY`: `docs.yml` mirrors the site to that S3 bucket and
  deletes stale objects. For now this happens on every push to `main`, later
  only for releases.
- The optional secret `GOOGLE_SITE_VERIFICATION`, the file name Google Search
  Console gives (`google<hex>.html`). `docs.yml` writes that file to the site
  root; it is not in the repository.
- Branch protection requiring `checks`, `test`, `build`, `docs`, `deb` and the
  `docker` image job.

## The apt repository and its key

`scripts/build-apt-repo.sh` builds the repository with reprepro from
`apt/conf/distributions`: suite `trixie` holds the latest release (amd64 and
arm64), suite `trixie-snapshot` the latest build of `main`. A run of
`docs.yml` rebuilds both: the one it does not build is carried over (`trixie`
from the latest GitHub release, `trixie-snapshot` from the published site).
Only `runakefli_*.deb` files are carried over: releases up to 0.14.0 contain the
old `lokihouse` package and are skipped, so `trixie` stays empty until the first
release under the new name.

The signing key is an Ed25519 key without a passphrase. The private key lives
only in the secret `APT_GPG_PRIVATE_KEY` (ASCII-armoured); the public key is
committed as `apt/runakefli-archive-keyring.asc`, and the job fails if the two
do not match. To rotate it:

```sh
export GNUPGHOME=$(mktemp -d /tmp/lh-gpg.XXXXXX)
gpg --batch --passphrase '' --quick-generate-key 'runakefli archive signing key <bernd@bzed.de>' ed25519 sign never
gpg --armor --export > apt/runakefli-archive-keyring.asc
gpg --armor --export-secret-keys | gh secret set APT_GPG_PRIVATE_KEY
gpg --list-keys --with-fingerprint   # update the fingerprint in docs/install/index.md
```

Users must fetch the new public key, so rotate rarely.

## Container image

`Dockerfile` is a multi-stage build (Go toolchain on the build platform,
cross-compiled, into distroless `static`); `docker/runakefli.yaml` is its
default config. `make docker` builds it, `make docker-test` also runs
`scripts/docker-test.sh` (version, `/ready`, push and query against a throwaway
ClickHouse). Keep `GO_VERSION` in the `Dockerfile` and in `debian/rules` equal
to the toolchain in `go.mod`.

## Local testing

- `make check` runs the full gate: gofmt/goimports, `go vet`, golangci-lint,
  govulncheck, the unit tests with the race detector, and the coverage floor.
- `make oracle` / `make oracle-all` run the Rúnakefli-vs-Loki oracle in one
  push-down mode (or all three). Both need a running ClickHouse
  (`podman run -d -p 9000:9000 docker.io/clickhouse/clickhouse-server:26.9`).
- `make bench` starts the benchmark's servers (RustFS and three ClickHouse
  servers, podman, ports 19100–19102 and 19900) and runs the latency
  benchmark against Loki (see [Benchmark](benchmark.md)); results land in
  `.cache/bench/results.md`. `make bench-down` removes the servers.

## Leak checks

Rúnakefli is pure Go (`CGO_ENABLED=0`), so valgrind does not apply (it sees the
runtime's allocator, not leaks). The equivalents are pprof, goleak and the race
detector (which `make check` already runs).

- **goleak**: `internal/api`, `internal/server`, `internal/tail` and
  `internal/ruler` run `goleak.VerifyTestMain`, so any test that leaves a
  goroutine behind fails the package. Each `leakIgnores()` entry names the
  dependency that owns the goroutine.
- **pprof / soak**: `make soak` (build tag `soak`) runs a mixed push/query/tail
  workload against Rúnakefli for `RUNAKEFLI_SOAK_DURATION` (default 10m),
  samples `go_memstats_heap_inuse_bytes` and `go_goroutines` after a forced GC,
  and fails if post-warm-up heap grows >20% or goroutines grow >20. It writes
  `samples.csv` and `warm-*`/`end-*` profiles to `RUNAKEFLI_SOAK_OUT`
  (default `.cache/soak/`) for `go tool pprof -base`.
- **Profiling in production**: with `server.register_instrumentation: true`
  (the default) the Go profiler is served at `/debug/pprof/*` — see
  [Profiling](operations.md#profiling).

Recorded result (2026-10-12, 10m soak, 10 streams, push+query+tail+ruler,
AMD Ryzen 9 9900X): 32 post-warm-up samples, post-GC heap floor first-quarter
mean ~13.5 MiB, last-quarter mean ~15.6 MiB (no sustained growth, <20%);
goroutines flat at ~26–35. PASS.
