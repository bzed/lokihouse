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
