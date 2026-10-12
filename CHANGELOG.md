# Changelog

All notable changes to lokihouse are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The Debian package
changelog (`debian/changelog`) carries the same releases.

## [Unreleased]

### Added

- Signed apt repository (suites `trixie` and `trixie-snapshot`, amd64 and arm64)
  published with the documentation.
- Container image `ghcr.io/bzed/lokihouse` (linux/amd64 and linux/arm64), signed
  with cosign, with SBOM and provenance.
- `SECURITY.md`, `CONTRIBUTING.md` and this changelog.

### Fixed

- Documentation links that pointed at the site root instead of `/lokihouse/`.
- The man page title (lintian `wrong-manual-section`).

## [0.5.0] - 2026-10-11

### Added

- Graceful shutdown, Debian packaging with a hardened systemd unit, user and
  reference documentation, CI, per-tenant limits, multi-tenant queries, metrics
  and query statistics, a Loki benchmark, and memory/goroutine leak checks
  (goleak, pprof, soak test).
