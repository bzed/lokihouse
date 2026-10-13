# Changelog

All notable changes to lokihouse are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The Debian package
changelog (`debian/changelog`) carries the same releases.

## [Unreleased]

### Changed

- A push spanning more than 60 day/retention partitions is rejected with 400 (discard reason `too_many_partitions`) instead of failing in ClickHouse with 500 forever; pushes over 8 partitions bypass the async-insert buffer, so a backfill can no longer fail other pushes that share its flush.

## [0.6.0] - 2026-10-13

### Added

- Signed apt repository (suites `trixie` and `trixie-snapshot`, amd64 and arm64)
  published with the documentation.
- Container image `ghcr.io/bzed/lokihouse` (linux/amd64 and linux/arm64), signed
  with cosign, with SBOM and provenance.
- `SECURITY.md`, `CONTRIBUTING.md` and this changelog.

### Changed

- Pinned Loki commit bumped from `fbc3f077` to `bc196286` (2026-10-12). The Loki
  code lokihouse copies is unchanged between the two; unit, cluster and oracle
  suites pass.

### Fixed

- Delete requests: a status change (processed, cancelled) written in the same
  clock tick as the previous write could lose to it; versions are now strictly
  increasing.
- Documentation links that pointed at the site root instead of `/lokihouse/`.
- The man page title (lintian `wrong-manual-section`).

## [0.5.0] - 2026-10-11

### Added

- Graceful shutdown, Debian packaging with a hardened systemd unit, user and
  reference documentation, CI, per-tenant limits, multi-tenant queries, metrics
  and query statistics, a Loki benchmark, and memory/goroutine leak checks
  (goleak, pprof, soak test).
