# Contributing

Thanks for helping. The rules for every change, by humans and by coding agents,
are in [`AGENTS.md`](AGENTS.md); `make check` enforces the mechanical ones.
[`docs/development.md`](docs/development.md) explains CI, releases and the apt
repository.

In short:

1. Write the test first, then the code. Coverage must stay at 85 % or higher.
2. Run `make check` (and `make test-cluster` and `make oracle-all` for query,
   ingest, ruler or SQL changes). ClickHouse-backed tests need a server, see
   `AGENTS.md`.
3. Record every observable difference from Loki in `docs/divergences.md`.
4. Add a line to `CHANGELOG.md` under `Unreleased` for every user-visible change.
5. One logical change per commit, with a conventional prefix (`feat:`, `fix:`,
   `test:`, `docs:`, `build:`, `ci:`, `refactor:`).

Security problems: see [`SECURITY.md`](SECURITY.md), not a public issue.
