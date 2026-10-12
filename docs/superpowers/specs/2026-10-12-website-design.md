# Lokihouse documentation site — design

Date: 2026-10-12

## Intent

lokihouse is a Go service that is wire-compatible with Grafana Loki's HTTP API
but stores logs in ClickHouse. It is currently work-in-progress (no packaging,
per `README.md`). Two audiences need a docs site:

1. **Fresh install** — a sysadmin who has never run lokihouse and wants to get
   one running on a bare machine (Go + ClickHouse + a static binary + a systemd
   unit). Because packaging (Debian repos, Docker images) is not ready, this
   page is a complete manual runbook *and* carries an explicit WiP banner
   saying that the packaging work is still in progress.
2. **Migrate from Loki** — a sysadmin running stock Loki who wants to swap
   Loki for lokihouse. Needs a full cutover runbook (stand up lokihouse next
   to Loki, shadow push traffic, verify with the same LogQL queries, flip the
   Grafana datasource), plus plain-language coverage of every entry in
   `docs/divergences.md` (the behavioural differences a migrator must know
   about before flipping the switch), plus a verification checklist.

Everything else (reference, developer notes) is secondary but included so the
site is self-contained.

## Goals

- A published GitHub Pages site that builds from Markdown in `docs/` with
  mkdocs, via a GitHub Action on push to `main`.
- Two audience tracks front-and-centre on the home page: *Install* and
  *Migrate from Loki*.
- Honest about WIP state: packaging is WiP; the docs say so without hiding it.
- Configuration reference accurate to the code (`internal/config/config.go`),
  not aspirational.
- Divergences documented in plain language, not just a pointer to
  `docs/divergences.md` (the existing file is written for the team and is
  dense; a migrator should not have to read it raw).
- No content drift: the docs live in `docs/` next to the existing
  `docs/divergences.md` and `docs/phase*-status.md`. The existing phase
  status docs are kept as developer docs; they are *not* re-published on the
  site (the site is a stable "current" set, not a phase changelog — the
  maintainer chose the stable-current model).

## Non-goals

- No per-phase release-notes pages on the site (covered by the existing
  `docs/phase*-status.md` in the repo; linked to, not duplicated).
- No i18n, no search-as-you-type beyond mkdocs' built-in, no custom theme
  beyond the default mkdocs material theme.
- No build of the Go binary in CI. The site builds docs only.
- No editing of the existing `docs/divergences.md` or `docs/phase*-status.md`
  files; new site files live in `docs/` under a new subfolder layout.
- No automated test of the docs content (e.g. "does the YAML in the config
  reference parse?"). That is future work.

## Constraints

- **AGENTS.md rules apply.** No new third-party Go dependency. The docs are
  not Go, but the rule of "no new third-party dependency unless it is already
  in Loki's module graph or the change explains why" applies to any tool the
  docs build needs. MkDocs + Material theme are Python packages installed in
  CI via `pip`; they are not a Go dependency, so this rule does not bind them,
  but we keep the dependency surface small: `mkdocs` + `mkdocs-material` only.
- **Existing docs stay.** `docs/divergences.md`, `docs/phase1-status.md` …
  `docs/phase4-status.md`, `docs/superpowers/**` remain in place and are the
  canonical dev docs. The site adds new files; it does not move or rename
  them.
- **MkDocs is the build.** No other static site generator. The GitHub Action
  runs `mkdocs build --strict` and deploys to GitHub Pages via the official
  `actions/deploy-pages` + `actions/configure-pages` pattern (or
  `deploy-pages` with a branch; we use the branch-based workflow for
  simplicity, since the repo is `bzed/lokihouse`).
- **WiP honesty.** Every install-related page carries a visible note that
  packaging (Debian repos, Docker images) is not yet available and that the
  documented path is "build the static Go binary + run systemd + ClickHouse",
  which is what a user has to do today.
- **Config reference must match the code.** `internal/config/config.go` is the
  source of truth for config keys. The reference page lists them exactly as
  they appear in YAML, with defaults and types. If the code changes, the docs
  change in the same commit (AGENTS.md: "every observable difference from
  Loki is recorded"; the same discipline applies to the docs being accurate
  about lokihouse itself).

## Content — pages

MkDocs `nav` (top-level items in the left nav):

1. **Index** (`index.md`)
   - One-paragraph description of lokihouse (from the README).
   - Two prominent "audience" cards: *Fresh install* (link) and *Migrate
     from Loki* (link).
   - Short "What's in it for you" bullets (Loki API compatibility, ClickHouse
     storage, no Loki storage layer to run).
   - WiP banner: "lokihouse is under active development. Packaging (Debian
     repos, Docker images) is work in progress. The install guide below is the
     manual path that works today."
   - Links: *Known differences from Loki* (migrator's reference), *License*.

2. **Installation** (`install/index.md` — one page, not a subnav)
   - **WiP banner** (same as index).
   - **Prerequisites**: Go (only needed for the build step; the binary is
     static — `CGO_ENABLED=0` in the Makefile), a running ClickHouse 26.9
     (single node or cluster — the cluster section is a short link to a
     cluster-configuration reference, not a full cluster runbook), systemd.
   - **Step 1 — Build the binary.** One line:
     `go build -trimpath -ldflags "-s -w" -o lokihouse ./cmd/lokihouse`
     (or `make build`, which adds version ldflags). Note: static, runs on any
     glibc-less Linux; no runtime Go toolchain needed after the build.
   - **Step 2 — Write the config.** Point to the reference page, show a
     minimal working `lokihouse.yaml` (server + clickhouse + nothing else),
     and call out the required fields (`server.http_listen_port`,
     `clickhouse.addr`, `clickhouse.database`, `clickhouse.username`;
     `password` or `password_file` if the CH user is not passwordless).
   - **Step 3 — systemd unit.** Full unit file: `Type=exec`, `ExecStart`,
     `Restart=on-failure`, `User=lokihouse`, `ProtectHome=true`,
     `ReadWritePaths=` for the ruler storage dir (if ruler is enabled),
     `Credentials=` example for `password_file` (systemd credentials —
     lokihouse's config supports `password_file`, which is the idiomatic
     systemd-credentials path). Note: `SystemdCredentials=` is optional; a
     plain `0o600` file also works.
   - **Step 4 — Health check.** `curl -fsS http://localhost:3100/ready`
     returns `ready\n` (200). `curl http://localhost:3100/loki/api/v1/status/buildinfo`
     returns the version JSON. Note the Loki-identical `/loki/api/v1/...`
     paths.
   - **Step 5 — First push + query.** A two-curl demo: push one entry to
     `/loki/api/v1/push`, then query it back with `/loki/api/v1/query?query=...`
     (the exact shape Loki's push JSON takes).
   - **Upgrading.** Rebuild and restart the unit. No migration step for now
     (pre-release; pre-review the schema may change, per the phase-1 post-
     completion review note "schema changed: databases created before this
     review must be recreated").
   - **Uninstall.** Remove the unit, drop the ClickHouse database, delete the
     binary. (One short paragraph.)
   - **ClickHouse cluster (optional, short).** Two-paragraph pointer: for a
     2-shard × 2-replica cluster, set `clickhouse.cluster`, `clickhouse.replicated`,
     and list all node addresses in `clickhouse.addr`. See *Reference →
     Configuration → `clickhouse`* for the keys. Full cluster runbook is
     out of scope for this doc set (that is a ClickHouse ops task, not a
     lokihouse one).

3. **Migrating from Loki** (`migrating.md`)
   - **TL;DR**: lokihouse is a drop-in at the HTTP layer. Loki's push
     endpoints and LogQL endpoints are the same paths, the same JSON shapes,
     the same headers. Point a Grafana datasource at lokihouse's base URL
     instead of Loki's and it works.
   - **What you keep and what you stop running.** Keep: your Grafana, your
     Alertmanager, your push clients, your LogQL queries, your rule files
     (ruler storage is a local dir). Stop: Loki's ingesters, compactor,
     distributor, query-frontend, ruler (if you move that too), TSDB index,
     object-store access for chunks.
   - **What is different (plain-language summary of `docs/divergences.md`).**
     A table or list, each entry: *what you'll notice* / *why* / *what to do
     about it*. Entries (from `docs/divergences.md`):
     - **Tie order inside one stream and timestamp** — you may see entries
       in a different order than Loki for entries that share a timestamp
       within one stream. If you rely on "Loki's insertion order" in your UI
       or downstream processing, you will notice. Otherwise invisible.
     - **`index/stats` / `index/volume` granularity** — Loki pro-rates per
       chunk (10-min blocks); lokihouse pro-rates per hourly bucket. If you
       build dashboards off `index/volume`, expect ~15–30% differences in
       volume numbers for ranges that cut through an hour. Ranges that cover
       whole hours agree.
     - **`index/volume` vs Loki's two code paths** — Loki's live volume query
       and its compactor-based volume query disagree with each other
       (different code paths); lokihouse matches the compactor/ingester
       accounting, not the querier path. If your dashboards were built on
       the querier path, the numbers will shift.
     - **Delete requests** — request IDs differ in format (8 hex chars vs
       Loki's). Stats and volume ignore deletes in both. Query-time delete
       visibility: lokihouse is authoritative per query; Loki caches a
       tenant's delete list for 5 minutes, so a delete created after a query
       is invisible for up to 5 minutes in Loki but visible immediately in
       lokihouse. Rows written between delete submission and processing are
       also deleted by lokihouse (matches Loki's compactor).
     - **Tail delivery** — Loki pushes tail entries as they arrive; lokihouse
       polls ClickHouse every second with a 10-second lookback. An entry
       pushed with a timestamp older than the current poll window, or a
       timestamp before the tail started, will not be sent live (it is
       reachable by a normal range query). History (rows before the tail
       start) is served the same way in both.
     - **Ruler sharding** — Loki distributes rule groups over a ring;
       lokihouse has no ring. Every lokihouse instance with `ruler_storage`
       configured evaluates *all* rule groups. If you run more than one
       lokihouse instance, configure the ruler on exactly one or you will
       duplicate alerts and recording-rule samples.
       `ruler.enable_sharding: true` and `ruler.evaluation.mode: remote` are
       rejected at startup.
     - **Ruler `rule_path` default** — `rules-temp` relative to the working
       directory instead of `/rules`. Set `ruler.rule_path` explicitly in
       your config if you were relying on Loki's default.
     - **Ruler WAL directory guard** — with remote write enabled, Loki wipes
       `ruler.wal.dir` at every start. lokihouse refuses to start when that
       directory is empty or is the working directory, the rule storage
       directory or `rule_path` (a safety measure; Loki would delete those).
     - **No `/metrics` endpoint (yet)** — Loki's ruler metrics are collected
       in a private registry and not exposed (Phase 5 work). If you have
       dashboards on Loki's Prometheus metrics, they will not work as-is.
   - **Cutover runbook** (the actual steps):
     1. **Build & install lokihouse** next to Loki (use the *Installation*
        page). Loki stays up. Both write to their own storage (Loki to its
        object store, lokihouse to ClickHouse).
     2. **Shadow push.** Point a *copy* of your push traffic at lokihouse's
        push endpoint (same JSON, same headers, same `X-Scope-OrgID` if you
        use multi-tenancy). Loki keeps its own traffic. If you have a load
        balancer or push gateway in front of Loki, add lokihouse as a second
        upstream.
     3. **Verify with the same queries.** Pick a set of LogQL queries you
        already use in Grafana. Run them against Loki and against lokihouse
        (two Grafana datasources, or `curl`). Compare results. The project's
        own differential oracle (`test/oracle`) runs the same corpus against
        both and compares byte-for-byte with float tolerance — that is the
        standard this page uses for "they agree".
     4. **Flip the Grafana datasource.** Change the base URL from
        `http://loki:3100` to `http://lokihouse:3100`. Same paths, same
        auth headers. Grafana does not need to know the difference.
     5. **Stop pushing to Loki** (remove it from your push LB / push gateway).
        Loki can stay up read-only for a transition period if you want a
        rollback path.
     6. **Rollback.** Point Grafana back at Loki. Loki's data is untouched
        (you were pushing to both in step 2). If you have already deleted
        data from Loki, that's not reversible — test on a copy of your data
        first.
     7. **Decommission Loki** (optional, after a soak period). Remove the
        Loki systemd units / pods, stop the object-store bill, etc.
   - **Verification checklist** (a short list the migrator can tick off):
     - [ ] `/ready` returns 200 on lokihouse
     - [ ] `/loki/api/v1/status/buildinfo` returns the expected version
     - [ ] A push to `/loki/api/v1/push` succeeds and is queryable
     - [ ] Your N favourite LogQL queries return the same result on Loki and
           on lokihouse (within float tolerance)
     - [ ] `labels` and `label/{name}/values` return the same sets
     - [ ] If you use the ruler: rule groups are visible via
           `/loki/api/v1/rules` and firing an alert rule sends to
           Alertmanager
     - [ ] If you use tail: a Grafana Live view works (allowing for the
           polling-vs-push delivery difference)
     - [ ] If you use deletes: a delete request is accepted, the query-time
           filter hides the rows, and the request eventually completes
     - [ ] Multi-tenancy: if you use `X-Scope-OrgID`, the same tenant
           isolation holds
     - [ ] Retention: if you set `retention_period` per tenant, old data
           expires on the expected UTC-day boundary

4. **Reference → Configuration** (`reference/configuration.md`)
   - YAML reference, grouped by top-level key. The lokihouse-owned keys are
     documented in full (key, type, default, one-line description); the
     Loki-inherited blocks are summarised and linked out to Loki's own
     config reference.
     - `server` (listen address/port, timeouts, log level, log format,
       graceful shutdown) — **fully documented**
     - `auth_enabled` (boolean; when true, requires `X-Scope-OrgID`) —
       **fully documented**
     - `clickhouse` (`addr`, `database`, `username`, `password` /
       `password_file`, `cluster`, `replicated`, `replica_path`,
       `replica_name`, `settings`) — **fully documented**
     - `query` (`pushdown`: `on | off | downstream`) — **fully documented**
     - `compactor` (`compaction_interval`, `delete_request_cancel_period`,
       `delete_max_interval`) — **fully documented**
     - `ruler` — **one-paragraph summary**: Loki's ruler block, same keys
       and defaults as Loki's `ruler` config. Link:
       [Loki ruler config reference](https://grafana.com/docs/loki/latest/configuration/#ruler).
       The two keys lokihouse rejects (`ruler.enable_sharding`,
       `ruler.evaluation.mode: remote`) are called out explicitly.
     - `ruler_storage` — **one-paragraph summary**: the ruler only runs when
       this is set. Two backends: `filesystem` (read/write rule files) and
       `local` (read-only). Link to Loki's ruler storage reference.
     - `limits_config` — **one-paragraph summary**: Loki's
       `validation.Limits`; every key Loki accepts is accepted here, with
       Loki's defaults. Link to [Loki limits
       reference](https://grafana.com/docs/loki/latest/configuration/#limits_config).
       The keys an operator is most likely to tune
       (`max_query_series`, `max_query_lookback`, `volume_enabled`) are
       listed with their Loki defaults.
   - Note on `password_file`: the file must be `0o600` (or use systemd
     credentials). The file path is read at startup; if the file is missing
     or unreadable, lokihouse refuses to start.

5. **Reference → HTTP API** (`reference/http-api.md`)
   - An exhaustive table of every HTTP endpoint lokihouse serves, enumerated
     directly from `internal/api/api.go`'s router. Columns: Method, Path,
     Auth, Notes. Every `mux.Handle` / `mux.HandleFunc` registration
     appears.
   - The table is grouped by function (Push, Query, Metadata, Deletes,
     Tail, Ruler, Admin) for readability, but no endpoint is omitted.
   - Note: all `/loki/api/v1/...` paths are Loki-identical. The
     `/api/prom/...` paths are the legacy Prometheus-compatible paths Loki
     also serves.
   - Note on `auth_enabled: true`: every endpoint except `/ready` and
     `format_query` requires the `X-Scope-OrgID` header (Loki's tenancy
     model).

6. **Reference → Divergences** (`reference/divergences.md`)
   - A pointer page: "The canonical, detailed list of behavioural differences
     from Loki lives in `docs/divergences.md` in the repo. The *Migrating
     from Loki* page covers the same differences in plain language for
     migrators. This page is a thin index linking to both."
   - (We do not duplicate the content; the migrator page is the reader-facing
     version, `docs/divergences.md` is the canonical source of truth.)

7. **About / License** (`about.md`) — small page: AGPL-3.0, link to LICENSE,
   link to the design spec, link to the phase status docs, link to the
   GitHub repo.

## mkdocs configuration

- `mkdocs.yml` at the repo root (not inside `docs/`; mkdocs wants it at the
  repo root or you have to set `docs_dir`). We put it at the repo root.
- Theme: `material` (the most common, good default, no extra config needed
  for a small site).
- `docs_dir: docs` (default; we do not change it, so the existing `docs/`
  folder is the docs dir).
- `site_name: lokihouse`
- `site_description: Grafana Loki's HTTP API, backed by ClickHouse`
- `nav:` as listed above.
- `strict: true` — the CI build fails on any warning (broken links,
  validation errors), so the published site never ships with a broken link.
- `validation:` — external link validation is on (catches typos in the
  GitHub URL etc.).
- No `mkdocs_macros` or other plugins — keep the dependency surface to
  `mkdocs` + `mkdocs-material`.

## File layout (new files)

```
mkdocs.yml                                # new, at repo root
.github/workflows/docs.yml                # new, the build+publish action
docs/index.md                             # new, home page (replaces README's
                                          #   role for the site; README stays)
docs/install/index.md                     # new, the install runbook
docs/migrating.md                         # new, the migration runbook
docs/reference/configuration.md           # new, full config reference
docs/reference/http-api.md                # new, HTTP API table
docs/reference/divergences.md             # new, pointer page
docs/about.md                             # new, license/about
```

Existing files untouched: `docs/divergences.md`, `docs/phase1-status.md` …
`docs/phase4-status.md`, `docs/superpowers/**`.

Note on `docs/index.md`: mkdocs uses `docs/index.md` as the home page. The
existing `README.md` at the repo root is the GitHub-readme and stays as-is.
The two are separate: the README is for GitHub, the index is for the site.
They will share the same one-paragraph description; that is fine (they are
maintained in the same repo, drift is a one-line fix).

## GitHub Action

`.github/workflows/docs.yml`:

- Triggers: `push` to `main` only (not PRs, not tags; the docs are a
  stable "current" set, so only `main` publishes).
- Job: `build`
  - Runs on `ubuntu-latest`.
  - Steps:
    1. `actions/checkout@v4`
    2. `actions/setup-python@v5` (Python 3.12, the mkdocs requirement)
    3. `pip install mkdocs mkdocs-material` (no lock file; the docs build
       does not need reproducible Python builds in the same way Go does —
       if this changes, add a `requirements-docs.txt`)
    4. `mkdocs build --strict` (output in `site/`)
  - Job: `deploy` (needs: build)
    - Permissions: `pages: write`, `id-token: write` (the GitHub-recommended
      pattern for Pages deploys).
    - Steps:
      1. `actions/configure-pages@v4`
      2. `actions/upload-pages-artifact@v3` (uploads `site/`)
      3. `actions/deploy-pages@v4`
- The workflow is split into two jobs (build, then deploy) so a broken
  build does not touch the live site.

## Out of scope (explicitly, so they don't get "discovered" later)

- No Docker image for the docs build (the docs are a pure static site; the
  GitHub Action runs on a runner, no Docker needed).
- No automated "does the YAML in the config reference parse" test (future
  work; the reference is hand-maintained against the code).
- No "what changed since the last phase" page on the site (the phase status
  docs in the repo cover that; the site is a stable current view).
- No search-as-you-type plugin beyond Material's built-in search.
- No dark-mode toggle config beyond Material's default.
- No `mkdocs-macros` or template variables (no build-time injection of the
  Go version etc.; the site is static Markdown).

## Decisions made by the builder (marked for the human to check)

These are settled by the builder, not left open. If any of them is wrong,
the cost of fixing it is small (a link target, a validation flag, or a
paragraph of prose).

- **Site URL:** `https://bzed.github.io/lokihouse/` (GitHub Pages default
  for a repo site). Used in internal links and the build-info / health-check
  examples.
- **mkdocs strictness:** `strict: true`, internal link validation on,
  **external link validation off**. Broken internal links fail the build
  (the real risk); transient external-link failures (network egress, rate
  limits) do not block a publish.
- **Config reference scope:** the reference page documents only the keys
  operators realistically set on the lokihouse side (`server`, `clickhouse`,
  `query`, `compactor`, `auth_enabled`). The inherited Loki blocks
  (`ruler`, `ruler_storage`, `limits_config`) are summarised in one
  paragraph each and linked out to Loki's own config reference, because
  their keys and defaults live in Loki, not in lokihouse. This keeps the
  reference accurate and maintainable without hand-transcribing hundreds of
  Loki keys.
- **HTTP API reference:** exhaustive, enumerated directly from
  `internal/api/api.go`'s router (the stated bar). Every endpoint the
  router registers appears in the table.
- **Install-runbook verification:** the curl examples are derived from the
  handler code and the response shapes in `internal/api`; they are marked
  "expected" where they have not been executed. The project's own oracle
  (`make oracle`) covers the push + query path end-to-end, so the runbook
  does not need a separate live verification pass.
- **`password_file` + systemd credentials:** the install page leads with
  `password_file` (simpler, works on any systemd) and mentions
  `SystemdCredentials=` as an alternative.
- **GitHub Pages setup:** the maintainer must configure the repo's Pages
  setting (Settings → Pages → Source: GitHub Actions, branch: `main`)
  before the first deploy. A comment at the top of the workflow file notes
  this.
