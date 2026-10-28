# Regex Simplifier Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Push `|~` / `!~` line filters that Loki's `RegexSimplifier` reduces to literal substrings down to ClickHouse as `LIKE` conditions instead of `match()`, so the SQL computes exactly what Loki's filter computes and skips the regex engine.

**Architecture:** A new helper in `internal/chstore` runs Loki's own `log.RegexSimplifier`. The helper passes constructors that record literals, not constructors that build filters. It then turns the recorded literals into an OR of `line LIKE '%lit%'`, or a `NOT (…)` of that OR. `exactLineFilterSQL` (Tier A, exact) and `regexNecessary` (log queries and the engine path, necessary) call it first and fall back to today's code when the regex does not simplify.

**Tech Stack:** Go, Loki `pkg/logql/log` at the pinned `LOKI_COMMIT`, `github.com/grafana/regexp/syntax` (already in Loki's module graph, currently `// indirect`), ClickHouse 26.9.

**Spec:** No separate spec file. The analysis (2026-10-27/28 session) found the following. For `sum(rate({app=~"app[1-3]"} |~ "level=(warn|error)" [1m]))`, lokihouse emits `line LIKE '%level=%' AND match(line, 'level=(warn|error)')`. The `LIKE` matches every line, and `match()` runs on all of them. `(line LIKE '%level=warn%' OR line LIKE '%level=error%')` measured 15–20 % faster in ClickHouse, on local disk, S3 + cache and uncached S3 alike. An OR of `LIKE`s on the same text index can skip granules when the needles are selective. Loki itself evaluates this filter as `contains("level=warn") || contains("level=error")` (`parseRegexpFilter` → `RegexSimplifier`), so the new SQL equals Loki's semantics by construction.

## Global Constraints

- **Prerequisite (met, `01c903d` on `main`):** `docs/superpowers/plans/2026-10-28-loki-bump-39c256e5.md` has landed (`LOKI_COMMIT := 39c256e50a804c045d5eb4e3c817a31653b535fe`). Also build on the `|=` or-chain fix (`fix: ignore empty operands in |= or chains`): `containsAny` drops empty operands, and `TestExactFilterSQL` has `|= "" or "x"` entries. Keep both. This plan assumes Loki's #25413 simplifier fix. Do not bump Loki again here. Re-checked on 2026-10-28 at `39c256e5`: `NewRegexSimplifier`, `NewMatcherFiltererFunc`, `Checker`, `MatcherFilterer`, `NewStageFunc`, `NewFilter` and the body of `parseRegexpFilter` are unchanged from `bc196286`, and `grafana/regexp` stays at the same version.
- Reuse Loki's code: call `log.NewRegexSimplifier` / `(*RegexSimplifier).Simplify`; do not re-implement the simplification rules.
- `github.com/grafana/regexp` moves from `// indirect` to a direct requirement at the version already in `go.mod` (`v0.0.0-20250905093917-f7b3be9d1853`); `go mod tidy` does this. No other new dependency.
- No panics on the request path: every interface method on the recording types returns a value.
- SQL arguments are bound parameters (`?`); literals are escaped with the existing `escapedLike`.
- `make check` passes (gofmt/goimports, vet, golangci-lint, govulncheck, `-race` tests, coverage ≥ 85 %).
- The change touches query/SQL code, so `make test-cluster`, `make oracle-all` and `make oracle-cluster` pass too.
- ClickHouse-backed tests need a server (`podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`, or `LOKIHOUSE_TEST_CH_ADDR`). Another benchmark may be running on this host. Use your own container on a free port and do not touch `lhb-*`, `ch1`–`ch4` or `keeper`.
- Known oracle failure, not caused by this plan: the six `TestOracle/p3/*/index_stats_*` tests fail on `main` (`bf8e4fd`) in every oracle mode and in `oracle-cluster` (Loki reports fewer chunks and entries than lokihouse). Every other oracle case must pass. Do not try to fix or skip those six here.
- The oracle's Loki at the pinned commit is already built at `/home/bzed/workspace/vibe/lokihouse/test/oracle/.cache/loki-bin/loki-39c256e50a80`. Pass it with `LOKIHOUSE_LOKI_BIN=…` from a worktree instead of rebuilding.
- `CHANGELOG.md` gets one line under `## [Unreleased]` → `### Changed`, in the same commit as the behaviour change.
- No entry in `docs/divergences.md`: results stay equal to Loki's. If the property test finds a case where they differ, the regex must fall back to the old path (do not add a divergence).

## Review Focus

1. **Case-insensitive regexes** (`(?i)error|warn`, `f|f(?i)oo`): Loki uses case-folding contains filters, which ILIKE does not reproduce (k/s folding). Expected: these fall back to today's path (`ok == false`). Task 1 tests `(?i)error|warn`.
2. **Regexes Loki reduces to `TrueFilter` / `ExistsFilter`** (`.*`, `.+`, `(?:)`). Expected: fall back, and never emit SQL that drops or keeps lines Loki keeps or drops. `foo|.*` is the trap, and it is a **Loki bug** at the pinned commit (verified 2026-10-28 with `log.NewFilter`). `newOrFilter` drops a `TrueFilter` leg instead of making the OR true, so Loki filters `foo|.*`, `.*|foo` and `foo|(?:)` as `contains("foo")` and drops `"bar"`, although the regex matches every line. lokihouse already returns Loki's result today, because `RegexExact` rejects `.` and the query runs in Loki's engine. The helper must keep reproducing Loki (`LIKE '%foo%'`): a correct answer would be an undocumented divergence. Task 1 tests the unit case. Task 2 adds oracle corpus cases, so a future Loki bump that fixes the bug fails the oracle until the pushdown follows.
3. **Literals with LIKE metacharacters** (`50%_x`, a backslash): must be escaped. Expected: `%50\%\_x%`. Task 1 tests it, and the ClickHouse property test includes `%`/`_` in its lines.
4. **`!~` with a simplifiable regex on empty and non-ASCII lines** (`""`, `"ä"`, `"\xff"`). Expected: identical keep/drop to Loki's `log.NewFilter(re, LineMatchNotRegexp)`. Task 1's ClickHouse property test runs every regex in both polarities over such lines.
5. **Non-ASCII literal regexes** (`fehler|ä`), which `RegexExact` rejects today. Expected: they become exact pushdowns, and ClickHouse's byte-wise `LIKE` agrees with Loki's `bytes.Contains`. Task 1's property test includes `fehler|ä`. Task 2 asserts that `ExactFilterSQL` accepts it.

---

### Task 1: `simplifiedRegexSQL` helper on top of Loki's simplifier

**Files:**
- Create: `internal/chstore/regexsimplify.go`
- Test: `internal/chstore/regexsimplify_test.go` (package `chstore`; unit table + ClickHouse property test)
- Modify: `go.mod` (`go mod tidy` makes `github.com/grafana/regexp` direct)

**Interfaces:**
- Consumes: `containsFragment(s string) Fragment` and `escapedLike` from `sqlbuild.go`; `log.NewRegexSimplifier`, `log.MatcherFilterer`, `log.Checker`, `log.NewStageFunc`, `log.StageHints` from Loki; `syntax.Parse`, `syntax.Perl` from `github.com/grafana/regexp/syntax`.
- Produces: `func simplifiedRegexSQL(re string, match bool) (Fragment, bool)`. It returns SQL over the column `line` that accepts exactly the lines Loki's `|~ re` (match=true) or `!~ re` (match=false) filter accepts. `ok == false` means Loki would run a real regex; the caller then falls back. The SQL shape is `"(line LIKE ? OR line LIKE ? …)"` with one `%…%` argument per literal, in the order Loki's `Matches` visits them, or `"NOT (" + that + ")"`.

- [ ] **Step 1: Write the failing unit test** `TestSimplifiedRegexSQL` (table-driven, `require.Equal` on the whole `Fragment`):

```go
ok := []struct{ re string; match bool; want Fragment }{
	{`level=(warn|error)`, true, Fragment{"(line LIKE ? OR line LIKE ?)", []any{"%level=warn%", "%level=error%"}}},
	{`level=(warn|error)`, false, Fragment{"NOT (line LIKE ? OR line LIKE ?)", []any{"%level=warn%", "%level=error%"}}},
	{`GET|POST|DELETE`, true, Fragment{"(line LIKE ? OR line LIKE ? OR line LIKE ?)", []any{"%GET%", "%POST%", "%DELETE%"}}},
	{`.*error.*`, true, Fragment{"(line LIKE ?)", []any{"%error%"}}},
	{`50%_x|y`, true, Fragment{"(line LIKE ? OR line LIKE ?)", []any{`%50\%\_x%`, "%y%"}}},
	{`fehler|ä`, true, Fragment{"(line LIKE ? OR line LIKE ?)", []any{"%fehler%", "%ä%"}}},
	// Loki bug, reproduced on purpose: newOrFilter drops a TrueFilter leg, so
	// Loki filters foo|.* (and foo|(?:)) as contains("foo"), not match-all.
	{`foo|.*`, true, Fragment{"(line LIKE ?)", []any{"%foo%"}}},
	{`foo|(?:)`, true, Fragment{"(line LIKE ?)", []any{"%foo%"}}},
}
notOK := []string{`status=(4|5)\d\d`, `(?i)error|warn`, `.*`, `.+`, `e.r`, `err(or)?: timeout`, `[`,
	// Go's parser turns ab(c|d) into the char class ab[c-d]; Loki runs it as a regex.
	`ab(c|d)`,
	// Loki #25413 (in the pin since 39c256e5): a .* between literal and alternation is not simplified.
	`a .*(bb|cc)`, `foo(.*bar|baz)`}
```

Before writing the expected argument order for `GET|POST|DELETE` and `level=(warn|error)`, confirm it: print what Loki's `Matches` visits once. Pin whatever order Loki produces; the order does not change semantics.

- [ ] **Step 2: Run it and confirm it fails**

Run: `go test ./internal/chstore -run TestSimplifiedRegexSQL -count=1`
Expected: FAIL to compile, `undefined: simplifiedRegexSQL`.

- [ ] **Step 3: Implement `simplifiedRegexSQL(re string, match bool) (Fragment, bool)` in `internal/chstore/regexsimplify.go`**

Mirror Loki's `parseRegexpFilter` exactly: `syntax.Parse(re, syntax.Perl)` (grafana/regexp), then `reg = reg.Simplify()`, then `log.NewRegexSimplifier(newLiteralLeaf, newLiteralLeaf).Simplify(reg, false)`. Two unexported types:
- `literalLeaf{lit []byte; fold bool}` implements `log.MatcherFilterer`. `Filter` is `bytes.Contains` (fold: case-insensitive, like Loki), and `ToStage` wraps `Filter` with `log.NewStageFunc`. `Matches(test log.Checker) bool` returns `test.Test(l.lit, l.fold, false)`.
- `literalCollector{lits [][]byte; fold bool}` implements `log.Checker`. `Test` appends the literal, ORs in `fold`, and returns `false`, so `orFilter.Matches` visits every leg. `TestRegex` returns `true`.

Return `ok == false` when parsing fails, `Simplify` reports false, `f.Matches(collector)` returns `true` (a `trueFilter`/`existsFilter` was reached), `collector.fold` is set, or no literal was recorded. Otherwise OR-join `containsFragment(string(lit))` in parentheses, and prefix `NOT ` when `!match`. Add a comment on `literalLeaf` naming `parseRegexpFilter` as the code being mirrored.

- [ ] **Step 4: Run it and confirm it passes**

Run: `go test ./internal/chstore -run TestSimplifiedRegexSQL -count=1`
Expected: PASS.

- [ ] **Step 5: Write the ClickHouse property test** `TestSimplifiedRegexSQL_MatchesLoki`. Connect as `prefilter_property_test.go` does (`LOKIHOUSE_TEST_CH_ADDR`, default `localhost:9000`).
  - Regexes: every `ok` entry above plus `err|error`, `ab(cc|dd)`, `x(?:)|y`, `a .*(bb|cc)`, `foo(.*bar|baz)`. The last two must come back `ok == false`. The test skips regexes the helper rejects, but asserts that these two are rejected.
  - Lines: 300 seeded-random concatenations (`rand.NewSource(42)`) of `{"level=warn", "level=error", "GET", "POST", "DELETE", "error", "err", "foo", "50%_x", "y", "ä", "\xff", "%", "_", "abcc", "abdd", "a x bb", "foo x bar", "foobaz", " "}`, plus `""`.
  - For each regex and each polarity (`log.LineMatchRegexp`, `log.LineMatchNotRegexp`), compute `want[i] = log.NewFilter(re, ty).Filter([]byte(line))`. Get ClickHouse's answers with one `SELECT arrayMap(line -> <f.SQL>, ?)` per regex and polarity (args: `f.Args…`, then the lines).
  - `require.Equal(t, want, got, re)`.

- [ ] **Step 6: Run it** (ClickHouse running)

Run: `LOKIHOUSE_TEST_CH_ADDR=127.0.0.1:<port> go test ./internal/chstore -run TestSimplifiedRegexSQL -count=1 -race`
Expected: PASS. If a case mismatches, make `simplifiedRegexSQL` return `ok == false` for that shape. Do not change the expected values.

- [ ] **Step 7: `go mod tidy`, then commit**

```bash
go mod tidy
git add go.mod go.sum internal/chstore/regexsimplify.go internal/chstore/regexsimplify_test.go
git commit -m "feat: translate Loki-simplifiable line regexes into LIKE conditions"
```

---

### Task 2: Use the helper for exact and necessary regex filters

**Files:**
- Modify: `internal/chstore/sqlbuild.go`: `exactLineFilterSQL` (cases `log.LineMatchRegexp`, `log.LineMatchNotRegexp`) and `regexNecessary`
- Test: `internal/chstore/sqlbuild_test.go` (`TestExactFilterSQL`, `TestLeadingLineFilterSQL_RegexAndPattern`), `internal/chstore/textindex_test.go` (`TestLineFilterSQL_UsesTextIndex`)
- Modify: `test/oracle/corpus/pushdown.logql`, `test/oracle/corpus/basic.logql`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `simplifiedRegexSQL(re string, match bool) (Fragment, bool)` from Task 1.
- Produces: no new API. `ExactFilterSQL` and `LeadingLineFilterSQL` return the new fragments for simplifiable regexes.

- [ ] **Step 1: Update the tests to the new behaviour**

In `TestExactFilterSQL`'s `exact` map, keep `` `{a="b"} |~ "ab(c|d)"` `` unchanged: it is a char class, so it still falls back to `line LIKE ? AND match(line, ?)`, and the entry now doubles as the fallback check. Replace the `!~ "ab"` entry and add two:
```go
`{a="b"} |~ "ab(cc|dd)"`:  {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%abcc%", "%abdd%"}},
`{a="b"} !~ "ab"`:         {SQL: "NOT (line LIKE ?)", Args: []any{"%ab%"}},
`{a="b"} |~ "fehler|ä"`:   {SQL: "(line LIKE ? OR line LIKE ?)", Args: []any{"%fehler%", "%ä%"}},
```
Add `` `{a="b"} |~ "a .*(bb|cc)"` `` to the `ok == false` list. `.` fails `RegexExact`, and since #25413 Loki no longer simplifies it, so it must not be pushed as exact.

In `TestLeadingLineFilterSQL_RegexAndPattern` change:
```go
`{a="b"} |~ "error"`:             "(line LIKE ?)",
`{a="b"} |= "x" or "y" |~ "z.*"`: "(line LIKE ? OR line LIKE ?) AND (line LIKE ?)",
```
(`error|warn` keeps `(line LIKE ? OR line LIKE ?)`; `err(or)?: timeout` keeps `line LIKE ? AND line LIKE ?`.)

In `TestLineFilterSQL_UsesTextIndex`, add `` `{a="b"} |~ "needle|haystack"` `` to the query list.

- [ ] **Step 2: Run them and confirm they fail**

Run: `go test ./internal/chstore -run 'TestExactFilterSQL|TestLeadingLineFilterSQL' -count=1`
Expected: FAIL on the changed entries (old `LIKE … AND match(…)` / `NOT match(…)` SQL).

- [ ] **Step 3: Wire the helper in `sqlbuild.go`**
  - In `exactLineFilterSQL`, after the `len(alts) > 1` check of both regex cases and before `RegexExact`, return `simplifiedRegexSQL(f.Match, f.Ty == log.LineMatchRegexp)` when it is ok.
  - In `regexNecessary`, return the helper's fragment for `match = true` first when it is ok. An exact fragment is also a necessary one.
  - Update the doc comment of `ExactFilterSQL`: "regex that Loki simplifies to literals, or passing RegexExact".

- [ ] **Step 4: Run the package tests**

Run: `LOKIHOUSE_TEST_CH_ADDR=127.0.0.1:<port> go test ./internal/chstore ./internal/pushdown -count=1 -race`
Expected: PASS, including `TestLineFilterSQL_UsesTextIndex` (rows read < rows/2 for `needle|haystack`) and `TestPrefilter_Necessary`. Other tests that pin SQL for a simplifiable regex may still expect the old SQL: `line LIKE ?` without parentheses, or `match(` in a generated query. Update those expectations to the new shape. Change code only if the semantics differ.

- [ ] **Step 5: Extend the oracle corpus**
  - `test/oracle/corpus/pushdown.logql`, next to the existing `level=(error|warn)` line:
    ```
    range:count_over_time({app="api"} !~ "level=(error|warn)"[5m])
    range:rate({app="web"} |~ "GET|POST"[1m])
    ```
  - `test/oracle/corpus/basic.logql`, next to `{app="web"} |~ "status=(4|5)\d\d"`:
    ```
    {app="api"} |~ "level=(error|warn)"
    {app="web"} !~ "GET|POST"
    {app="web"} |~ "GET|.*"
    ```
    plus `range:count_over_time({app="web"} |~ "GET|.*"[5m])` in `pushdown.logql`. These pin Loki's `newOrFilter` bug (Review Focus 2). Today they run in Loki's engine; after this change they run as SQL; the oracle must agree both times.

- [ ] **Step 6: Run the full gates**

Run, in order: `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`.
Expected: all pass, coverage ≥ 85 %. `make test-cluster` and `make oracle-cluster` start the `ch1`–`ch4`/`keeper` cluster. If another session is using it, wait until it is idle; do not tear it down.

- [ ] **Step 7: CHANGELOG and commit**

Under `## [Unreleased]` → `### Changed` add:
`- Line filters whose regex Loki reduces to literal substrings (`|~ "level=(warn|error)"`, `!~ "GET|POST"`, `|~ ".*error.*"`) run in ClickHouse as `LIKE` conditions instead of `match()`, as Loki evaluates them; they skip the regex engine and can use the line text index.`

```bash
git add internal/chstore/sqlbuild.go internal/chstore/sqlbuild_test.go internal/chstore/textindex_test.go test/oracle/corpus/pushdown.logql test/oracle/corpus/basic.logql CHANGELOG.md
git commit -m "feat: push Loki-simplified line regexes down as LIKE"
```
