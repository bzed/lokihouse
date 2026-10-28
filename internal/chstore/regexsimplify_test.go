package chstore

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/stretchr/testify/require"
)

var simplifiedRegexOK = []struct {
	re    string
	match bool
	want  Fragment
}{
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

var simplifiedRegexNotOK = []string{
	`status=(4|5)\d\d`, `(?i)error|warn`, `.*`, `.+`, `e.r`, `err(or)?: timeout`, `[`,
	// Go's parser turns ab(c|d) into the char class ab[c-d]; Loki runs it as a regex.
	`ab(c|d)`,
	// Loki #25413 (in the pin since 39c256e5): a .* between literal and alternation is not simplified.
	`a .*(bb|cc)`, `foo(.*bar|baz)`,
}

func TestSimplifiedRegexSQL(t *testing.T) {
	for _, c := range simplifiedRegexOK {
		got, ok := simplifiedRegexSQL(c.re, c.match)
		require.True(t, ok, "%q match=%v", c.re, c.match)
		require.Equal(t, c.want, got, "%q match=%v", c.re, c.match)
	}
	for _, re := range simplifiedRegexNotOK {
		for _, match := range []bool{true, false} {
			_, ok := simplifiedRegexSQL(re, match)
			require.False(t, ok, "%q match=%v", re, match)
		}
	}
}

// TestSimplifiedRegexSQL_MatchesLoki checks, against a real ClickHouse, that the
// SQL keeps exactly the lines Loki's own filter keeps, in both polarities.
func TestSimplifiedRegexSQL_MatchesLoki(t *testing.T) {
	addr := os.Getenv("LOKIHOUSE_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()

	var res []string
	for _, c := range simplifiedRegexOK {
		res = append(res, c.re)
	}
	res = append(res, `err|error`, `ab(cc|dd)`, `x(?:)|y`, `a .*(bb|cc)`, `foo(.*bar|baz)`)

	words := []string{"level=warn", "level=error", "GET", "POST", "DELETE", "error", "err", "foo",
		"50%_x", "y", "ä", "\xff", "%", "_", "abcc", "abdd", "a x bb", "foo x bar", "foobaz", " "}
	rng := rand.New(rand.NewSource(42))
	lines := []string{""}
	for i := 0; i < 300; i++ {
		var b strings.Builder
		for j, n := 0, 1+rng.Intn(4); j < n; j++ {
			b.WriteString(words[rng.Intn(len(words))])
		}
		lines = append(lines, b.String())
	}

	rejected := map[string]bool{}
	checked := 0
	for _, re := range res {
		for _, pol := range []struct {
			ty    log.LineMatchType
			match bool
		}{{log.LineMatchRegexp, true}, {log.LineMatchNotRegexp, false}} {
			f, ok := simplifiedRegexSQL(re, pol.match)
			if !ok {
				rejected[re] = true
				continue
			}
			lf, err := log.NewFilter(re, pol.ty)
			require.NoError(t, err, re)
			want := make([]bool, len(lines))
			for i, l := range lines {
				want[i] = lf.Filter([]byte(l))
			}
			var raw []uint8
			args := append(append([]any{}, f.Args...), lines)
			require.NoError(t, conn.QueryRow(ctx, "SELECT arrayMap(line -> "+f.SQL+", ?)", args...).Scan(&raw), re)
			got := make([]bool, len(raw))
			for i, v := range raw {
				got[i] = v != 0
			}
			require.Equal(t, want, got, "%q match=%v", re, pol.match)
			checked++
		}
	}
	require.Greater(t, checked, 0)
	require.True(t, rejected[`a .*(bb|cc)`])
	require.True(t, rejected[`foo(.*bar|baz)`])
}
