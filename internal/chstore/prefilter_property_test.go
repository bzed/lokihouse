package chstore

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

// TestPrefilter_Necessary is a seeded property test: for random (filter, line)
// pairs, if Loki's filter accepts the line, then ClickHouse must evaluate the
// generated SQL fragment as true for that line (the fragment is necessary, not exact).
func TestPrefilter_Necessary(t *testing.T) {
	addr := os.Getenv("LOKIHOUSE_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()

	words := []string{"err", "or", "timeout", "GET", "ä", "\xff", "a", "b", "c", "x"}
	// Build a small set of filters (match, type) and the fragment we generate for them.
	type filt struct {
		match string
		typ   log.LineMatchType
	}
	filters := []filt{
		{"err", log.LineMatchRegexp},
		{"or", log.LineMatchRegexp},
		{"err|or", log.LineMatchRegexp},
		{"GET", log.LineMatchRegexp},
		{"err(a)?", log.LineMatchRegexp},
		{"e.r", log.LineMatchRegexp}, // not pushed (emit nothing) — still necessary
		{"> GET <x> 200", log.LineMatchPattern},
		{"> <x>", log.LineMatchPattern}, // no literals -> nothing
		{"timeout", log.LineMatchEqual},
		{"GET", log.LineMatchNotEqual},
	}

	lines := make([]string, 0, 200)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 200; i++ {
		var b strings.Builder
		n := 1 + rng.Intn(6)
		for j := 0; j < n; j++ {
			b.WriteString(words[rng.Intn(len(words))])
			if rng.Intn(2) == 0 {
				b.WriteByte(' ')
			}
		}
		if rng.Intn(3) == 0 {
			b.WriteByte(byte(0x80 + rng.Intn(0x40))) // non-ascii byte
		}
		lines = append(lines, b.String())
	}

	var checked int
	for _, f := range filters {
		// The fragment is generated from the same match/ty as the filter.
		frag := lineFilterSQLFor(f.match, f.typ)
		if frag.Empty() {
			// Nothing pushed: the property holds trivially (no rows excluded by SQL).
			continue
		}
		lf, err := log.NewFilter(f.match, f.typ)
		if err != nil {
			continue // invalid filter for this type
		}
		for _, line := range lines {
			ok := lf.Filter([]byte(line))
			if !ok {
				continue
			}
			// ClickHouse must return true for this line. Inline the line as a literal
			// (a ?-as-alias subquery mis-binds the leading argument in clickhouse-go).
			sql := "SELECT " + frag.SQL + " FROM (SELECT " + sqlString(line) + " AS line)"
			args := frag.Args
			var res bool
			if err := conn.QueryRow(ctx, sql, args...).Scan(&res); err != nil {
				t.Fatalf("clickhouse %q err: %v", frag.SQL, err)
			}
			require.True(t, res, "line %q accepted by Loki filter %v (%v) but SQL %q is false", line, f.typ, f.match, frag.SQL)
			checked++
		}
	}
	require.Greater(t, checked, 0, "property test checked zero rows")
}

// lineFilterSQLFor mirrors lineFilterSQL for a single filter (no Left chain), used
// by the property test to get the fragment for one (match, ty).
func lineFilterSQLFor(match string, ty log.LineMatchType) Fragment {
	return lineFilterSQL(&syntax.LineFilterExpr{Ty: ty, Match: match})
}
