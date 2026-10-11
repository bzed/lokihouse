package chstore

import (
	"context"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// TestRegexGate_FuzzAgainstClickHouse is a seeded property test: generate random
// regexes (keeping those with RegexExact == true) and compare Go's regexp against
// ClickHouse match() on a battery of lines. Zero mismatches required.
func TestRegexGate_FuzzAgainstClickHouse(t *testing.T) {
	addr := "localhost:9000"
	if v := os.Getenv("LOKIHOUSE_TEST_CH_ADDR"); v != "" {
		addr = v
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	defer conn.Close()
	ctx := context.Background()

	rng := rand.New(rand.NewSource(1234))

	// 1. Generate random regexes from an ASCII grammar, keep the gated-true ones.
	var regexes []string
	for i := 0; i < 6000 && len(regexes) < 400; i++ {
		re := randomRegex(rng)
		if RegexExact(re) {
			regexes = append(regexes, re)
		}
	}
	require.NotEmpty(t, regexes, "gate accepted zero generated regexes")

	// 2. Generate lines: ASCII, valid multibyte UTF-8, invalid bytes.
	lines := []string{
		"error occurred here",
		"GET /api 200",
		"warn timeout",
		"",
		"plain ascii only",
		"äöü multibyte utf8",                  // valid multibyte
		"café",                                // valid multibyte
		string([]byte{0xff, 0xfe, 0xfd, 'x'}), // invalid utf-8
		string([]byte{'a', 0xc3, 'b'}),        // lone 0xc3 (invalid)
		string([]byte{0x01, 0x02, 'a', 0xff}), // control + invalid
	}

	// 3+4. For every (regex, line) pair, compare Go vs ClickHouse match().
	// Batch: one query per regex with all lines as an array argument.
	mismatches := 0
	for _, re := range regexes {
		goRes := make([]bool, len(lines))
		for i, line := range lines {
			goRes[i] = regexp.MustCompile(re).MatchString(line)
		}
		// One query per regex: arrayMap(l -> match(l, ?), ?) — match() returns UInt8,
		// so cast to an array of 0/1 and compare.
		q := "SELECT arrayMap(l -> match(l, ?), ?)"
		var got []uint8
		if err := conn.QueryRow(ctx, q, re, lines).Scan(&got); err != nil {
			t.Fatalf("clickhouse query for %q: %v", re, err)
		}
		if len(got) != len(lines) {
			t.Fatalf("row count mismatch for %q", re)
		}
		for i := range lines {
			ch := got[i] == 1
			if goRes[i] != ch {
				if mismatches < 10 {
					t.Errorf("mismatch regex=%q line=%q go=%v ch=%v", re, lines[i], goRes[i], ch)
				}
				mismatches++
			}
		}
	}
	require.Equal(t, 0, mismatches, "Go vs ClickHouse regex mismatches")
}

// randomRegex builds a random regex from a small ASCII grammar.
func randomRegex(rng *rand.Rand) string {
	var b strings.Builder
	piece := &randPiece{rng: rng, b: &b}
	piece.gen(0)
	if rng.Intn(2) == 0 {
		b.WriteString("|")
		piece.gen(0)
	}
	if rng.Intn(4) == 0 {
		b.WriteString(`{1,3}`)
	}
	return b.String()
}

type randPiece struct {
	rng *rand.Rand
	b   *strings.Builder
}

func (p *randPiece) gen(depth int) {
	if depth > 3 {
		p.b.WriteString(randomLiteral(p.rng, 1+p.rng.Intn(3)))
		return
	}
	switch p.rng.Intn(6) {
	case 0:
		p.b.WriteString(randomLiteral(p.rng, 1+p.rng.Intn(4)))
	case 1:
		p.b.WriteString("[a-c0-9]")
	case 2:
		p.b.WriteString(`\d`)
	case 3:
		p.b.WriteString("(")
		p.gen(depth + 1)
		p.b.WriteString(")")
		if p.rng.Intn(3) == 0 {
			p.b.WriteString(string([]rune{'*', '+', '?'}[p.rng.Intn(3)]))
		}
	case 4:
		if p.rng.Intn(2) == 0 {
			p.b.WriteString("^")
		} else {
			p.b.WriteString("$")
		}
	default:
		p.gen(depth + 1)
		p.gen(depth + 1)
	}
}

func randomLiteral(rng *rand.Rand, n int) string {
	const letters = "abcxyz019"
	r := make([]byte, n)
	for i := range r {
		r[i] = letters[rng.Intn(len(letters))]
	}
	return string(r)
}
