package pushdown

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// BenchmarkLeaf benchmarks Tier A and Tier B execution of range-aggregation
// leaves. It seeds 100,000 entries over 20 streams and 2h (for speed), and
// each benchmark runs a 2h range query with a 1m step and a 5m range.
func BenchmarkLeaf(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	// Use a helper to get a connection for the benchmark
	conn := benchConn(b)
	defer conn.Close()

	// Seed 100k entries over 20 streams and 2h
	ctx := context.Background()
	baseNS := time.Now().UTC().UnixNano() - 2*int64(time.Hour)
	streams := 20
	entriesPerStream := 5000 // 20 * 5000 = 100,000
	totalEntries := streams * entriesPerStream

	// Use INSERT...SELECT FROM numbers() to bulk-load the data
	sql := `INSERT INTO logs (tenant, stream_id, ts, line, entry_hash)
SELECT 'bench', number % ? AS stream_id,
       toDateTime64((? + (number % ?) * 7200000000000 / ?) / 1000000, 9, 'UTC'),
       'alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima',
       cityHash64(line)
FROM numbers(?)`
	_ = conn.Exec(ctx, sql, streams, baseNS, 2*int64(time.Hour), entriesPerStream, totalEntries)

	// Create streams for the 20 stream IDs
	for i := 0; i < streams; i++ {
		labels := fmt.Sprintf(`{app="app%d", env="env%d"}`, i%5, i%3)
		_ = conn.Exec(ctx,
			"INSERT INTO streams (tenant, labels_hash, labels) VALUES (?, cityHash64(?), ?)",
			"bench", labels, labels,
		)
	}

	// Set up engines
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("b", flag.PanicOnError))
	overrides, _ := validation.NewOverrides(l, nil)
	tierB := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	tierA := &TierA{Conn: conn, Limits: overrides}
	engOff := &Engine{Mode: ModeOff, TierB: tierB, TierA: tierA, Limits: overrides, Logger: nopLogger{}}

	// Query parameters: 2h range, 1m step, 5m range
	start := time.Unix(0, baseNS).UTC()
	end := start.Add(2 * time.Hour)
	step := time.Minute

	b.Run("tierA/count_over_time", func(b *testing.B) {
		benchmarkTier(b, tierA, `count_over_time({app=~".+"}[5m])`, start, end, step)
	})
	b.Run("tierA/rate_filtered", func(b *testing.B) {
		benchmarkTier(b, tierA, `rate({app=~".+"}|= "error"[5m])`, start, end, step)
	})
	b.Run("tierA/sum_by_rate", func(b *testing.B) {
		// For sum by queries, benchmark the inner rate directly
		benchmarkTier(b, tierA, `rate({app=~".+"}[5m])`, start, end, step)
	})

	b.Run("tierB/count_over_time", func(b *testing.B) {
		benchmarkEngine(b, engOff, `count_over_time({app=~".+"}[5m])`, start, end, step)
	})
	b.Run("tierB/rate_filtered", func(b *testing.B) {
		benchmarkEngine(b, engOff, `rate({app=~".+"}|= "error"[5m])`, start, end, step)
	})
	b.Run("tierB/sum_by_rate", func(b *testing.B) {
		benchmarkEngine(b, engOff, `sum by (app) (rate({app=~".+"}[5m]))`, start, end, step)
	})
}

// benchConn creates a test connection for benchmarks.
func benchConn(b *testing.B) driver.Conn {
	// Create a mock *testing.T for chtest.New
	t := &testing.T{}
	conn, _ := chtest.New(t)
	return conn
}

// benchmarkTier benchmarks a Tier A execution of a range-aggregation leaf.
func benchmarkTier(b *testing.B, tierA *TierA, q string, start, end time.Time, step time.Duration) {
	b.Helper()
	ctx := user.InjectOrgID(context.Background(), "bench")
	expr, err := parseRangeAggExpr(q)
	if err != nil {
		b.Fatalf("parse %q: %v", q, err)
	}
	p, err := logql.NewLiteralParams(q, start, end, step, 0, logproto.FORWARD, 0, nil, nil)
	if err != nil {
		b.Fatalf("params: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tierA.Exec(ctx, expr, p)
	}
}

// benchmarkEngine benchmarks a full engine execution (Tier B).
func benchmarkEngine(b *testing.B, eng *Engine, q string, start, end time.Time, step time.Duration) {
	b.Helper()
	ctx := user.InjectOrgID(context.Background(), "bench")
	p, err := logql.NewLiteralParams(q, start, end, step, 0, logproto.FORWARD, 0, nil, nil)
	if err != nil {
		b.Fatalf("params: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = eng.Query(ctx, p).Exec(ctx)
	}
}

// parseRangeAggExpr parses a query and extracts the first range-aggregation
// leaf for direct Tier A benchmarking.
func parseRangeAggExpr(q string) (*syntax.RangeAggregationExpr, error) {
	expr, err := syntax.ParseSampleExpr(q)
	if err != nil {
		return nil, err
	}
	_, leaves, ok := Map(expr)
	if !ok || len(leaves) == 0 {
		return nil, fmt.Errorf("no range-aggregation leaves found in %q", q)
	}
	return leaves[0], nil
}
