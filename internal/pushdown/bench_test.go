package pushdown

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// BenchmarkLeaf compares the push-down engine (mode on, Tier A leaves) with the
// Phase 1 engine (mode off, Tier B) end to end. It seeds 100,000 entries over
// 20 streams and 2h; each query is a 2h range query with a 1m step and a 5m
// range. Both modes must return the same data and Tier A must actually run.
func BenchmarkLeaf(b *testing.B) {
	conn, _ := chtest.New(b)
	const streams, perStream = 20, 5000
	base := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Hour)
	words := []string{"alpha", "bravo", "error", "delta", "echo"}
	for s := range streams {
		es := make([]logproto.Entry, perStream)
		for i := range es {
			es[i] = logproto.Entry{
				Timestamp: base.Add(time.Duration(i) * 2 * time.Hour / perStream).Add(time.Duration(s) * time.Millisecond),
				Line:      fmt.Sprintf("%s request %d took %dms", words[(i+s)%len(words)], i, i%977),
			}
		}
		lbls := fmt.Sprintf(`{app="app%d", env="env%d", n="%d"}`, s%5, s%3, s)
		require.NoError(b, (&chstore.Writer{Conn: conn}).Write(context.Background(), "bench",
			[]logproto.Stream{{Labels: lbls, Entries: es}}, nil))
	}

	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("b", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(b, err)
	tierB := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	tierA := &TierA{Conn: conn, Limits: overrides}
	engs := map[string]*Engine{
		"tierA": {Mode: ModeOn, TierB: tierB, TierA: tierA, Limits: overrides, Logger: nopLogger{}},
		"tierB": {Mode: ModeOff, TierB: tierB, TierA: tierA, Limits: overrides, Logger: nopLogger{}},
	}
	ctx := user.InjectOrgID(context.Background(), "bench")
	start, end := base, base.Add(2*time.Hour)

	for _, q := range []struct{ name, q string }{
		{"count_over_time", `count_over_time({app=~".+"}[5m])`},
		{"rate_filtered", `rate({app=~".+"} |= "error"[5m])`},
		{"bytes_rate", `bytes_rate({app=~".+"}[5m])`},
		{"sum_by_rate", `sum by (app) (rate({app=~".+"}[5m]))`},
	} {
		p, err := logql.NewLiteralParams(q.q, start, end, time.Minute, 0, logproto.FORWARD, 0, nil, nil)
		require.NoError(b, err)
		want, err := engs["tierB"].Query(ctx, p).Exec(ctx)
		require.NoError(b, err)
		before := TierALeaves.Load()
		got, err := engs["tierA"].Query(ctx, p).Exec(ctx)
		require.NoError(b, err)
		requireDataEqual(b, want.Data, got.Data, q.q)
		require.Greater(b, TierALeaves.Load(), before, "Tier A did not run for %s", q.q)
		for _, tier := range []string{"tierA", "tierB"} {
			b.Run(tier+"/"+q.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := engs[tier].Query(ctx, p).Exec(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
