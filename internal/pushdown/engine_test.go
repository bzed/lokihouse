package pushdown

import (
	"context"
	"flag"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

type nopLogger struct{}

func (nopLogger) Log(...any) error { return nil }

func TestEngine_DownstreamEqualsTierB(t *testing.T) {
	conn, _ := chtest.New(t)
	ng := &chstore.Querier{Conn: conn}
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	tierB := logql.NewEngine(logql.EngineOpts{}, ng, overrides, nopLogger{})

	// Seed a few streams.
	w := &chstore.Writer{Conn: conn}
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	var es []logproto.Entry
	for i := 0; i < 60; i++ {
		es = append(es, logproto.Entry{Timestamp: base.Add(time.Duration(i) * time.Second), Line: "hello world"})
	}
	require.NoError(t, w.Write(context.Background(), "t1",
		[]logproto.Stream{
			{Labels: `{app="a", env="dev"}`, Entries: es},
			{Labels: `{app="a", env="prod"}`, Entries: es[:30]},
		}, nil))

	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := base, base.Add(2*time.Minute)

	// skipTopk is true for queries where topk's winner depends on internal
	// (non-deterministic) iteration order at a float tie; values are equal but
	// series ordering may differ.
	queries := []string{
		`count_over_time({app="a"}[1m])`,
		`sum by (env) (count_over_time({app="a"}[1m]))`,
		`count_over_time({app="a"}[1m]) / count_over_time({app="a"}[2m])`,
		`sum by (env) (count_over_time({app="a"}[1m])) / sum by (env) (count_over_time({app="a"}[2m]))`,
		`topk(1, sum by (env) (count_over_time({app="a"}[1m])))`,
		`label_replace(count_over_time({app="a"}[1m]), "x", "$1", "env", "(.*)")`,
		`absent_over_time({app="nope"}[5m])`,
		`count_over_time({app="a"}[1m]) * 2`,
		`sum(rate({app="a"}[1m]))`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			p, err := logql.NewLiteralParams(q, start, end, time.Minute, 0, logproto.FORWARD, 0, nil, nil)
			require.NoError(t, err)
			want, err := tierB.Query(p).Exec(ctx)
			require.NoError(t, err)
			e := &Engine{Mode: ModeDownstream, TierB: tierB, Limits: overrides, Logger: nopLogger{}}
			got, err := e.Query(ctx, p).Exec(ctx)
			require.NoError(t, err)
			// topk with k < candidates picks its winner by a float tie-break that is
			// non-deterministic between runs; compare the value multiset instead.
			if strings.HasPrefix(strings.TrimSpace(q), "topk") {
				require.Equal(t, sortedValues(want.Data), sortedValues(got.Data), q)
			} else {
				require.Equal(t, want.Data, got.Data, q)
			}
		})
	}
}

// sortedValues returns the sorted list of float values in a Matrix/Vector, so
// non-deterministic series ordering (topk tie-break) does not affect the compare.
func sortedValues(d any) []float64 {
	switch v := d.(type) {
	case promql.Matrix:
		var out []float64
		for _, s := range v {
			for _, p := range s.Floats {
				out = append(out, p.F)
			}
		}
		sort.Float64s(out)
		return out
	case promql.Vector:
		var out []float64
		for _, p := range v {
			out = append(out, p.F)
		}
		sort.Float64s(out)
		return out
	default:
		return nil
	}
}
