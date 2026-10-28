package chstore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// specCounts snapshots the speculation counters; delta reports the change since.
type specCounts struct{ miss, hit, mismatch float64 }

func snapSpec() specCounts {
	return specCounts{chstore.StreamSpeculations("miss"), chstore.StreamSpeculations("hit"), chstore.StreamSpeculations("mismatch")}
}

func (s specCounts) delta() specCounts {
	n := snapSpec()
	return specCounts{n.miss - s.miss, n.hit - s.hit, n.mismatch - s.mismatch}
}

func entriesOf(res logqlmodel.Result) []logproto.Entry {
	var out []logproto.Entry
	for _, s := range res.Data.(logqlmodel.Streams) {
		out = append(out, s.Entries...)
	}
	return out
}

func TestSelectLogs_SpeculationHitSameResult(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "x 0"}, {Timestamp: t0.Add(30 * time.Minute), Line: "x 1"}, {Timestamp: t0.Add(70 * time.Minute), Line: "x 2"},
		{Timestamp: t0.Add(110 * time.Minute), Line: "x 3"}, {Timestamp: t0.Add(150 * time.Minute), Line: "x 4"}}})
	ref := engine(t, &chstore.Querier{Conn: conn})
	for name, r := range map[string][2]time.Time{
		"1h": {t0, t0.Add(time.Hour)},
		"3h": {t0.Add(-time.Hour), t0.Add(2 * time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			ng := engine(t, &chstore.Querier{Conn: conn, StreamCache: chstore.NewStreamSetCache()})
			want := run(t, ref, `{app="a"} |= "x"`, r[0], r[1], 0, logproto.BACKWARD, 100)
			require.NotEmpty(t, entriesOf(want))
			before := snapSpec()
			require.Equal(t, entriesOf(want), entriesOf(run(t, ng, `{app="a"} |= "x"`, r[0], r[1], 0, logproto.BACKWARD, 100)))
			require.Equal(t, specCounts{miss: 1}, before.delta())
			require.Equal(t, entriesOf(want), entriesOf(run(t, ng, `{app="a"} |= "x"`, r[0], r[1], 0, logproto.BACKWARD, 100)))
			require.Equal(t, specCounts{miss: 1, hit: 1}, before.delta())
		})
	}
}

func TestSelectLogs_SpeculationMismatchSeesNewStream(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a", pod="1"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "p1 0"}, {Timestamp: t0.Add(2 * time.Second), Line: "p1 2"}}})
	ref := engine(t, &chstore.Querier{Conn: conn})
	ng := engine(t, &chstore.Querier{Conn: conn, StreamCache: chstore.NewStreamSetCache()})
	end := t0.Add(time.Minute)

	require.Len(t, entriesOf(run(t, ng, `{app="a"}`, t0, end, 0, logproto.FORWARD, 100)), 2)
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a", pod="2"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(time.Second), Line: "p2 1"}, {Timestamp: t0.Add(3 * time.Second), Line: "p2 3"}}})

	before := snapSpec()
	got := entriesOf(run(t, ng, `{app="a"}`, t0, end, 0, logproto.FORWARD, 100))
	require.Equal(t, entriesOf(run(t, ref, `{app="a"}`, t0, end, 0, logproto.FORWARD, 100)), got)
	require.Len(t, got, 4)
	require.Equal(t, specCounts{mismatch: 1}, before.delta())

	run(t, ng, `{app="a"}`, t0, end, 0, logproto.FORWARD, 100)
	require.Equal(t, specCounts{mismatch: 1, hit: 1}, before.delta())
}

func TestSelectLogs_SpeculationStaleSet(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	cache := chstore.NewStreamSetCache()
	end := t0.Add(time.Minute)
	chstore.SeedStreamSet(cache, "t1", syntax.MustParseExpr(`{app="a"}`).(syntax.LogSelectorExpr).Matchers(), t0, end,
		[]chstore.Stream{{ID: 42, Labels: labels.FromStrings("app", "a")}})

	before := snapSpec()
	got := run(t, engine(t, &chstore.Querier{Conn: conn, StreamCache: cache}), `{app="a"}`, t0, end, 0, logproto.FORWARD, 100)
	require.Equal(t, entriesOf(run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, end, 0, logproto.FORWARD, 100)), entriesOf(got))
	require.Len(t, entriesOf(got), 1)
	require.Equal(t, specCounts{mismatch: 1}, before.delta())
}

func TestSelectLogs_SpeculationConcurrent(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "x 0"}, {Timestamp: t0.Add(time.Second), Line: "x 1"}}})
	end := t0.Add(time.Minute)
	want := entriesOf(run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, end, 0, logproto.BACKWARD, 100))
	ng := engine(t, &chstore.Querier{Conn: conn, StreamCache: chstore.NewStreamSetCache()})
	run(t, ng, `{app="a"}`, t0, end, 0, logproto.BACKWARD, 100) // warm the cache

	const n = 8
	got := make([][]logproto.Entry, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { got[i] = entriesOf(run(t, ng, `{app="a"}`, t0, end, 0, logproto.BACKWARD, 100)) })
	}
	wg.Wait()
	for _, g := range got {
		require.Equal(t, want, g)
	}
}

func TestSelectLogs_SpeculationCancelled(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	q := &chstore.Querier{Conn: conn, StreamCache: chstore.NewStreamSetCache()}
	end := t0.Add(time.Minute)
	p := logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: `{app="a"}`, Start: t0, End: end, Limit: 100, Direction: logproto.BACKWARD,
		Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="a"}`)},
	}}
	ctx := user.InjectOrgID(context.Background(), "t1")
	it, err := q.SelectLogs(ctx, p) // warm the cache
	require.NoError(t, err)
	require.NoError(t, it.Close())

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	it, err = q.SelectLogs(cctx, p)
	require.ErrorIs(t, err, context.Canceled)
	if it != nil {
		require.False(t, it.Next())
		require.NoError(t, it.Close())
	}
}
