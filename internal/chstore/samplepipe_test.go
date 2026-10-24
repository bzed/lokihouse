package chstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func openSamples(ctx context.Context, t *testing.T, q *chstore.Querier, qs string, from, to time.Time) iter.SampleIterator {
	t.Helper()
	expr, err := syntax.ParseSampleExpr(qs)
	require.NoError(t, err)
	it, err := q.SelectSamples(user.InjectOrgID(ctx, "t1"), logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: from, End: to, Plan: &plan.QueryPlan{AST: expr},
	}})
	require.NoError(t, err)
	return it
}

func seedMany(t *testing.T, conn driver.Conn, n int) {
	t.Helper()
	var es []logproto.Entry
	for i := range n {
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprint("line ", i)})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: es[:n/2]}, logproto.Stream{Labels: `{app="b"}`, Entries: es[n/2:]})
}

func TestSelectSamples_SameResultForAnyWorkerCount(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	var streams []logproto.Stream
	for s := range 12 {
		var es []logproto.Entry
		for i := range 300 {
			es = append(es, logproto.Entry{
				Timestamp: t0.Add(time.Duration(i) * 36 * time.Second),
				Line:      fmt.Sprintf("level=%s duration=%dms", []string{"info", "warn", "error"}[(i+s)%3], 10+(i*7+s)%90),
			})
		}
		streams = append(streams, logproto.Stream{Labels: fmt.Sprintf(`{app="a%d", env="%s"}`, s, []string{"dev", "prod"}[s%2]), Entries: es})
	}
	seed(t, w, "t1", streams...)
	for range 3 { // re-pushes: unmerged duplicates
		seed(t, w, "t1", logproto.Stream{Labels: streams[0].Labels, Entries: streams[0].Entries[10:11]})
	}

	queries := []string{
		`count_over_time({env=~".+"}[5m])`,
		`sum by (level) (rate({env=~".+"} | logfmt [5m]))`,
		`quantile_over_time(0.95, {env="prod"} | logfmt | unwrap duration(duration) [5m]) by (app)`,
		`count_over_time({app="a1"} | logfmt [5m])`,
	}
	start, end := t0.Add(5*time.Minute), t0.Add(3*time.Hour)
	for _, qs := range queries {
		for _, rows := range []uint64{0, 7} {
			want := run(t, engine(t, &chstore.Querier{Conn: conn, Workers: 1, WindowRows: rows}), qs, start, end, 5*time.Minute, logproto.FORWARD, 0)
			for _, n := range []int{3, 64} {
				got := run(t, engine(t, &chstore.Querier{Conn: conn, Workers: n, WindowRows: rows}), qs, start, end, 5*time.Minute, logproto.FORWARD, 0)
				require.Equal(t, want.Data, got.Data, "%s workers=%d window rows=%d", qs, n, rows)
			}
		}
	}

	postFilter := func(workers int) int64 {
		st, ctx := stats.NewContext(user.InjectOrgID(context.Background(), "t1"))
		it := openSamples(ctx, t, &chstore.Querier{Conn: conn, Workers: workers}, `count_over_time({env=~".+"} | logfmt [5m])`, t0, t0.Add(3*time.Hour))
		for it.Next() {
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return st.Result(0, 0, 0).Summary.TotalPostFilterLines
	}
	one := postFilter(1)
	require.Positive(t, one)
	require.Equal(t, one, postFilter(3))
}

// Closing a half-read iterator must stop the producer and workers and cancel the
// ClickHouse query; goleak in TestMain proves the goroutines are gone. FanoutMax
// 1 keeps the query on the global read: its producer is lazy, while a fan-out
// drains its streams before the query returns.
func TestSelectSamples_CloseMidStream(t *testing.T) {
	conn, _ := chtest.New(t)
	seedMany(t, conn, 20000)
	q := &chstore.Querier{Conn: conn, Workers: 4, FanoutMax: 1}
	it := openSamples(context.Background(), t, q, `count_over_time({app=~"a|b"}[1m])`, t0, t0.Add(time.Minute))
	require.True(t, it.Next())

	done := make(chan error, 1)
	go func() { done <- it.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close(), "Close is idempotent")

	unread := openSamples(context.Background(), t, q, `count_over_time({app=~"a|b"}[1m])`, t0, t0.Add(time.Minute))
	require.NoError(t, unread.Close())
}

// A cancelled query context ends the iteration with the context's error, without a hang.
// FanoutMax 1 keeps the query on the global read, whose producer is lazy; a
// fan-out drains before the query returns and surfaces a cancel as its error.
func TestSelectSamples_CancelledContext(t *testing.T) {
	conn, _ := chtest.New(t)
	seedMany(t, conn, 20000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := openSamples(ctx, t, &chstore.Querier{Conn: conn, Workers: 4, FanoutMax: 1}, `count_over_time({app=~"a|b"}[1m])`, t0, t0.Add(time.Minute))
	defer it.Close()
	require.True(t, it.Next())
	cancel()

	drained := make(chan struct{})
	go func() {
		for it.Next() {
		}
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("Next hung after the context was cancelled")
	}
	require.True(t, errors.Is(it.Err(), context.Canceled), "%v", it.Err())
}
