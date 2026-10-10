package chstore_test

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// Yesterday 12:00 UTC.
var t0 = time.Now().UTC().Truncate(24 * time.Hour).Add(-12 * time.Hour)

func seed(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams, nil))
}

func engine(t *testing.T, q logql.Querier) *logql.QueryEngine {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return logql.NewEngine(logql.EngineOpts{}, q, o, log.NewNopLogger())
}

func run(t *testing.T, ng *logql.QueryEngine, qs string, start, end time.Time, step time.Duration, dir logproto.Direction, limit uint32) logqlmodel.Result {
	p, err := logql.NewLiteralParams(qs, start, end, step, 0, dir, limit, nil, nil)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	res, err := ng.Query(p).Exec(ctx)
	require.NoError(t, err)
	return res
}

func TestSelectLogs_FilterAndDirection(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: t0, Line: "keep 1"}, {Timestamp: t0.Add(time.Second), Line: "drop"}, {Timestamp: t0.Add(2 * time.Second), Line: "keep 2"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep other app"}}})
	seed(t, w, "t2", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep foreign tenant"}}})

	ng := engine(t, &chstore.Querier{Conn: conn})
	res := run(t, ng, `{app="a"} |= "keep"`, t0, t0.Add(time.Minute), 0, logproto.BACKWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="a"}`, streams[0].Labels)
	require.Equal(t, []string{"keep 2", "keep 1"}, lines(streams[0]))
}

func TestSelectLogs_TimeBoundaries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0.Add(-time.Nanosecond), Line: "before"}, {Timestamp: t0, Line: "start"},
		{Timestamp: t0.Add(time.Minute - time.Nanosecond), Line: "last"}, {Timestamp: t0.Add(time.Minute), Line: "end"}},
	})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"start", "last"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectLogs_DedupesIdenticalEntries(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	s := logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "same"}}}
	seed(t, w, "t1", s)
	seed(t, w, "t1", s)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"}`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{"same"}, lines(res.Data.(logqlmodel.Streams)[0]))
}

func TestSelectLogs_RegexSelectorAndParser(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="api-1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}, {Timestamp: t0.Add(time.Second), Line: `{"status":200}`}}},
		logproto.Stream{Labels: `{app="web"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"status":500}`}}})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app=~"api-.*"} | json | status="500"`, t0, t0.Add(time.Minute), 0, logproto.FORWARD, 100)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="api-1", status="500"}`, streams[0].Labels)
}

func TestSelectSamples_CountOverTime(t *testing.T) {
	conn, _ := chtest.New(t)
	var es []logproto.Entry
	for i := 0; i < 10; i++ {
		es = append(es, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * 10 * time.Second), Line: "x"})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: es})
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `sum(count_over_time({app="a"}[1m]))`, t0.Add(time.Minute), t0.Add(2*time.Minute), time.Minute, logproto.FORWARD, 0)
	m := res.Data.(promql.Matrix)
	require.Len(t, m, 1)
	// entries at t0+0s..90s; Loki windows are left-open: (t0, t0+1m] = 10s..60s, (t0+1m, t0+2m] = 70s..90s
	require.Equal(t, 6.0, m[0].Floats[0].F)
	require.Equal(t, 3.0, m[0].Floats[1].F)
}

func TestSelectSamples_StreamsInTimestampOrder(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(2 * time.Second), Line: "3"}}},
		logproto.Stream{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "1"}, {Timestamp: t0.Add(time.Second), Line: "2"}}},
	)
	q := &chstore.Querier{Conn: conn}
	expr, err := syntax.ParseSampleExpr(`count_over_time({app=~"a|b"}[1m])`)
	require.NoError(t, err)
	ctx := user.InjectOrgID(context.Background(), "t1")
	it, err := q.SelectSamples(ctx, logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: t0.Add(-time.Minute), End: t0.Add(time.Minute), Plan: &plan.QueryPlan{AST: expr},
	}})
	require.NoError(t, err)
	defer it.Close()
	var got []string
	var last int64
	for it.Next() {
		s := it.At()
		require.GreaterOrEqual(t, s.Timestamp, last, "timestamps never go backwards")
		last = s.Timestamp
		got = append(got, it.Labels())
	}
	require.NoError(t, it.Err())
	// Equal timestamps order by stream_id (cityHash64 of the label set);
	// at t0 the b stream comes first, then at t0+1s b, at t0+2s a.
	require.Equal(t, []string{`{app="b"}`, `{app="a"}`, `{app="b"}`, `{app="a"}`}, got)
}

func lines(s logproto.Stream) []string {
	var out []string
	for _, e := range s.Entries {
		out = append(out, e.Line)
	}
	return out
}
