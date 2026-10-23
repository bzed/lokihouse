package chstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// seedWindowData writes three hours of logfmt lines, one per minute per
// stream, plus exact duplicates of the entry that sits on the hour boundary.
func seedWindowData(t *testing.T, conn driver.Conn) {
	w := &chstore.Writer{Conn: conn}
	var a, b []logproto.Entry
	for i := range 180 {
		level := []string{"info", "warn"}[i%2]
		ts := t0.Add(time.Duration(i) * time.Minute)
		a = append(a, logproto.Entry{Timestamp: ts, Line: fmt.Sprintf("level=%s n=%d", level, i)})
		b = append(b, logproto.Entry{Timestamp: ts, Line: fmt.Sprintf("level=%s n=%d", level, i+1000)})
	}
	seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: a}, logproto.Stream{Labels: `{app="b"}`, Entries: b})
	for range 3 {
		seed(t, w, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: a[60:61]})
	}
}

func TestSelectSamples_SameResultForAnyWindowSize(t *testing.T) {
	conn, _ := chtest.New(t)
	seedWindowData(t, conn)
	for _, qs := range []string{
		`count_over_time({app=~"a|b"}[5m])`,
		`sum by (level) (rate({app=~"a|b"} | logfmt [5m]))`,
		`bytes_over_time({app="a"}[1h])`,
	} {
		want := run(t, engine(t, &chstore.Querier{Conn: conn}), qs, t0, t0.Add(3*time.Hour), time.Minute, logproto.FORWARD, 0)
		require.NotEmpty(t, want.Data)
		for _, rows := range []uint64{1, 7} {
			got := run(t, engine(t, &chstore.Querier{Conn: conn, WindowRows: rows}), qs, t0, t0.Add(3*time.Hour), time.Minute, logproto.FORWARD, 0)
			require.Equal(t, want.Data, got.Data, "%s windowRows=%d", qs, rows)
		}
	}
	// the three duplicates and the original collapse to one sample
	res := run(t, engine(t, &chstore.Querier{Conn: conn, WindowRows: 7}), `count_over_time({app="a"}[1m])`, t0.Add(time.Hour), t0.Add(time.Hour), 0, logproto.FORWARD, 0)
	require.Equal(t, 1.0, res.Data.(promql.Vector)[0].F)
}

func TestSelectSamples_ReadsInWindows(t *testing.T) {
	conn, _ := chtest.New(t)
	seedWindowData(t, conn)
	q := &chstore.Querier{Conn: conn, WindowRows: 60}
	expr, err := syntax.ParseSampleExpr(`count_over_time({app=~"a|b"}[5m])`)
	require.NoError(t, err)
	it, err := q.SelectSamples(user.InjectOrgID(context.Background(), "t1"), logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
		Start: t0, End: t0.Add(3 * time.Hour), Plan: &plan.QueryPlan{AST: expr},
	}})
	require.NoError(t, err)
	defer it.Close()
	for it.Next() {
	}
	require.NoError(t, it.Err())
	require.GreaterOrEqual(t, chstore.OpenedSampleWindows(it), 6)
}
