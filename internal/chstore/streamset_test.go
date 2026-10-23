package chstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// A selector can match far more streams than fit into max_query_size when the
// IDs are written into the SQL text. Every path that filters by stream must cope.
func TestManyStreams_AllPathsWork(t *testing.T) {
	const n = 20000
	conn, _ := chtest.New(t)
	streams := make([]logproto.Stream, n)
	for i := range streams {
		streams[i] = logproto.Stream{Labels: fmt.Sprintf(`{app="many", n="%d"}`, i), Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}}
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", streams...)
	ctx := user.InjectOrgID(context.Background(), "t1")
	q := &chstore.Querier{Conn: conn}

	t.Run("SelectLogs", func(t *testing.T) {
		it, err := q.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
			Selector: `{app="many"}`, Start: t0.Add(-time.Minute), End: t0.Add(time.Minute), Direction: logproto.FORWARD, Limit: 1000,
			Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="many"}`)},
		}})
		require.NoError(t, err)
		defer it.Close()
		seen := map[uint64]struct{}{}
		for it.Next() {
			seen[it.StreamHash()] = struct{}{}
		}
		require.NoError(t, it.Err())
		require.Len(t, seen, n)
	})

	t.Run("SelectSamples", func(t *testing.T) {
		res := run(t, engine(t, q), `sum(count_over_time({app="many"}[1m]))`, t0.Add(30*time.Second), t0.Add(30*time.Second), 0, logproto.FORWARD, 0)
		v := res.Data.(promql.Vector)
		require.Len(t, v, 1)
		require.Equal(t, float64(n), v[0].F)
	})

	t.Run("IndexStats", func(t *testing.T) {
		resp, err := q.IndexStats(ctx, &logproto.IndexStatsRequest{
			Matchers: `{app="many"}`,
			From:     model.TimeFromUnixNano(t0.Add(-time.Hour).UnixNano()),
			Through:  model.TimeFromUnixNano(t0.Add(time.Hour).UnixNano()),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(n), resp.Streams)
	})

	ss, err := chstore.ResolveStreams(context.Background(), conn, "t1", mustMatchers(t, `{app="many"}`), t0.Add(-time.Hour), t0.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, ss, n)
	ids := make([]uint64, len(ss))
	for i, s := range ss {
		ids[i] = s.ID
	}

	t.Run("ScanRows", func(t *testing.T) {
		var rows int
		require.NoError(t, chstore.ScanRows(context.Background(), conn, "t1", ids, t0.Add(-time.Hour).UnixNano(), t0.Add(time.Hour).UnixNano(), func(chstore.Row) error {
			rows++
			return nil
		}))
		require.Equal(t, n, rows)
	})

	t.Run("DaysWithRows", func(t *testing.T) {
		days, err := chstore.DaysWithRows(context.Background(), conn, "t1", ids, t0.Add(48*time.Hour))
		require.NoError(t, err)
		require.Equal(t, []time.Time{t0.Truncate(24 * time.Hour)}, days)
	})

	t.Run("BucketCounts", func(t *testing.T) {
		rows, err := chstore.BucketCounts(context.Background(), conn, "t1", ids, t0.Add(-time.Minute).UnixNano(), t0.Add(time.Minute).UnixNano(), int64(time.Minute), chstore.Fragment{}, chstore.BucketOpts{Bytes: true, AllMD: true})
		require.NoError(t, err)
		var total uint64
		for _, r := range rows {
			total += r.Count
		}
		require.Equal(t, uint64(n), total)
	})
}

// Sending the IDs as an external table must still let ClickHouse prune by the
// (tenant, stream_id, ts) primary key.
func TestStreamSet_PrunesByPrimaryKey(t *testing.T) {
	const streams, perStream = 50, 20000
	ctx := context.Background()
	conn, db := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	var ss []logproto.Stream
	for i := 0; i < streams; i++ {
		ss = append(ss, logproto.Stream{Labels: fmt.Sprintf(`{app="p", n="%d"}`, i), Entries: []logproto.Entry{{Timestamp: t0, Line: "seed"}}})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", ss...)
	for i := 0; i < streams; i++ {
		_, id, err := chstore.StreamIdentity(fmt.Sprintf(`{app="p", n="%d"}`, i))
		require.NoError(t, err)
		require.NoError(t, conn.Exec(ctx, "INSERT INTO "+logs+" (tenant, stream_id, ts, line) SELECT 't1', ?, fromUnixTimestamp64Nano(toInt64(?) + toInt64(number) * 1000), 'row' FROM numbers(?)",
			id, t0.UnixNano()+1000, perStream))
	}
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+logs+" FINAL"))

	q := &chstore.Querier{Conn: conn}
	uctx := user.InjectOrgID(ctx, "t1")
	since := time.Now()
	it, err := q.SelectLogs(uctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: `{app="p", n="7"}`, Start: t0.Add(-time.Minute), End: t0.Add(time.Hour), Direction: logproto.FORWARD, Limit: 100000,
		Plan: &plan.QueryPlan{AST: syntax.MustParseExpr(`{app="p", n="7"}`)},
	}})
	require.NoError(t, err)
	var got int
	for it.Next() {
		got++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, perStream+1, got)
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(streams*perStream/10))
}

func TestStreamSet_EmptyIsNoQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	h, err := chstore.HourlyStats(ctx, conn, "t1", nil, t0, t0.Add(time.Hour))
	require.NoError(t, err)
	require.Nil(t, h)
	b, err := chstore.BucketCounts(ctx, conn, "t1", nil, 0, 1, 1, chstore.Fragment{}, chstore.BucketOpts{})
	require.NoError(t, err)
	require.Nil(t, b)
}
