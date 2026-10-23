package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestQuerier_Volume(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	tenant := "t1"
	t0 := time.Now().UTC().Truncate(24 * time.Hour)

	// Seed some data
	seedStats(t, w, tenant, logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "test"},
		{Timestamp: t0.Add(time.Second), Line: "test2"},
	}})

	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), tenant)

	// Test Volume with matchers
	req := &logproto.VolumeRequest{
		Matchers:    `{app="a"}`,
		From:        model.TimeFromUnixNano(t0.UnixNano()),
		Through:     model.TimeFromUnixNano(t0.Add(2 * time.Hour).UnixNano()),
		Limit:       10,
		AggregateBy: "series",
	}
	resp, err := q.Volume(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Should return some volume data
	_ = resp
}

func TestQuerier_IndexStats(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	tenant := "t1"
	t0 := time.Now().UTC().Truncate(24 * time.Hour)

	// Seed some data
	seedStats(t, w, tenant, logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "test"},
		{Timestamp: t0.Add(time.Second), Line: "test2"},
	}})

	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), tenant)

	// Test IndexStats with matchers
	req := &logproto.IndexStatsRequest{
		Matchers: `{app="a"}`,
		From:     model.TimeFromUnixNano(t0.UnixNano()),
		Through:  model.TimeFromUnixNano(t0.Add(2 * time.Hour).UnixNano()),
	}
	resp, err := q.IndexStats(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Should return some stats
	_ = resp
}

func TestBucketCounts(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	tenant := "t1"
	t0 := time.Now().UTC().Truncate(24 * time.Hour)
	md := func(level, trace string) []logproto.LabelAdapter {
		return []logproto.LabelAdapter{{Name: "level", Value: level}, {Name: "trace_id", Value: trace}}
	}
	seedStats(t, w, tenant, logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "test", StructuredMetadata: md("info", "t1")},
		{Timestamp: t0.Add(time.Second), Line: "test2", StructuredMetadata: md("info", "t2")},
		{Timestamp: t0.Add(2 * time.Second), Line: "test3", StructuredMetadata: md("warn", "t3")},
	}})

	ctx := user.InjectOrgID(context.Background(), tenant)
	streams, err := chstore.ResolveStreams(ctx, conn, tenant, nil, t0, t0.Add(time.Hour))
	require.NoError(t, err)
	var ids []uint64
	for _, s := range streams {
		ids = append(ids, s.ID)
	}
	origin := t0.Add(-time.Second).UnixNano()
	through := t0.Add(time.Hour).UnixNano()
	g := time.Hour.Nanoseconds()
	run := func(opt chstore.BucketOpts) []chstore.BucketRow {
		rows, err := chstore.BucketCounts(ctx, conn, tenant, ids, origin, through, g, chstore.Fragment{}, opt)
		require.NoError(t, err)
		return rows
	}
	total := func(rows []chstore.BucketRow) (n, b uint64) {
		for _, r := range rows {
			n += r.Count
			b += r.Bytes
		}
		return n, b
	}

	t.Run("bytes and all metadata", func(t *testing.T) {
		rows := run(chstore.BucketOpts{Bytes: true, AllMD: true})
		require.Len(t, rows, 3)
		n, b := total(rows)
		require.Equal(t, uint64(3), n)
		require.Equal(t, uint64(len("test")+len("test2")+len("test3")), b)
		for _, r := range rows {
			require.Len(t, r.MD, 2)
		}
	})
	t.Run("no bytes", func(t *testing.T) {
		rows := run(chstore.BucketOpts{AllMD: true})
		n, b := total(rows)
		require.Equal(t, uint64(3), n)
		require.Zero(t, b)
	})
	t.Run("metadata subset", func(t *testing.T) {
		rows := run(chstore.BucketOpts{MDKeys: []string{"level"}})
		require.Len(t, rows, 2)
		perLevel := map[string]uint64{}
		for _, r := range rows {
			require.Len(t, r.MD, 1)
			perLevel[r.MD["level"]] += r.Count
		}
		require.Equal(t, map[string]uint64{"info": 2, "warn": 1}, perLevel)
	})
	t.Run("no metadata", func(t *testing.T) {
		rows := run(chstore.BucketOpts{MDKeys: []string{}})
		require.Len(t, rows, 1)
		require.Empty(t, rows[0].MD)
		require.Equal(t, uint64(3), rows[0].Count)
	})
}

func seedStats(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams))
}
