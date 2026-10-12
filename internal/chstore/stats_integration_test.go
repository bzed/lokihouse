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

	// Seed some data
	seedStats(t, w, tenant, logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "test"},
		{Timestamp: t0.Add(time.Second), Line: "test2"},
	}})

	ctx := user.InjectOrgID(context.Background(), tenant)
	streams, err := chstore.ResolveStreams(ctx, conn, tenant, nil, t0, t0.Add(time.Hour))
	require.NoError(t, err)

	// Extract stream IDs
	var ids []uint64
	for _, s := range streams {
		ids = append(ids, s.ID)
	}
	origin := t0.UnixNano()
	through := t0.Add(time.Hour).UnixNano()
	g := time.Hour.Nanoseconds()

	// Test BucketCounts with no filter
	rows, err := chstore.BucketCounts(ctx, conn, tenant, ids, origin, through, g, chstore.Fragment{})
	require.NoError(t, err)
	require.NotNil(t, rows)
	_ = rows
}

func seedStats(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams, nil))
}
