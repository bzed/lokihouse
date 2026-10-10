package chstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func requireCluster(t *testing.T) {
	if chtest.Topology().Cluster == "" {
		t.Skip("single-node run; use make test-cluster")
	}
}

// A push is acknowledged only when every replica a later read may hit has the rows.
func TestCluster_ReadAfterPush(t *testing.T) {
	requireCluster(t)
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"load_balancing": "round_robin"}))
	for i := 0; i < 40; i++ {
		seed(t, w, "t1", logproto.Stream{
			Labels:  fmt.Sprintf(`{app="a%d"}`, i%7),
			Entries: []logproto.Entry{{Timestamp: t0.Add(time.Duration(i) * time.Second), Line: "x"}},
		})
		var logs, streams uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1'").Scan(&logs))
		require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(stream_id) FROM streams WHERE tenant = 't1'").Scan(&streams))
		require.Equal(t, uint64(i+1), logs, "push %d not visible on every replica", i)
		require.Equal(t, uint64(min(i+1, 7)), streams)
	}
}

// Rows of one stream, in logs and streams, share a shard; streams spread over shards.
func TestCluster_StreamColocation(t *testing.T) {
	requireCluster(t)
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	for i := 0; i < 30; i++ {
		seed(t, w, "t1", logproto.Stream{
			Labels:  fmt.Sprintf(`{app="a%d"}`, i),
			Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}, {Timestamp: t0.Add(time.Second), Line: "y"}},
		})
	}
	ctx := context.Background()
	var split uint64
	require.NoError(t, conn.QueryRow(ctx, `SELECT count() FROM (SELECT stream_id FROM (
		SELECT stream_id, shardNum() AS s FROM logs WHERE tenant = 't1'
		UNION DISTINCT SELECT stream_id, shardNum() AS s FROM streams WHERE tenant = 't1')
		GROUP BY stream_id HAVING uniqExact(s) > 1)`).Scan(&split))
	require.Zero(t, split, "a stream's rows live on more than one shard")
	var shards uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(shardNum()) FROM logs WHERE tenant = 't1'").Scan(&shards))
	require.Equal(t, uint64(2), shards, "30 streams should hash onto both shards")
}
