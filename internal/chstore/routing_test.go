package chstore_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// A single-stream log query is routed to the shard owning the stream: a
// stream on the initiator's shard causes no remote query, a stream on another
// shard exactly one.
func TestSelectLogs_SingleStreamRoutesToOwningShard(t *testing.T) {
	topo := chtest.Topology()
	if topo.Cluster == "" {
		t.Skip("needs a cluster")
	}
	conn, db := chtest.New(t)
	local, remote := seedRoutedStreams(t, conn, db, topo.Cluster)

	remoteSelects := func(app string) uint64 {
		since := time.Now()
		run(t, engine(t, &chstore.Querier{Conn: conn}), fmt.Sprintf(`{app="%s"}`, app), t0.Add(-time.Minute), t0.Add(10*time.Minute), 0, logproto.FORWARD, 100)
		require.NoError(t, conn.Exec(context.Background(), "SYSTEM FLUSH LOGS ON CLUSTER "+topo.Cluster))
		var got uint64
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM clusterAllReplicas("+topo.Cluster+", system, query_log) WHERE type = 'QueryFinish' AND NOT is_initial_query AND query_kind = 'Select' AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", since.UnixMicro(), "`"+db+"`.`"+topo.Local("logs")+"`").Scan(&got))
		return got
	}
	require.Equal(t, uint64(0), remoteSelects(local), "a stream on the initiator's shard produces no remote query")
	require.Equal(t, uint64(1), remoteSelects(remote), "a stream on another shard produces exactly one remote query")
}

// seedRoutedStreams seeds candidate streams and returns one whose shard holds
// the initiator and one whose shard does not. It skips the test when the
// candidates did not split across shards.
func seedRoutedStreams(t *testing.T, conn driver.Conn, db, cluster string) (local, remote string) {
	t.Helper()
	ctx := context.Background()

	var initHost string
	require.NoError(t, conn.QueryRow(ctx, "SELECT hostName()").Scan(&initHost))

	// replicasPerShard is the number of hosts every shard of the test cluster
	// runs; the inserts are quorum-synchronous, so once a stream's rows are
	// visible on that many hosts the owner set below is complete.
	replicas := map[uint32]int{}
	rows, err := conn.Query(ctx, "SELECT shard_num, count() FROM system.clusters WHERE cluster = ? GROUP BY shard_num", cluster)
	require.NoError(t, err)
	for rows.Next() {
		var shard uint32
		var n uint64
		require.NoError(t, rows.Scan(&shard, &n))
		replicas[shard] = int(n)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotEmpty(t, replicas)
	replicasPerShard := -1
	for _, n := range replicas {
		if replicasPerShard == -1 {
			replicasPerShard = n
		}
		require.Equal(t, replicasPerShard, n, "uniform replica count per shard")
	}

	// Seed candidate streams and classify each by the shard owning it.
	const candidates = 16 // enough to land streams on every shard
	seeded := make([]logproto.Stream, 0, candidates)
	for i := range candidates {
		seeded = append(seeded, logproto.Stream{
			Labels: fmt.Sprintf(`{app="s%02d"}`, i),
			Entries: []logproto.Entry{
				{Timestamp: t0, Line: "line one"},
				{Timestamp: t0.Add(time.Minute), Line: "line two"},
			},
		})
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", seeded...)

	ms, err := syntax.ParseMatchers(`{app=~"s[0-9]+"}`, false)
	require.NoError(t, err)
	streams, err := chstore.ResolveStreams(user.InjectOrgID(ctx, "t1"), conn, "t1", ms, t0.Add(-time.Minute), t0.Add(10*time.Minute))
	require.NoError(t, err)
	require.Len(t, streams, candidates)
	for _, s := range streams {
		var hosts []string
		require.Eventually(t, func() bool {
			hosts = hostsWithStream(t, conn, cluster, db, s.ID)
			return len(hosts) == replicasPerShard
		}, 10*time.Second, 50*time.Millisecond, "stream %d on every replica of its shard", s.ID)
		app := s.Labels.Get("app")
		if slices.Contains(hosts, initHost) {
			local = app // on the initiator's shard: no remote query
		} else {
			remote = app // on another shard: exactly one remote query
		}
	}
	if local == "" || remote == "" {
		t.Skipf("the %d candidate streams did not split across shards", candidates)
	}
	return local, remote
}

// hostsWithStream returns the hosts whose local logs table holds rows of streamID.
func hostsWithStream(t *testing.T, conn driver.Conn, cluster, db string, streamID uint64) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT DISTINCT hostName() FROM clusterAllReplicas("+cluster+", "+db+", logs_local) WHERE stream_id = ?", streamID)
	require.NoError(t, err)
	defer rows.Close()
	var hosts []string
	for rows.Next() {
		var h string
		require.NoError(t, rows.Scan(&h))
		hosts = append(hosts, h)
	}
	require.NoError(t, rows.Err())
	return hosts
}

// Above the fan-out limit the sample query reads all streams in one query in
// the global order, planning windows from the hourly stats as before.

// queriesSince returns the finished Select queries logged since t that contain substr.
func queriesSince(t *testing.T, conn driver.Conn, db string, since time.Time, substr string) []string {
	t.Helper()
	flush, src := "SYSTEM FLUSH LOGS query_log", "system.query_log"
	if topo := chtest.Topology(); topo.Cluster != "" {
		flush = "SYSTEM FLUSH LOGS ON CLUSTER " + topo.Cluster
		src = fmt.Sprintf("clusterAllReplicas(%s, system, query_log)", topo.Cluster)
	}
	require.NoError(t, conn.Exec(context.Background(), flush))
	rows, err := conn.Query(context.Background(), "SELECT query FROM "+src+" WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0", db, since.UnixMicro(), substr)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	require.NoError(t, rows.Err())
	return out
}
