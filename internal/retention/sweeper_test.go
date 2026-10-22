package retention_test

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/retention"
)

// now is just after a UTC midnight: a day D with a retention of N days is gone when D+N <= 2026-10-22.
var now = time.Date(2026, 10, 22, 0, 0, 1, 0, time.UTC)

func day(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }

func period(d int) model.Duration { return model.Duration(time.Duration(d) * 24 * time.Hour) }

func limitsWith(t *testing.T, retention int, rules ...validation.StreamRetention) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.RetentionPeriod = period(retention)
	l.StreamRetention = rules
	require.NoError(t, l.Validate())
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func rule(selector string, d int) validation.StreamRetention {
	return validation.StreamRetention{Selector: selector, Priority: 1, Period: period(d)}
}

// push writes one noon entry per given day to the stream.
func push(t *testing.T, conn driver.Conn, tenant, lbls string, days ...int) {
	t.Helper()
	var es []logproto.Entry
	for _, d := range days {
		es = append(es, logproto.Entry{Timestamp: day(d).Add(12 * time.Hour), Line: fmt.Sprintf("day %d", d)})
	}
	require.NoError(t, (&chstore.Writer{Conn: conn}).Write(context.Background(), tenant, []logproto.Stream{{Labels: lbls, Entries: es}}))
}

func rng(from, to int) []int {
	var out []int
	for d := from; d <= to; d++ {
		out = append(out, d)
	}
	return out
}

func sweeper(conn driver.Conn, db string, l *validation.Overrides) *retention.Sweeper {
	return &retention.Sweeper{Conn: conn, DB: db, Topo: chtest.Topology(), Limits: l, Logger: log.NewNopLogger()}
}

// daysOf returns the sorted UTC days (of October) that hold rows in table, for one stream
// (id != 0) or for all streams.
func daysOf(t *testing.T, conn driver.Conn, table string, id uint64) []int {
	t.Helper()
	col := map[string]string{"logs": "toDate(ts)", "streams": "day", "stream_stats_hourly": "toDate(hour)"}[table]
	q := fmt.Sprintf("SELECT DISTINCT toDayOfMonth(%s) AS d FROM %s WHERE ? = 0 OR stream_id = ? ORDER BY d", col, table)
	rows, err := conn.Query(context.Background(), q, id, id)
	require.NoError(t, err)
	defer rows.Close()
	var out []int
	for rows.Next() {
		var d uint8
		require.NoError(t, rows.Scan(&d))
		out = append(out, int(d))
	}
	require.NoError(t, rows.Err())
	return out
}

func streamID(t *testing.T, lbls string) uint64 {
	_, id, err := chstore.StreamIdentity(lbls)
	require.NoError(t, err)
	return id
}

func requireAllTables(t *testing.T, conn driver.Conn, id uint64, want []int) {
	t.Helper()
	for _, tbl := range []string{"logs", "streams", "stream_stats_hourly"} {
		require.Equal(t, want, daysOf(t, conn, tbl, id), tbl)
	}
}

func TestSweep_GlobalRetentionDropsDays(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="a"}`, rng(10, 21)...)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 7)).Sweep(context.Background(), now))
	requireAllTables(t, conn, 0, rng(16, 21))
}

// A changed retention reaches data that is already stored.
func TestSweep_ConfigChangeAppliesToExistingData(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="a"}`, rng(10, 21)...)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 30)).Sweep(context.Background(), now))
	requireAllTables(t, conn, 0, rng(10, 21))
	require.NoError(t, sweeper(conn, db, limitsWith(t, 3)).Sweep(context.Background(), now))
	requireAllTables(t, conn, 0, rng(20, 21))
}

func TestSweep_StreamRuleShorter(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="noisy"}`, rng(10, 21)...)
	push(t, conn, "t1", `{app="other"}`, rng(10, 21)...)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 30, rule(`{app="noisy"}`, 2))).Sweep(context.Background(), now))
	requireAllTables(t, conn, streamID(t, `{app="noisy"}`), rng(21, 21))
	requireAllTables(t, conn, streamID(t, `{app="other"}`), rng(10, 21))
}

func TestSweep_StreamRuleLonger(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="audit"}`, rng(10, 21)...)
	push(t, conn, "t1", `{app="other"}`, rng(10, 21)...)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 2, rule(`{app="audit"}`, 30))).Sweep(context.Background(), now))
	requireAllTables(t, conn, streamID(t, `{app="audit"}`), rng(10, 21))
	requireAllTables(t, conn, streamID(t, `{app="other"}`), rng(21, 21))
}

func TestSweep_ForeverWithRule(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="tmp"}`, rng(10, 21)...)
	push(t, conn, "t1", `{app="other"}`, rng(10, 21)...)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 0, rule(`{app="tmp"}`, 1))).Sweep(context.Background(), now))
	requireAllTables(t, conn, streamID(t, `{app="tmp"}`), nil)
	requireAllTables(t, conn, streamID(t, `{app="other"}`), rng(10, 21))
}

func TestSweep_DayBoundary(t *testing.T) {
	conn, db := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{{Labels: `{app="a"}`, Entries: []logproto.Entry{
		{Timestamp: time.Date(2026, 10, 21, 23, 59, 59, 999999999, time.UTC), Line: "last of 21"},
		{Timestamp: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Line: "first of 22"},
	}}}))
	require.NoError(t, sweeper(conn, db, limitsWith(t, 1)).Sweep(context.Background(), now))
	var lines []string
	rows, err := conn.Query(context.Background(), "SELECT line FROM logs")
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var l string
		require.NoError(t, rows.Scan(&l))
		lines = append(lines, l)
	}
	require.Equal(t, []string{"first of 22"}, lines)
}

// More streams than one IN batch must all be swept.
func TestSweep_ManyStreams(t *testing.T) {
	conn, db := chtest.New(t)
	const n = 5001
	w := &chstore.Writer{Conn: conn}
	var streams []logproto.Stream
	for i := range n {
		streams = append(streams, logproto.Stream{Labels: fmt.Sprintf(`{app="tmp", i="%d"}`, i),
			Entries: []logproto.Entry{{Timestamp: day(10).Add(time.Hour), Line: "old"}}})
	}
	require.NoError(t, w.Write(context.Background(), "t1", streams))
	// keeps a different, longer-lived stream so the day partition cannot be dropped whole
	push(t, conn, "t1", `{app="keep"}`, 10)
	require.NoError(t, sweeper(conn, db, limitsWith(t, 30, rule(`{app="tmp"}`, 1))).Sweep(context.Background(), now))
	var left uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE stream_id != ?", streamID(t, `{app="keep"}`)).Scan(&left))
	require.Zero(t, left)
	require.Equal(t, []int{10}, daysOf(t, conn, "logs", streamID(t, `{app="keep"}`)))
}

func mutations(t *testing.T, conn driver.Conn, db string) uint64 {
	t.Helper()
	src := "system.mutations"
	if c := chtest.Topology().Cluster; c != "" {
		src = fmt.Sprintf("clusterAllReplicas('%s', system, mutations)", c)
	}
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM "+src+" WHERE database = ?", db).Scan(&n))
	return n
}

func TestSweep_NothingToDoIssuesNoMutation(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="tmp"}`, rng(10, 21)...)
	s := sweeper(conn, db, limitsWith(t, 30, rule(`{app="tmp"}`, 2)))
	require.NoError(t, s.Sweep(context.Background(), now))
	after := mutations(t, conn, db)
	require.Positive(t, after)
	require.NoError(t, s.Sweep(context.Background(), now))
	require.Equal(t, after, mutations(t, conn, db))
}

func TestRun_StopsWithContext(t *testing.T) {
	conn, db := chtest.New(t)
	push(t, conn, "t1", `{app="a"}`, rng(10, 21)...)
	s := sweeper(conn, db, limitsWith(t, 7))
	s.Interval = 10 * time.Millisecond
	s.Now = func() time.Time { return now }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	require.Eventually(t, func() bool { return len(daysOf(t, conn, "logs", 0)) == 6 }, 30*time.Second, 20*time.Millisecond)
	cancel()
	<-done
}
