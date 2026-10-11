package chstore_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// mustMatchers parses a logQL selector and returns its matchers.
func mustMatchers(t *testing.T, q string) []*labels.Matcher {
	t.Helper()
	sel, err := syntax.ParseLogSelector(q, true)
	require.NoError(t, err)
	return sel.Matchers()
}

func TestScanRows_InclusiveBounds(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: "l0"},
			{Timestamp: base.Add(time.Millisecond), Line: "l1"},
			{Timestamp: base.Add(2 * time.Millisecond), Line: "l2"},
			{Timestamp: base.Add(3 * time.Millisecond), Line: "l3"},
		}},
	}, nil))
	// Resolve the stream id.
	streams, err := chstore.ResolveStreams(context.Background(), conn, "t1",
		mustMatchers(t, `{app="a"}`), base.Add(-time.Second), base.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, streams, 1)
	id := streams[0].ID

	var got []string
	err = chstore.ScanRows(context.Background(), conn, "t1", []uint64{id}, base.UnixNano(), base.Add(2*time.Millisecond).UnixNano(), func(r chstore.Row) error {
		got = append(got, r.Line)
		return nil
	})
	require.NoError(t, err)
	sort.Strings(got)
	// Inclusive [base, base+2ms] -> l0, l1, l2 (not l3).
	require.Equal(t, []string{"l0", "l1", "l2"}, got)
}

func TestDeleteRows_Exact(t *testing.T) {
	conn, db := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: "keep"},
			{Timestamp: base.Add(time.Millisecond), Line: "del1"},
			{Timestamp: base.Add(2 * time.Millisecond), Line: "del2"},
		}},
	}, nil))
	streams, _ := chstore.ResolveStreams(context.Background(), conn, "t1",
		mustMatchers(t, `{app="a"}`), base.Add(-time.Second), base.Add(time.Second))
	id := streams[0].ID

	// Collect the keys of del1/del2 via ScanRows.
	var keys []chstore.RowKey
	require.NoError(t, chstore.ScanRows(context.Background(), conn, "t1", []uint64{id}, base.UnixNano(), base.Add(2*time.Millisecond).UnixNano(), func(r chstore.Row) error {
		if r.Line == "del1" || r.Line == "del2" {
			keys = append(keys, r.RowKey)
		}
		return nil
	}))
	require.Len(t, keys, 2)

	require.NoError(t, chstore.DeleteRows(context.Background(), conn, db, chtest.Topology(), "t1", keys))

	got := t1Lines(t, conn)
	require.Equal(t, []string{"keep"}, got)
}

func TestDeleteRange_Exact(t *testing.T) {
	conn, db := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: "keep"},
			{Timestamp: base.Add(time.Millisecond), Line: "x"},
			{Timestamp: base.Add(2 * time.Millisecond), Line: "y"},
		}},
		{Labels: `{app="b"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: "other"},
		}},
	}, nil))
	streams, _ := chstore.ResolveStreams(context.Background(), conn, "t1",
		mustMatchers(t, `{app="a"}`), base.Add(-time.Second), base.Add(time.Second))
	require.Len(t, streams, 1)
	id := streams[0].ID

	// Delete all of {app="a"} in [base, base+2ms]; keep the out-of-range? none.
	require.NoError(t, chstore.DeleteRange(context.Background(), conn, db, chtest.Topology(), "t1", []uint64{id}, base.UnixNano(), base.Add(2*time.Millisecond).UnixNano()))

	// {app="a"} rows gone; {app="b"} other remains.
	got := t1Lines(t, conn)
	require.Equal(t, []string{"other"}, got)
}

// t1Lines returns the sorted distinct lines of tenant t1's logs table.
func t1Lines(t *testing.T, conn driver.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT DISTINCT line FROM logs WHERE tenant = 't1' ORDER BY line")
	require.NoError(t, err)
	var out []string
	for rows.Next() {
		var l string
		_ = rows.Scan(&l)
		out = append(out, l)
	}
	require.NoError(t, rows.Err())
	return out
}
