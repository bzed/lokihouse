package chstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// The pushed-down line filters must be in a form ClickHouse can answer from the
// line_text index; otherwise every granule of the selected streams is read.
func TestLineFilterSQL_UsesTextIndex(t *testing.T) {
	ctx := context.Background()
	const rows = 50000
	conn, db := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	require.NoError(t, conn.Exec(ctx, "INSERT INTO "+logs+" (tenant, stream_id, ts, line) SELECT 't1', 1, toDateTime64(1, 9, 'UTC') + number, concat('row ', toString(number), if(number = 31337, ' needle here', '')) FROM numbers(?)", rows))
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+logs+" FINAL"))

	count := func(f chstore.Fragment) {
		t.Helper()
		var n uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1' AND "+f.SQL, f.Args...).Scan(&n))
	}
	check := func(q string, f chstore.Fragment) {
		t.Helper()
		since := time.Now()
		count(f)
		require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2), q)
	}
	for _, q := range []string{`{a="b"} |= "needle"`, `{a="b"} |~ "needle"`, `{a="b"} |~ "needle(s|d)?"`, `{a="b"} |= "needle" or "haystack"`, `{a="b"} |~ "(?i)needle"`} {
		sel, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		check("leading: "+q, chstore.LeadingLineFilterSQL(sel))
		if strings.Contains(q, "(?i)") {
			continue // case-insensitive filters are not exact, so Tier A does not use them
		}
		f, _, ok := chstore.ExactFilterSQL(sel)
		require.True(t, ok, q)
		check("exact: "+q, f)
	}
}

// The sample read must not stop the text index from skipping granules of an
// unmerged table. A single stream reads fanned out, in the part's physical
// order and without FINAL (its duplicates arrive adjacent and the row reader
// removes them), so the index prunes fully; above the fan-out limit the read
// keeps FINAL and stays exact (TestSelectSamples_TextIndexAboveFanoutMax).
func TestSelectSamples_KeepsTextIndex(t *testing.T) {
	ctx := context.Background()
	const rows = 50000
	conn, db := chtest.New(t)
	es := make([]logproto.Entry, rows)
	for i := range es {
		es[i] = logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprint("row ", i)}
	}
	es[31337].Line += " needle here"
	require.NoError(t, (&chstore.Writer{Conn: conn}).Write(ctx, "t1", []logproto.Stream{{Labels: `{a="b"}`, Entries: es}}))
	since := time.Now()
	end := t0.Add(time.Hour)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `count_over_time({a="b"} |= "needle" [1h])`, end, end, 0, logproto.FORWARD, 0)
	v := res.Data.(promql.Vector)
	require.Len(t, v, 1)
	require.Equal(t, 1.0, v[0].F)
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2))
}

// With overlapping unmerged parts the needle is found once even though a newer
// part re-pushes it and holds a second match: the part-order read returns the
// duplicates adjacent and the row reader removes them (above the fan-out limit
// FINAL collapses them instead, see TestSelectSamples_TextIndexAboveFanoutMax).
func TestSelectSamples_DedupAcrossParts(t *testing.T) {
	ctx := context.Background()
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	es := make([]logproto.Entry, 20000)
	for i := range es {
		es[i] = logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprint("row ", i)}
	}
	es[100].Line += " needle"
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{a="b"}`, Entries: es}}))
	require.NoError(t, w.Write(ctx, "t1", []logproto.Stream{{Labels: `{a="b"}`, Entries: []logproto.Entry{
		es[100], {Timestamp: es[5000].Timestamp, Line: "other needle"}}}}))

	end := t0.Add(time.Hour)
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `count_over_time({a="b"} |= "needle" [1h])`, end, end, 0, logproto.FORWARD, 0)
	v := res.Data.(promql.Vector)
	require.Len(t, v, 1)
	require.Equal(t, 2.0, v[0].F)
}
