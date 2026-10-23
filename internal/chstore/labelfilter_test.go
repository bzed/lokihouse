package chstore_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func streamsOf(lbls ...string) []chstore.Stream {
	var out []chstore.Stream
	for i, l := range lbls {
		ls, err := syntax.ParseLabels(l)
		if err != nil {
			panic(err)
		}
		out = append(out, chstore.Stream{ID: uint64(i + 1), Labels: ls})
	}
	return out
}

func parseSel(t *testing.T, q string) syntax.LogSelectorExpr {
	t.Helper()
	sel, err := syntax.ParseLogSelector(q, true)
	require.NoError(t, err)
	return sel
}

func TestMetadataFilterSQL(t *testing.T) {
	a := streamsOf(`{app="a"}`)
	for _, tc := range []struct {
		name    string
		query   string
		streams []chstore.Stream
		sql     string
		args    []any
	}{
		{"equal", `{app="a"} | trace_id="x"`, a, "structured_metadata[?] = ?", []any{"trace_id", "x"}},
		{"regex alternation", `{app="a"} | trace_id=~"ab|cd"`, a, "(structured_metadata[?] = ? OR structured_metadata[?] = ?)", []any{"trace_id", "ab", "trace_id", "cd"}},
		{"single-character alternation is not pushed", `{app="a"} | trace_id=~"x|y"`, a, "", nil},
		{"regex any", `{app="a"} | trace_id=~".+"`, a, "mapContainsKey(structured_metadata, ?)", []any{"trace_id"}},
		{"or", `{app="a"} | trace_id="x" or level="y"`, a, "(structured_metadata[?] = ? OR structured_metadata[?] = ?)", []any{"trace_id", "x", "level", "y"}},
		{"and", `{app="a"} | trace_id="x" and level="y"`, a, "(structured_metadata[?] = ? AND structured_metadata[?] = ?)", []any{"trace_id", "x", "level", "y"}},
		{"two stages", `{app="a"} | trace_id="x" | level="y"`, a, "structured_metadata[?] = ? AND structured_metadata[?] = ?", []any{"trace_id", "x", "level", "y"}},
		{"after json", `{app="a"} | json | trace_id="x"`, a, "", nil},
		{"after label_format", `{app="a"} | label_format trace_id="x" | trace_id="x"`, a, "", nil},
		{"empty value", `{app="a"} | trace_id=""`, a, "", nil},
		{"not equal", `{app="a"} | trace_id!="x"`, a, "", nil},
		{"stream label of the same name", `{app="a"} | trace_id="x"`, streamsOf(`{app="a", trace_id="x"}`), "", nil},
		{"one stream carries the label", `{app=~"a|b"} | trace_id="x"`, streamsOf(`{app="a"}`, `{app="b", trace_id="x"}`), "", nil},
		{"reserved name", `{app="a"} | __error__="x"`, a, "", nil},
		{"or with unsupported side", `{app="a"} | trace_id="x" or level!="y"`, a, "", nil},
		{"and with unsupported side", `{app="a"} | trace_id="x" and level!="y"`, a, "structured_metadata[?] = ?", []any{"trace_id", "x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := chstore.MetadataFilterSQL(parseSel(t, tc.query), tc.streams)
			require.Equal(t, tc.sql, f.SQL)
			require.Equal(t, tc.args, f.Args)
		})
	}
}

// allLines returns the lines of every stream of a log query result, sorted.
func allLines(res logqlmodel.Result) []string {
	var out []string
	for _, s := range res.Data.(logqlmodel.Streams) {
		out = append(out, lines(s)...)
	}
	sort.Strings(out)
	return out
}

func md(kv ...string) []logproto.LabelAdapter {
	var out []logproto.LabelAdapter
	for i := 0; i < len(kv); i += 2 {
		out = append(out, logproto.LabelAdapter{Name: kv[i], Value: kv[i+1]})
	}
	return out
}

func TestSelectLogs_MetadataFilter(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: t0, Line: "a-t1", StructuredMetadata: md("trace_id", "t1")},
			{Timestamp: t0.Add(time.Second), Line: "a-t2", StructuredMetadata: md("trace_id", "t2")},
			{Timestamp: t0.Add(2 * time.Second), Line: "a-none"},
			{Timestamp: t0.Add(3 * time.Second), Line: "a-empty", StructuredMetadata: md("trace_id", "")},
		}},
		logproto.Stream{Labels: `{app="b", trace_id="t1"}`, Entries: []logproto.Entry{
			{Timestamp: t0, Line: "b-1"}, {Timestamp: t0.Add(time.Second), Line: "b-2"},
		}})
	ng := engine(t, &chstore.Querier{Conn: conn})
	q := func(expr string) []string {
		return allLines(run(t, ng, expr, t0.Add(-time.Minute), t0.Add(time.Minute), 0, logproto.FORWARD, 100))
	}
	require.Equal(t, []string{"a-t1", "b-1", "b-2"}, q(`{app=~"a|b"} | trace_id="t1"`))
	require.Equal(t, []string{"a-t1", "a-t2", "b-1", "b-2"}, q(`{app=~"a|b"} | trace_id=~"t1|t2"`))
	require.Equal(t, []string{"a-t1"}, q(`{app="a"} | trace_id="t1"`))
	require.Equal(t, []string{"a-t1", "a-t2"}, q(`{app="a"} | trace_id=~".+"`))
}

func TestMetadataFilter_UsesIndex(t *testing.T) {
	ctx := context.Background()
	const rows = 50000
	conn, db := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	require.NoError(t, conn.Exec(ctx, "INSERT INTO "+logs+" (tenant, stream_id, ts, line, structured_metadata) SELECT 't1', 1, toDateTime64(1, 9, 'UTC') + number, concat('row ', toString(number)), map('trace_id', concat('id', toString(number))) FROM numbers(?)", rows))
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+logs+" FINAL"))

	sel := parseSel(t, `{app="a"} | trace_id="id31337"`)
	f := chstore.MetadataFilterSQL(sel, streamsOf(`{app="a"}`))
	require.False(t, f.Empty())
	since := time.Now()
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1' AND "+f.SQL, f.Args...).Scan(&n))
	require.Equal(t, uint64(1), n)
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2))
}
