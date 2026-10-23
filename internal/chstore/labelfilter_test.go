package chstore_test

import (
	"context"
	"fmt"
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
		{"renamed on a clash with a stream label", `{app="a"} | level_extracted="x"`, streamsOf(`{app="a", level="y"}`), "", nil},
		{"_extracted without a clash", `{app="a"} | level_extracted="x"`, a, "structured_metadata[?] = ?", []any{"level_extracted", "x"}},
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
	// A metadata key that clashes with a stream label is renamed to <key>_extracted.
	seed(t, w, "t1", logproto.Stream{Labels: `{app="c", level="x"}`, Entries: []logproto.Entry{
		{Timestamp: t0, Line: "c-y", StructuredMetadata: md("level", "y")},
		{Timestamp: t0.Add(time.Second), Line: "c-z", StructuredMetadata: md("level", "z")},
	}})
	ng := engine(t, &chstore.Querier{Conn: conn})
	q := func(expr string) []string {
		return allLines(run(t, ng, expr, t0.Add(-time.Minute), t0.Add(time.Minute), 0, logproto.FORWARD, 100))
	}
	require.Equal(t, []string{"a-t1", "b-1", "b-2"}, q(`{app=~"a|b"} | trace_id="t1"`))
	require.Equal(t, []string{"a-t1", "a-t2", "b-1", "b-2"}, q(`{app=~"a|b"} | trace_id=~"t1|t2"`))
	require.Equal(t, []string{"a-t1"}, q(`{app="a"} | trace_id="t1"`))
	require.Equal(t, []string{"a-t1", "a-t2"}, q(`{app="a"} | trace_id=~".+"`))
	require.Equal(t, []string{"c-y"}, q(`{app="c"} | level_extracted="y"`))
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

func TestParsedFilterSQL(t *testing.T) {
	a := streamsOf(`{app="a"}`)
	const tail = " OR mapContainsKey(structured_metadata, ?) OR line_ascii_escape)"
	for _, tc := range []struct {
		name    string
		query   string
		streams []chstore.Stream
		sql     string
		args    []any
	}{
		{"json equality", `{app="a"} | json | level="error"`, a, "(line LIKE ?" + tail, []any{"%error%", "level"}},
		{"logfmt value with space", `{app="a"} | logfmt | msg="hello world"`, a, "(line LIKE ?" + tail, []any{"%hello world%", "msg"}},
		{"value with a newline splits into runs", `{app="a"} | json | msg="alpha\nbravo"`, a, "((line LIKE ? AND line LIKE ?)" + tail, []any{"%alpha%", "%bravo%", "msg"}},
		{"html-escaped character splits the run", `{app="a"} | json | msg="a<bcde"`, a, "(line LIKE ?" + tail, []any{"%bcde%", "msg"}},
		{"LIKE wildcards are escaped", `{app="a"} | json | msg="ab_c%d"`, a, "(line LIKE ?" + tail, []any{`%ab\_c\%d%`, "msg"}},
		{"line filter before the label filter", `{app="a"} | json |= "x" | level="error"`, a, "(line LIKE ?" + tail, []any{"%error%", "level"}},
		{"unpushable filter between", `{app="a"} | json | status >= 500 | level="error"`, a, "(line LIKE ?" + tail, []any{"%error%", "level"}},
		{"two filters", `{app="a"} | json | level="error" | code="E4711"`, a,
			"(line LIKE ?" + tail + " AND (line LIKE ?" + tail, []any{"%error%", "level", "%E4711%", "code"}},
		{"stage after the filter ends the walk", `{app="a"} | json | level="error" | line_format "x" | code="E4711"`, a, "(line LIKE ?" + tail, []any{"%error%", "level"}},
		{"and", `{app="a"} | json | level="error" and code="E4711"`, a,
			"((line LIKE ?" + tail + " AND (line LIKE ?" + tail + ")", []any{"%error%", "level", "%E4711%", "code"}},
		{"or", `{app="a"} | json | level="error" or code="E4711"`, a,
			"((line LIKE ?" + tail + " OR (line LIKE ?" + tail + ")", []any{"%error%", "level", "%E4711%", "code"}},
		{"or with a short side", `{app="a"} | json | level="error" or code="E1"`, a, "", nil},
		{"no run of four", `{app="a"} | json | code="E1"`, a, "", nil},
		{"non-ASCII value", `{app="a"} | json | level="fehlerö"`, a, "", nil},
		{"before the parser", `{app="a"} | level="error" | json`, a, "", nil},
		{"no parser", `{app="a"} |= "x"`, a, "", nil},
		{"json with expressions", `{app="a"} | json lvl="level" | lvl="error"`, a, "", nil},
		{"line_format before the filter", `{app="a"} | json | line_format "x" | level="error"`, a, "", nil},
		{"label_format before the filter", `{app="a"} | json | label_format level="x" | level="error"`, a, "", nil},
		{"regexp parser", `{app="a"} | regexp "(?P<level>\\w+)" | level="error"`, a, "", nil},
		{"numeric comparison", `{app="a"} | json | status >= 500`, a, "", nil},
		{"regex", `{app="a"} | json | level=~"error.*"`, a, "", nil},
		{"not equal", `{app="a"} | json | level!="error"`, a, "", nil},
		{"reserved name", `{app="a"} | json | __error__="JSONParserErr"`, a, "", nil},
		{"stream label of the same name", `{app="a"} | json | level="error"`, streamsOf(`{app="a", level="x"}`), "", nil},
		{"renamed on a clash with a stream label", `{app="a"} | json | level_extracted="error"`, streamsOf(`{app="a", level="x"}`), "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := chstore.ParsedFilterSQL(parseSel(t, tc.query), tc.streams)
			require.Equal(t, tc.sql, f.SQL)
			require.Equal(t, tc.args, f.Args)
		})
	}
}

func TestSelectLogs_ParsedFilter(t *testing.T) {
	type tcase struct {
		name   string
		stream string
		lines  []string
		md     []logproto.LabelAdapter
		query  string
		want   []string
	}
	for _, tc := range []tcase{
		{name: "json equality", stream: `{app="a"}`, lines: []string{`{"level":"error"}`, `{"level":"info"}`}, query: `| json | level="error"`, want: []string{`{"level":"error"}`}},
		{name: "newline in value", stream: `{app="a"}`, lines: []string{`{"msg":"alpha\nbravo"}`, `{"msg":"alpha bravo"}`}, query: `| json | msg="alpha\nbravo"`, want: []string{`{"msg":"alpha\nbravo"}`}},
		{name: "html escape in value", stream: `{app="a"}`, lines: []string{`{"msg":"a\u003cbcde"}`}, query: `| json | msg="a<bcde"`, want: []string{`{"msg":"a\u003cbcde"}`}},
		{name: "letters as escapes", stream: `{app="a"}`, lines: []string{`{"level":"\u0065rror"}`, `{"level":"info"}`}, query: `| json | level="error"`, want: []string{`{"level":"\u0065rror"}`}},
		{name: "metadata of the same name", stream: `{app="a"}`, lines: []string{`{"level":"info"}`}, md: md("level", "error"), query: `| json | level="error"`, want: []string{`{"level":"info"}`}},
		{name: "stream label of the same name", stream: `{app="a", level="error"}`, lines: []string{`{"level":"info"}`}, query: `| json | level="error"`, want: []string{`{"level":"info"}`}},
		{name: "logfmt", stream: `{app="a"}`, lines: []string{`level=error msg="hello world"`, `level=error msg="hello there"`}, query: `| logfmt | msg="hello world"`, want: []string{`level=error msg="hello world"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, _ := chtest.New(t)
			w := &chstore.Writer{Conn: conn}
			var entries []logproto.Entry
			for i, l := range tc.lines {
				entries = append(entries, logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Second), Line: l, StructuredMetadata: tc.md})
			}
			seed(t, w, "t1", logproto.Stream{Labels: tc.stream, Entries: entries},
				logproto.Stream{Labels: `{app="other"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: `{"level":"noise"}`}}})
			ng := engine(t, &chstore.Querier{Conn: conn})
			res := run(t, ng, `{app=~"a|other"} `+tc.query, t0.Add(-time.Minute), t0.Add(time.Minute), 0, logproto.FORWARD, 100)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			require.Equal(t, want, allLines(res))
		})
	}
}

func TestParsedFilter_UsesIndex(t *testing.T) {
	ctx := context.Background()
	const rows = 50000
	conn, db := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	// Go's JSON encoder writes < as an HTML escape (backslash, u003c); such lines must
	// not set the escape flag, or the flag would be useless as a filter.
	htmlEscaped := string(rune(92)) + "u003c"
	entries := make([]logproto.Entry, rows)
	for i := range entries {
		line := fmt.Sprintf(`{"code":"ok","n":%d}`, i)
		switch {
		case i == 31337:
			line = `{"code":"E4711xyz","n":1}`
		case i%50 == 0:
			line = fmt.Sprintf(`{"msg":"a%sb","n":%d}`, htmlEscaped, i)
		}
		entries[i] = logproto.Entry{Timestamp: t0.Add(time.Duration(i) * time.Millisecond), Line: line}
	}
	seed(t, &chstore.Writer{Conn: conn}, "t1", logproto.Stream{Labels: `{app="a"}`, Entries: entries})
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+logs+" FINAL"))

	var flagged uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE line_ascii_escape").Scan(&flagged))
	require.Zero(t, flagged)

	f := chstore.ParsedFilterSQL(parseSel(t, `{app="a"} | json | code="E4711xyz"`), streamsOf(`{app="a"}`))
	require.False(t, f.Empty())
	since := time.Now()
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1' AND "+f.SQL, f.Args...).Scan(&n))
	require.Equal(t, uint64(1), n)
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2))

	// The querier adds the fragment to its reads.
	since = time.Now()
	res := run(t, engine(t, &chstore.Querier{Conn: conn}), `{app="a"} | json | code="E4711xyz"`, t0.Add(-time.Minute), t0.Add(time.Hour), 0, logproto.FORWARD, 100)
	require.Equal(t, []string{`{"code":"E4711xyz","n":1}`}, allLines(res))
	require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2))
}
