package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestStreamIdentity_CanonicalOrder(t *testing.T) {
	a, ha, err := chstore.StreamIdentity(`{b="2", a="1"}`)
	require.NoError(t, err)
	b, hb, err := chstore.StreamIdentity(`{a="1",b="2"}`)
	require.NoError(t, err)
	require.Equal(t, ha, hb)
	require.Equal(t, `{a="1", b="2"}`, a.String())
	require.Equal(t, a.String(), b.String())
	require.Equal(t, labels.StableHash(labels.FromStrings("a", "1", "b", "2")), ha)
}

func TestWriter_RoundTrip(t *testing.T) {
	conn, _ := chtest.New(t)
	ctx := context.Background()
	w := &chstore.Writer{Conn: conn}
	// Yesterday 23:59:59.123456789 UTC: recent enough for the 31-day TTL (ClickHouse drops
	// already-expired rows at insert time), and the second entry crosses midnight.
	ts := time.Now().UTC().Truncate(24 * time.Hour).Add(-time.Second + 123456789)
	err := w.Write(ctx, "t1", []logproto.Stream{{
		Labels: `{job="x", app="y"}`,
		Entries: []logproto.Entry{
			{Timestamp: ts, Line: "hello", StructuredMetadata: logproto.FromLabelsToLabelAdapters(labels.FromStrings("trace_id", "abc"))},
			{Timestamp: ts.Add(2 * time.Second), Line: "next day"},
		},
	}}, nil)
	require.NoError(t, err)

	var got time.Time
	var line, trace string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT ts, line, structured_metadata['trace_id'] FROM logs WHERE tenant='t1' ORDER BY ts LIMIT 1").Scan(&got, &line, &trace))
	require.True(t, ts.Equal(got), "nanosecond precision preserved: %v vs %v", ts, got)
	require.Equal(t, "hello", line)
	require.Equal(t, "abc", trace)

	var days uint64
	var lstr string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(), any(labels_str) FROM streams FINAL WHERE tenant='t1'").Scan(&days, &lstr))
	require.Equal(t, uint64(2), days, "one streams row per UTC day touched")
	require.Equal(t, `{app="y", job="x"}`, lstr)
}
