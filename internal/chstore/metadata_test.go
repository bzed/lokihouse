package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestLabelAndSeries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="a", env="dev"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", env="prod"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	seed(t, &chstore.Writer{Conn: conn}, "t2",
		logproto.Stream{Labels: `{secret="1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := t0.Add(-time.Hour), t0.Add(time.Hour)

	names, err := q.Label(ctx, &logproto.LabelRequest{Start: &start, End: &end})
	require.NoError(t, err)
	require.Equal(t, []string{"app", "env"}, names.Values)

	vals, err := q.Label(ctx, &logproto.LabelRequest{Name: "app", Values: true, Start: &start, End: &end, Query: `{env="prod"}`})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, vals.Values)

	ser, err := q.Series(ctx, &logproto.SeriesRequest{Start: start, End: end, Groups: []string{`{app=~"a|b"}`}})
	require.NoError(t, err)
	require.Len(t, ser.Series, 2)
	require.Equal(t, "app", ser.Series[0].Labels[0].Key)
	require.Equal(t, "a", ser.Series[0].Labels[0].Value)
}
