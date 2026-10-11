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

func TestDetectedLabels_All(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a", pod="p1", env="dev", id="0f8e8c1a-5b2d-4c7e-9a3f-1d2e3f4a5b6c"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="a", pod="p2", env="dev", id="7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", pod="p1", env="prod", id="9a2b3c4d-5e6f-4789-a0b1-c2d3e4f5a6b7"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})

	q := &chstore.Querier{Conn: conn}
	res, err := q.DetectedLabels(user.InjectOrgID(context.Background(), "t1"), &logproto.DetectedLabelsRequest{
		// no query: all streams
		Start: t0, End: t0.Add(time.Minute),
	})
	require.NoError(t, err)
	got := map[string]uint64{}
	for _, dl := range res.DetectedLabels {
		got[dl.Label] = dl.Cardinality
		require.NotEmpty(t, dl.Sketch, "sketch for %s must be non-empty", dl.Label)
	}
	// id is rejected: every value is an ID (UUID).
	require.Equal(t, map[string]uint64{"app": 2, "pod": 2, "env": 2}, got)
}

func TestDetectedLabels_WithQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		logproto.Stream{Labels: `{app="a", pod="p1", env="dev", id="0f8e8c1a-5b2d-4c7e-9a3f-1d2e3f4a5b6c"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="a", pod="p2", env="dev", id="7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", pod="p1", env="prod", id="9a2b3c4d-5e6f-4789-a0b1-c2d3e4f5a6b7"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})

	q := &chstore.Querier{Conn: conn}
	res, err := q.DetectedLabels(user.InjectOrgID(context.Background(), "t1"), &logproto.DetectedLabelsRequest{
		Query: `{app="a"}`,
		Start: t0, End: t0.Add(time.Minute),
	})
	require.NoError(t, err)
	got := map[string]uint64{}
	for _, dl := range res.DetectedLabels {
		got[dl.Label] = dl.Cardinality
	}
	// id is an ID → rejected. app (value "a"), env (value "dev") and pod (static) are kept.
	// A single non-ID value is NOT rejected by shouldRejectLabel (only a single ID value is).
	require.Equal(t, map[string]uint64{"app": 1, "env": 1, "pod": 2}, got)
}
