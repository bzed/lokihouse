package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func TestSampleKeyLess(t *testing.T) {
	mk := func(stream uint64, ts int64) drainedSample {
		return drainedSample{sample: logproto.Sample{Timestamp: ts}, stream: stream}
	}
	for _, tc := range []struct {
		name          string
		a, b          drainedSample
		want, wantRev bool
	}{
		{"earlier ts", mk(2, 100), mk(1, 200), true, false},
		{"later ts", mk(1, 200), mk(2, 100), false, true},
		{"same ts, smaller stream", mk(1, 100), mk(2, 100), true, false},
		{"same ts, bigger stream", mk(2, 100), mk(1, 100), false, true},
		{"equal", mk(1, 100), mk(1, 100), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sampleKeyLess(tc.a, tc.b))
			require.Equal(t, tc.wantRev, sampleKeyLess(tc.b, tc.a))
		})
	}
}

// The merge yields the streams' samples in the exact global (ts, stream_id)
// order, across slices and empty streams, preserving one stream's order
// within a timestamp.
func TestMergeSamples_MergesInGlobalOrder(t *testing.T) {
	mk := func(stream uint64, ts int64) drainedSample {
		return drainedSample{sample: logproto.Sample{Timestamp: ts}, stream: stream}
	}
	perStream := [][]drainedSample{
		{mk(1, 100), mk(1, 100), mk(1, 300)}, // two samples at one timestamp: FIFO
		{mk(2, 100), mk(2, 200)},
		{}, // empty stream
	}
	got := mergeSamples(perStream)
	want := []drainedSample{mk(1, 100), mk(1, 100), mk(2, 100), mk(2, 200), mk(1, 300)}
	require.Equal(t, want, got)
}

// An empty fan-out merges to an empty slice.
func TestMergeSamples_Empty(t *testing.T) {
	require.Empty(t, mergeSamples(nil))
	require.Empty(t, mergeSamples([][]drainedSample{{}, {}}))
}

// The slice iterator replays the drained samples in order and reports no
// error; Close is a no-op.
func TestSliceSampleIter_ReplaysSamples(t *testing.T) {
	mk := func(stream uint64, ts int64) drainedSample {
		return drainedSample{sample: logproto.Sample{Timestamp: ts, Value: float64(ts)}, lbls: "{a=\"b\"}", stream: stream}
	}
	samples := []drainedSample{mk(1, 100), mk(2, 200)}
	it := newSliceSampleIter(samples, 2)
	require.True(t, it.Next())
	require.Equal(t, int64(100), it.At().Timestamp)
	require.Equal(t, "{a=\"b\"}", it.Labels())
	require.Equal(t, uint64(1), it.StreamHash())
	require.True(t, it.Next())
	require.Equal(t, int64(200), it.At().Timestamp)
	require.False(t, it.Next())
	require.False(t, it.Next(), "the end sticks")
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
}
