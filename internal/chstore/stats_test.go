package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestStatsFromHours(t *testing.T) {
	const s = int64(1e9)
	rows := []HourStat{
		{StreamID: 1, Entries: 100, Bytes: 1000, MinTS: 0, MaxTS: 100 * s},    // half inside [50s, 200s]
		{StreamID: 1, Entries: 10, Bytes: 10, MinTS: 150 * s, MaxTS: 160 * s}, // fully inside
		{StreamID: 2, Entries: 1, Bytes: 7, MinTS: 120 * s, MaxTS: 120 * s},   // single entry: factor 1
		{StreamID: 3, Entries: 5, Bytes: 5, MinTS: 300 * s, MaxTS: 310 * s},   // outside
		{StreamID: 4, Entries: 1, Bytes: 1, MinTS: 50 * s, MaxTS: 50 * s},     // from == min == max: Loki skips (from < max is false)
	}
	got := statsFromHours(rows, 50*s, 200*s)
	require.Equal(t, uint64(2), got.Streams)
	require.Equal(t, uint64(3), got.Chunks)
	require.Equal(t, uint64(50+10+1), got.Entries)
	require.Equal(t, uint64(500+10+7), got.Bytes)

	empty := statsFromHours(nil, 0, s)
	require.Zero(t, empty.Streams+empty.Chunks+empty.Entries+empty.Bytes)
}

func TestVolumeFromHours(t *testing.T) {
	const s = int64(1e9)
	a := Stream{ID: 1, Labels: labels.FromStrings("app", "a", "env", "dev")}
	b := Stream{ID: 2, Labels: labels.FromStrings("app", "b", "env", "dev")}
	rows := []HourStat{
		{StreamID: 1, Bytes: 100, MinTS: 0, MaxTS: 10 * s},
		{StreamID: 2, Bytes: 40, MinTS: 0, MaxTS: 10 * s},
	}
	// aggregate by series, all labels
	got := volumeFromHours([]Stream{a, b}, rows, 0, 20*s, &logproto.VolumeRequest{Limit: 10, AggregateBy: "series"}, nil, true)
	require.Equal(t, []logproto.Volume{
		{Name: `{app="a", env="dev"}`, Volume: 100},
		{Name: `{app="b", env="dev"}`, Volume: 40},
	}, got.Volumes)
	// aggregate by labels with a target label: per label name
	got = volumeFromHours([]Stream{a, b}, rows, 0, 20*s,
		&logproto.VolumeRequest{Limit: 10, AggregateBy: "labels", TargetLabels: []string{"env"}},
		map[string]struct{}{"env": {}}, false)
	require.Equal(t, []logproto.Volume{{Name: "env", Volume: 140}}, got.Volumes)
	// limit
	got = volumeFromHours([]Stream{a, b}, rows, 0, 20*s, &logproto.VolumeRequest{Limit: 1, AggregateBy: "series"}, nil, true)
	require.Len(t, got.Volumes, 1)
}
