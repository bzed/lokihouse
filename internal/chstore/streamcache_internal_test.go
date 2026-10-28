package chstore

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestStreamSetKey(t *testing.T) {
	a := labels.MustNewMatcher(labels.MatchEqual, "app", "a")
	b := labels.MustNewMatcher(labels.MatchRegexp, "env", "p.*")
	d := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)

	base := streamSetKey("t", []*labels.Matcher{a, b}, d.Add(time.Hour), d.Add(2*time.Hour))
	require.Equal(t, base, streamSetKey("t", []*labels.Matcher{b, a}, d.Add(3*time.Hour), d.Add(5*time.Hour)))
	require.NotEqual(t, base, streamSetKey("u", []*labels.Matcher{a, b}, d.Add(time.Hour), d.Add(2*time.Hour)))
	require.NotEqual(t, base, streamSetKey("t", []*labels.Matcher{a}, d.Add(time.Hour), d.Add(2*time.Hour)))
	require.NotEqual(t, base, streamSetKey("t", []*labels.Matcher{a, b}, d.Add(time.Hour), d.Add(25*time.Hour)))
}

func TestStreamSetCache_PutGet(t *testing.T) {
	c := NewStreamSetCache()
	in := []Stream{{ID: 1, Labels: labels.FromStrings("app", "a")}, {ID: 2, Labels: labels.FromStrings("app", "b")}}
	c.put("k", in)
	got, ok := c.get("k")
	require.True(t, ok)
	require.Equal(t, in, got)
	_, ok = c.get("other")
	require.False(t, ok)
}

func TestStreamSetCache_PutRemoves(t *testing.T) {
	for name, streams := range map[string][]Stream{
		"empty":    nil,
		"over cap": make([]Stream, maxCachedStreams+1),
	} {
		t.Run(name, func(t *testing.T) {
			c := NewStreamSetCache()
			c.put("k", []Stream{{ID: 1}})
			_, ok := c.get("k")
			require.True(t, ok)
			c.put("k", streams)
			_, ok = c.get("k")
			require.False(t, ok)
		})
	}
}

func TestStreamSetCache_Nil(t *testing.T) {
	var c *StreamSetCache
	c.put("k", []Stream{{ID: 1}})
	_, ok := c.get("k")
	require.False(t, ok)
}
