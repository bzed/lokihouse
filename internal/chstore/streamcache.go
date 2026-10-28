package chstore

import (
	"slices"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// streamSetCacheEntries bounds the number of cached stream sets.
	streamSetCacheEntries = 256
	// maxCachedStreams is the largest set that is cached; with
	// streamSetCacheEntries this bounds the cache at about 256 000 streams.
	maxCachedStreams = 1000
)

// StreamSetCache remembers the streams a selector resolved to, so a repeated
// log query can start reading before its stream lookup returns. It is safe for
// concurrent use (the underlying LRU is thread-safe). A nil *StreamSetCache is
// a disabled cache.
type StreamSetCache struct {
	lru *lru.Cache[string, []Stream]
}

// NewStreamSetCache returns an empty cache.
func NewStreamSetCache() *StreamSetCache {
	c, err := lru.New[string, []Stream](streamSetCacheEntries)
	if err != nil {
		panic(err) // only for a size <= 0: a programmer error at init time
	}
	return &StreamSetCache{lru: c}
}

// streamSetKey identifies a stream lookup: the tenant, the matchers (order
// independent) and the UTC days of [from, through], the granularity
// ResolveStreams filters on. A bad key only costs hit rate: the fresh lookup
// validates every hit.
func streamSetKey(tenant string, ms []*labels.Matcher, from, through time.Time) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	slices.Sort(parts)
	const day = "2006-01-02"
	return tenant + "\x00" + strings.Join(parts, "\x00") + "\x00" + from.UTC().Format(day) + "\x00" + through.UTC().Format(day)
}

func (c *StreamSetCache) get(key string) ([]Stream, bool) {
	if c == nil {
		return nil, false
	}
	return c.lru.Get(key)
}

// put stores streams under key; an empty or oversized set removes the key.
func (c *StreamSetCache) put(key string, streams []Stream) {
	if c == nil {
		return
	}
	if len(streams) == 0 || len(streams) > maxCachedStreams {
		c.lru.Remove(key)
		return
	}
	c.lru.Add(key, streams)
}
