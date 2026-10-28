package chstore

import (
	"time"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
)

// SetLogChunkSize overrides the minimum chunk size (default 1000) for tests.
func SetLogChunkSize(n int) { logChunkSize = n }

// CheckFreeSpace exposes checkFreeSpace to the external tests.
var CheckFreeSpace = checkFreeSpace

// DiskSpace and RenderSchemaWithSuffix expose internals to the external tests.
var (
	DiskSpace              = diskSpace
	RenderSchemaWithSuffix = renderSchema
)

// OpenedSampleWindows reports how many read windows a sample iterator opened.
func OpenedSampleWindows(it iter.SampleIterator) int {
	return it.(*sampleIter).rr.(*windowReader).opened
}

// OpenedLogWindows reports how many read windows a log iterator opened.
func OpenedLogWindows(it iter.EntryIterator) int { return it.(*entryIter).rr.(*windowReader).opened }

// StreamSpeculations reports the speculation counter for a result label.
func StreamSpeculations(result string) float64 {
	return testutil.ToFloat64(streamSpeculations.WithLabelValues(result))
}

// SeedStreamSet puts a stream set into c under the key of the given lookup.
func SeedStreamSet(c *StreamSetCache, tenant string, ms []*labels.Matcher, from, through time.Time, streams []Stream) {
	c.put(streamSetKey(tenant, ms, from, through), streams)
}
