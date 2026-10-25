package chstore

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	switch it := it.(type) {
	case *sampleIter: // the global read: one windowReader over all streams
		return it.rr.(*windowReader).opened
	case *sliceSampleIter: // the drained fan-out: one window per stream
		return it.opened
	default:
		panic(fmt.Sprintf("unexpected sample iterator %T", it))
	}
}

// AcquireFanoutBudget takes n units of q's fan-out connection budget, all or
// nothing; ReleaseFanoutBudget returns them. Tests hold the whole budget to
// make a query fall back to the global read deterministically.
func AcquireFanoutBudget(q *Querier, n int64) bool { return q.fanoutSemaphore().TryAcquire(n) }
func ReleaseFanoutBudget(q *Querier, n int64)      { q.fanoutSemaphore().Release(n) }

// FanoutFallbackCount reports the current value of the fan-out fallback
// counter (lokihouse_chstore_fanout_fallbacks_total) for reason ("budget" or
// "cap"), for tests.
func FanoutFallbackCount(reason string) float64 {
	return testutil.ToFloat64(fanoutFallbacks.WithLabelValues(reason))
}

// OpenedLogWindows reports how many read windows a log iterator opened.
func OpenedLogWindows(it iter.EntryIterator) int { return it.(*entryIter).rr.(*windowReader).opened }
