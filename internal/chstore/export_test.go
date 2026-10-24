package chstore

import (
	"fmt"

	"github.com/grafana/loki/v3/pkg/iter"
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

// SetMaxFanoutSamples overrides the fan-out sample cap (default 1<<20) for
// tests.
func SetMaxFanoutSamples(n int64) { maxFanoutSamples = n }

// AcquireFanoutBudget takes n units of q's fan-out connection budget, all or
// nothing; ReleaseFanoutBudget returns them. Tests hold the whole budget to
// make a query fall back to the global read deterministically.
func AcquireFanoutBudget(q *Querier, n int64) bool { return q.fanoutSemaphore().TryAcquire(n) }
func ReleaseFanoutBudget(q *Querier, n int64)      { q.fanoutSemaphore().Release(n) }

// OpenedLogWindows reports how many read windows a log iterator opened.
func OpenedLogWindows(it iter.EntryIterator) int { return it.(*entryIter).rr.(*windowReader).opened }
