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
	switch rr := it.(*sampleIter).rr.(type) {
	case *windowReader:
		return rr.opened
	case *streamReader:
		return rr.openedWindows()
	default:
		panic(fmt.Sprintf("unexpected sample row source %T", rr))
	}
}

// OpenedLogWindows reports how many read windows a log iterator opened.
func OpenedLogWindows(it iter.EntryIterator) int { return it.(*entryIter).rr.(*windowReader).opened }
