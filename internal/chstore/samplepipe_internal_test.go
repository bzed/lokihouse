package chstore

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

type fakeRows struct{ rows []row }

func (f *fakeRows) next() bool {
	if len(f.rows) == 0 {
		return false
	}
	f.rows = f.rows[1:]
	return true
}
func (f *fakeRows) cur() row     { return row{streamID: 1, ts: time.Unix(1, 0), line: "x"} }
func (f *fakeRows) errOf() error { return nil }
func (f *fakeRows) close() error { return nil }

// A context cancelled while batches are queued must never end in a short
// result without an error: either all samples arrive or Err reports it.
func TestSampleIter_CancelledContextNeverShortWithoutError(t *testing.T) {
	expr, err := syntax.ParseSampleExpr(`count_over_time({app="a"}[1m])`)
	require.NoError(t, err)
	for range 300 {
		ex, err := expr.Extractor()
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		it := newSampleIter(ctx, cancel, &fakeRows{rows: make([]row, 3)}, map[uint64]Stream{1: {ID: 1}}, []log.SampleExtractor{ex}, nil)
		it.start()
		cancel() // the producer may already be done; the workers have not necessarily run
		n := 0
		for it.Next() {
			n++
		}
		if n < 3 {
			require.ErrorIs(t, it.Err(), context.Canceled, "short result (%d of 3) without an error", n)
		}
		require.NoError(t, it.Close())
	}
}

type hashRows struct {
	rows   []row
	curRow row
}

func (h *hashRows) next() bool {
	if len(h.rows) == 0 {
		return false
	}
	h.curRow = h.rows[0]
	h.rows = h.rows[1:]
	return true
}
func (h *hashRows) cur() row     { return h.curRow }
func (h *hashRows) errOf() error { return nil }
func (h *hashRows) close() error { return nil }

// Sample.Hash carries the row's entry hash, the table's dedup identity
// (cityHash64 of the line and the structured metadata): it is already decoded
// with the row, and it identifies exact repeats, which is what a merge dedup
// needs.
func TestSampleIter_HashIsEntryHash(t *testing.T) {
	expr, err := syntax.ParseSampleExpr(`count_over_time({app="a"}[1m])`)
	require.NoError(t, err)
	ex, err := expr.Extractor()
	require.NoError(t, err)
	rows := &hashRows{rows: []row{
		{streamID: 1, ts: time.Unix(1, 0), line: "x", entryHash: 42},
		{streamID: 1, ts: time.Unix(2, 0), line: "x", entryHash: 43},
	}}
	it := newSampleIter(context.Background(), func() {}, rows, map[uint64]Stream{1: {ID: 1}}, []log.SampleExtractor{ex}, nil)
	require.True(t, it.Next())
	require.Equal(t, uint64(42), it.At().Hash)
	require.True(t, it.Next())
	require.Equal(t, uint64(43), it.At().Hash)
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
}
