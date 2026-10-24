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
