package chstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRowKeyLess(t *testing.T) {
	t1 := time.Unix(0, 100)
	t2 := time.Unix(0, 200)
	for _, tc := range []struct {
		name          string
		a, b          row
		want, wantRev bool
	}{
		{"earlier ts", row{ts: t1, streamID: 2, entryHash: 9}, row{ts: t2, streamID: 1, entryHash: 1}, true, false},
		{"later ts", row{ts: t2, streamID: 1, entryHash: 1}, row{ts: t1, streamID: 2, entryHash: 9}, false, true},
		{"same ts, smaller stream", row{ts: t1, streamID: 1, entryHash: 9}, row{ts: t1, streamID: 2, entryHash: 1}, true, false},
		{"same ts, bigger stream", row{ts: t1, streamID: 2, entryHash: 1}, row{ts: t1, streamID: 1, entryHash: 9}, false, true},
		{"same ts and stream, smaller hash", row{ts: t1, streamID: 1, entryHash: 1}, row{ts: t1, streamID: 1, entryHash: 2}, true, false},
		{"equal rows", row{ts: t1, streamID: 1, entryHash: 1}, row{ts: t1, streamID: 1, entryHash: 1}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rowKeyLess(tc.a, tc.b))
			require.Equal(t, tc.wantRev, rowKeyLess(tc.b, tc.a))
		})
	}
}

// feed sends the batches on the head's channel, then closes it, like a stream
// goroutine does.
func feed(h *streamHead, batches ...rowBatch) {
	go func() {
		for _, b := range batches {
			h.ch <- b
		}
		close(h.ch)
	}()
}

func newTestStreamReader(ctx context.Context, heads ...*streamHead) *streamReader {
	ctx, cancel := context.WithCancel(ctx)
	return &streamReader{ctx: ctx, cancel: cancel, heads: heads}
}

// The merge yields the streams' rows in the exact global (ts, stream_id,
// entry_hash) order, across batches and empty streams.
func TestStreamReader_MergesInGlobalOrder(t *testing.T) {
	ts := func(ns int64) time.Time { return time.Unix(0, ns).UTC() }
	mk := func(sid, hash, ns uint64) row { return row{streamID: sid, entryHash: hash, ts: ts(int64(ns))} }
	h1 := &streamHead{id: 1, ch: make(chan rowBatch, 4)}
	h2 := &streamHead{id: 2, ch: make(chan rowBatch, 4)}
	h3 := &streamHead{id: 3, ch: make(chan rowBatch, 4)} // empty stream
	sr := newTestStreamReader(context.Background(), h1, h2, h3)
	feed(h1, rowBatch{rows: []row{mk(1, 5, 100), mk(1, 1, 300)}})
	feed(h2, rowBatch{rows: []row{mk(2, 7, 100)}}, rowBatch{rows: []row{mk(2, 2, 200)}})
	feed(h3)

	var got []row
	for sr.next() {
		got = append(got, sr.cur())
	}
	require.NoError(t, sr.errOf())
	require.Equal(t, []row{mk(1, 5, 100), mk(2, 7, 100), mk(2, 2, 200), mk(1, 1, 300)}, got)
	require.NoError(t, sr.close())
}

// A stream's terminal error ends the read with that error.
func TestStreamReader_StreamError(t *testing.T) {
	boom := errors.New("boom")
	h1 := &streamHead{id: 1, ch: make(chan rowBatch, 4)}
	h2 := &streamHead{id: 2, ch: make(chan rowBatch, 4)}
	sr := newTestStreamReader(context.Background(), h1, h2)
	feed(h1, rowBatch{rows: []row{{streamID: 1, ts: time.Unix(0, 1)}}}, rowBatch{err: boom})
	feed(h2, rowBatch{rows: []row{{streamID: 2, ts: time.Unix(0, 2)}}})

	require.True(t, sr.next())
	require.False(t, sr.next())
	require.ErrorIs(t, sr.errOf(), boom)
	require.False(t, sr.next(), "the error sticks")
	require.NoError(t, sr.close())
}

// A cancelled read context ends the read with the context's error, also when
// the stream goroutines are cut short (their channels close without a batch).
func TestStreamReader_CancelStopsRead(t *testing.T) {
	sr := newTestStreamReader(context.Background(), &streamHead{id: 1, ch: make(chan rowBatch)})
	sr.cancel()
	require.False(t, sr.next())
	require.ErrorIs(t, sr.errOf(), context.Canceled)
	require.NoError(t, sr.close())
}

// close cancels the read and waits for a producer blocked on a full channel.
func TestStreamReader_CloseStopsProducers(t *testing.T) {
	sr := newTestStreamReader(context.Background(), &streamHead{id: 1, ch: make(chan rowBatch)})
	sr.wg.Add(1)
	go func() {
		defer sr.wg.Done()
		select {
		case sr.heads[0].ch <- rowBatch{rows: []row{{streamID: 1}}}:
		case <-sr.ctx.Done():
		}
	}()
	require.NoError(t, sr.close())
}
