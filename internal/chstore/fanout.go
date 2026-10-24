package chstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/semaphore"
)

// defaultFanoutMax is the default of Querier.FanoutMax: the maximum number of
// streams a sample query reads in parallel, one goroutine per stream.
const defaultFanoutMax = 16

// rowBatch is a run of rows of one stream in (ts, entry_hash) order, or the
// terminal error of the stream.
type rowBatch struct {
	rows []row
	err  error
}

// streamHead is one stream of the fan-out: the channel its goroutine sends
// batches on, and the consumer's position in the batch it holds.
type streamHead struct {
	id   uint64
	ch   chan rowBatch
	rows []row // batch being consumed
	pos  int
	done bool // the stream ended
}

// streamReader is a rowSource that reads every resolved stream on its own
// goroutine — one query per stream and window, in the part's physical order,
// so ClickHouse reads each stream without sorting — and merges the streams in
// the global (ts, stream_id, entry_hash) order the sample pipeline needs.
//
// The windows are contiguous, so a stream's rows arrive in timestamp order
// whatever window it is reading, and a linear scan over the heads finds the
// smallest row: the merged stream is exactly the ORDER BY ts, stream_id,
// entry_hash of the single-query read.
//
// The goroutines start with the reader and are owned by it: close cancels the
// read and waits for them.
type streamReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	heads  []*streamHead
	wg     sync.WaitGroup
	opened atomic.Int64 // windows opened across the streams, for tests

	// reserve and weight are the connection reservation the fan-out holds
	// while it reads; close returns it.
	reserve     *semaphore.Weighted
	weight      int64
	releaseOnce sync.Once

	curRow row
	err    error
}

// newStreamReader starts one goroutine per stream reading rq over ws. rq must
// be a forward sample query without FINAL: the part-order read returns each
// stream's duplicates adjacent, and the row reader's dedup removes them (see
// Querier.SelectSamples). reserve is the connection reservation for the
// streams (one per stream), released by close.
func newStreamReader(ctx context.Context, conn driver.Conn, rq rowQuery, ids []uint64, ws []window, reserve *semaphore.Weighted, weight int64) *streamReader {
	ctx = perStreamContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	sr := &streamReader{ctx: ctx, cancel: cancel, reserve: reserve, weight: weight}
	for _, id := range ids {
		h := &streamHead{id: id, ch: make(chan rowBatch, 1)}
		sr.heads = append(sr.heads, h)
		sr.wg.Add(1)
		go sr.read(ctx, conn, rq, h, ws)
	}
	return sr
}

// read streams the stream's rows window by window into batches. On exit it
// closes the head's channel: a clean end, an error batch, or a cancellation
// that stopped the sends.
func (sr *streamReader) read(ctx context.Context, conn driver.Conn, rq rowQuery, h *streamHead, ws []window) {
	defer sr.wg.Done()
	defer close(h.ch)
	crq := rq
	crq.single = true
	crq.streamID = &h.id
	wr := &windowReader{ws: ws, open: func(w window) (rowSource, error) {
		sr.opened.Add(1)
		sql, args := crq.sql(w)
		return openCursor(ctx, conn, sql, args...)
	}}
	defer func() { _ = wr.close() }() // the read is over; a close error is not actionable
	batch := make([]row, 0, sampleBatchRows)
	for wr.next() {
		batch = append(batch, wr.cur())
		if len(batch) < sampleBatchRows {
			continue
		}
		if !sr.send(ctx, h, rowBatch{rows: batch}) {
			return
		}
		batch = make([]row, 0, sampleBatchRows)
	}
	if err := wr.errOf(); err != nil {
		// On cancellation the send gives up and the error is dropped; the
		// consumer records the cancellation when the channel closes.
		sr.send(ctx, h, rowBatch{err: err})
		return
	}
	if len(batch) > 0 {
		sr.send(ctx, h, rowBatch{rows: batch})
	}
}

// send hands b to the consumer, giving up when the read context ends.
func (sr *streamReader) send(ctx context.Context, h *streamHead, b rowBatch) bool {
	select {
	case h.ch <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

// rowKeyLess orders rows by (ts, stream_id, entry_hash): the global sample order.
func rowKeyLess(a, b row) bool {
	if !a.ts.Equal(b.ts) {
		return a.ts.Before(b.ts)
	}
	if a.streamID != b.streamID {
		return a.streamID < b.streamID
	}
	return a.entryHash < b.entryHash
}

// fill pulls the head's next batch. It reports false when the stream ended,
// recording a terminal error or the cancellation that cut the stream short.
func (sr *streamReader) fill(h *streamHead) bool {
	select {
	case b, ok := <-h.ch:
		if !ok {
			h.done = true
			if err := sr.ctx.Err(); err != nil && sr.err == nil {
				sr.err = err // the stream was cut short by the cancellation
			}
			return false
		}
		if b.err != nil {
			h.done = true
			if sr.err == nil {
				sr.err = b.err
			}
			return false
		}
		h.rows, h.pos = b.rows, 0
		return true
	case <-sr.ctx.Done():
		h.done = true
		if sr.err == nil {
			sr.err = sr.ctx.Err()
		}
		return false
	}
}

// head returns the head's next row, pulling batches as needed.
func (sr *streamReader) head(h *streamHead) (row, bool) {
	for h.pos >= len(h.rows) {
		if h.done || !sr.fill(h) {
			return row{}, false
		}
	}
	return h.rows[h.pos], true
}

func (sr *streamReader) next() bool {
	if sr.err != nil {
		return false
	}
	var best *streamHead
	var bestRow row
	for _, h := range sr.heads {
		r, ok := sr.head(h)
		if !ok {
			if sr.err != nil {
				return false
			}
			continue
		}
		if best == nil || rowKeyLess(r, bestRow) {
			best, bestRow = h, r
		}
	}
	if best == nil {
		return false
	}
	sr.curRow = bestRow
	best.pos++
	return true
}

func (sr *streamReader) cur() row     { return sr.curRow }
func (sr *streamReader) errOf() error { return sr.err }

// openedWindows reports how many windows the streams opened, for tests.
func (sr *streamReader) openedWindows() int { return int(sr.opened.Load()) }

// close cancels the read, waits for the stream goroutines and returns the
// connection reservation. It is safe to call more than once.
func (sr *streamReader) close() error {
	sr.cancel()
	sr.wg.Wait()
	sr.releaseOnce.Do(func() {
		if sr.reserve != nil {
			sr.reserve.Release(sr.weight)
		}
	})
	return nil
}
