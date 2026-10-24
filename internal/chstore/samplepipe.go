package chstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// sampleBatchRows is the number of rows the producer hands over at once.
	sampleBatchRows = 1024
	// sampleBatchesInFlight is the capacity of the channels between the
	// producer, the workers and the consumer; it bounds the rows in memory to
	// about sampleBatchesInFlight × sampleBatchRows.
	sampleBatchesInFlight = 4
)

// sampleOut is the extraction result of one row of a batch.
type sampleOut struct {
	ok    bool
	value float64
	lbls  string
	hash  uint64
}

// sampleBatch is a run of consecutive rows. Worker i fills out[j] for the rows
// j with worker[j] == i; done counts the workers that still have to finish.
type sampleBatch struct {
	rows   []row
	worker []int
	out    []sampleOut
	done   sync.WaitGroup
}

// sampleIter turns the ts-ordered row cursor into samples. A producer goroutine
// scans rows into batches, one worker per extractor extracts the samples of its
// streams (a stream always goes to the same worker, so each per-stream
// extractor sees its lines in order), and Next yields the results in row order.
// Loki's range-vector iterators only need samples in timestamp order; they group by Labels().
//
// Goroutines start on the first Next and are owned by the iterator: Close
// cancels the read context and waits for all of them.
type sampleIter struct {
	rr         rowSource // read and closed by the producer once started
	byID       map[uint64]Stream
	extractors []log.SampleExtractor
	stats      *stats.Context
	ctx        context.Context
	cancel     context.CancelFunc

	started   bool // consumer-owned
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
	chans     []chan *sampleBatch // one per worker, closed by the producer
	ordered   chan *sampleBatch   // batches in row order, closed by the producer

	mu  sync.Mutex // guards err
	err error

	batch *sampleBatch // consumer-owned
	pos   int
	cur   logproto.Sample
	lbls  string
	hash  uint64
}

// newSampleIter returns an iterator over rr with one worker per extractor. ctx
// is the read context that rr's queries run under and cancel cancels it.
func newSampleIter(ctx context.Context, cancel context.CancelFunc, rr rowSource, byID map[uint64]Stream, extractors []log.SampleExtractor, st *stats.Context) *sampleIter {
	return &sampleIter{rr: rr, byID: byID, extractors: extractors, stats: st, ctx: ctx, cancel: cancel}
}

func (it *sampleIter) start() {
	it.started = true
	it.ordered = make(chan *sampleBatch, sampleBatchesInFlight)
	it.chans = make([]chan *sampleBatch, len(it.extractors))
	it.wg.Add(1 + len(it.extractors))
	for i, ex := range it.extractors {
		it.chans[i] = make(chan *sampleBatch, sampleBatchesInFlight)
		go it.work(i, ex)
	}
	go it.produce()
}

func (it *sampleIter) setErr(err error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.err == nil {
		it.err = err
	}
}

// produce scans rows into batches and routes each to the workers owning its
// streams. On exit (end, error or cancel) it stashes the error and closes every
// channel, which stops the workers and ends the consumer.
func (it *sampleIter) produce() {
	defer it.wg.Done()
	defer func() {
		_ = it.rr.close() // the read is over; a close error is not actionable
		for _, ch := range it.chans {
			close(ch)
		}
		close(it.ordered)
	}()
	assigned := map[uint64]int{}
	used := make([]bool, len(it.chans))
	for {
		b := &sampleBatch{rows: make([]row, 0, sampleBatchRows), worker: make([]int, 0, sampleBatchRows)}
		for len(b.rows) < sampleBatchRows && it.rr.next() {
			r := it.rr.cur()
			w, ok := assigned[r.streamID]
			if !ok {
				w = len(assigned) % len(it.chans)
				assigned[r.streamID] = w
			}
			b.rows = append(b.rows, r)
			b.worker = append(b.worker, w)
		}
		if len(b.rows) > 0 && !it.dispatch(b, used) {
			return
		}
		if len(b.rows) < sampleBatchRows {
			it.setErr(it.rr.errOf())
			return
		}
	}
}

// dispatch sends b to the workers that own rows in it, then to the consumer.
// It reports false if the read context ended first.
func (it *sampleIter) dispatch(b *sampleBatch, used []bool) bool {
	clear(used)
	n := 0
	for _, w := range b.worker {
		if !used[w] {
			used[w] = true
			n++
		}
	}
	b.out = make([]sampleOut, len(b.rows))
	b.done.Add(n) // before any send, so the consumer's Wait never races Add
	for w, u := range used {
		if !u {
			continue
		}
		select {
		case it.chans[w] <- b:
		case <-it.ctx.Done():
			it.setErr(it.ctx.Err())
			return false
		}
	}
	select {
	case it.ordered <- b:
		return true
	case <-it.ctx.Done():
		it.setErr(it.ctx.Err())
		return false
	}
}

// work extracts the samples of the batches' rows that belong to worker w. It
// calls Done for every batch it receives, also after cancellation, so a
// consumer waiting on a batch is never stranded.
func (it *sampleIter) work(w int, ex log.SampleExtractor) {
	defer it.wg.Done()
	streams := map[uint64]log.StreamSampleExtractor{}
	var digest xxhash.Digest
	for b := range it.chans[w] {
		if it.ctx.Err() == nil {
			var kept int64
			for i, r := range b.rows {
				if b.worker[i] != w {
					continue
				}
				se, ok := streams[r.streamID]
				if !ok {
					se = ex.ForStream(it.byID[r.streamID].Labels)
					streams[r.streamID] = se
				}
				s, ok := se.ProcessString(r.ts.UnixNano(), r.line, labels.FromMap(r.md))
				if !ok {
					continue
				}
				kept++
				lbls := s.Labels.String()
				digest.Reset()
				_, _ = digest.WriteString(lbls) // xxhash writes never fail
				_, _ = digest.WriteString(r.line)
				b.out[i] = sampleOut{ok: true, value: s.Value, lbls: lbls, hash: digest.Sum64()}
			}
			if it.stats != nil && kept > 0 {
				it.stats.AddPostFilterLines(kept)
			}
		}
		b.done.Done()
	}
}

func (it *sampleIter) Next() bool {
	if it.closed.Load() {
		return false
	}
	if !it.started {
		it.start()
	}
	for {
		for it.batch != nil && it.pos < len(it.batch.rows) {
			i := it.pos
			it.pos++
			o := it.batch.out[i]
			if !o.ok {
				continue
			}
			it.lbls, it.hash = o.lbls, it.batch.rows[i].streamID
			it.cur = logproto.Sample{Timestamp: it.batch.rows[i].ts.UnixNano(), Value: o.value, Hash: o.hash}
			return true
		}
		b, ok := <-it.ordered
		if !ok {
			it.batch = nil
			return false
		}
		b.done.Wait()
		it.batch, it.pos = b, 0
	}
}

func (it *sampleIter) At() logproto.Sample { return it.cur }
func (it *sampleIter) Labels() string      { return it.lbls }
func (it *sampleIter) StreamHash() uint64  { return it.hash }

// Err returns the read error, but not the cancellation that Close itself caused.
func (it *sampleIter) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed.Load() && it.err != nil && isCanceled(it.err) {
		return nil
	}
	return it.err
}

func (it *sampleIter) Close() error {
	it.closeOnce.Do(func() {
		it.closed.Store(true)
		it.cancel()
		if it.started {
			it.wg.Wait()
		} else {
			_ = it.rr.close() // never read: nothing to report
		}
	})
	return nil
}
