package chstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/semaphore"
)

// defaultFanoutMax is the default of Querier.FanoutMax: the maximum number of
// streams a sample query reads in parallel, one goroutine per stream.
const defaultFanoutMax = 16

// errFanoutTooLarge reports a drained fan-out that exceeded
// Querier.FanoutMaxSamples; the caller falls back to the global read.
var errFanoutTooLarge = errors.New("fan-out result exceeds the sample cap")

// drainedSample is one sample of a drained fan-out, with the labels and the
// stream hash the engine reads from the iterator.
type drainedSample struct {
	sample logproto.Sample
	lbls   string
	stream uint64
}

// drainFanout reads every stream of the fan-out on its own goroutine — one
// ClickHouse query per stream and window, in the part's physical order —
// extracts each stream's samples and merges them in the global (ts,
// stream_id) order the single-query read produces. reserve is the
// connection reservation for the streams (one per stream), returned when the
// drain ends.
//
// A goroutine waits on nothing: it consumes its query as fast as the server
// sends and appends to its own slice, so it holds its connection only for the
// transfer. The leg therefore holds its connections only until its streams
// are drained — never while the engine waits on a sibling leg of the same
// request (a binary operation, a multi-tenant merge), and never while the
// merge waits for a stream whose query has not started — so neither
// concurrent multi-leg queries nor a pool smaller than a fan-out can
// deadlock: there is no circular wait. The engine materialises a range
// query's full result before it responds, so draining first costs no
// end-to-end latency.
//
// The result is capped at the Querier's FanoutMaxSamples; over the cap the
// drain gives up (errFanoutTooLarge) and the caller reads all streams in one
// query instead.
func (q *Querier) drainFanout(ctx context.Context, rq rowQuery, ids []uint64, ws []window, reserve *semaphore.Weighted, byID map[uint64]Stream, extractors []log.SampleExtractor, st *stats.Context) ([]drainedSample, int, error) {
	if reserve != nil {
		defer reserve.Release(int64(len(ids)))
	}
	maxSamples := q.fanoutMaxSamples()
	ctx = perStreamContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	perStream := make([][]drainedSample, len(ids))
	errs := make([]error, len(ids))
	var total, opened atomic.Int64
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sid := id
			crq := rq
			crq.single = true
			crq.streamID = &sid
			wr := &windowReader{ws: ws, open: func(w window) (rowSource, error) {
				opened.Add(1)
				sql, args := crq.sql(w)
				return openCursor(ctx, q.Conn, sql, args...)
			}}
			defer func() { _ = wr.close() }() // the read is over; a close error is not actionable
			se := extractors[i].ForStream(byID[id].Labels)
			var kept int64
			for wr.next() {
				r := wr.cur()
				s, ok := se.ProcessString(r.ts.UnixNano(), r.line, labels.FromMap(r.md))
				if !ok {
					continue
				}
				kept++
				// The sample hash is the row's entry hash, the table's dedup
				// identity: decoded with the row for free, and it identifies
				// exact repeats, which is what a merge dedup needs. It is not
				// Loki's definition (a hash of the extracted sample labels and
				// the line), which differs when a query drops the metadata:
				// nothing consumes the hash for merging today, but a future
				// path that merges sample iterators must know.
				perStream[i] = append(perStream[i], drainedSample{
					sample: logproto.Sample{Timestamp: r.ts.UnixNano(), Value: s.Value, Hash: r.entryHash},
					lbls:   s.Labels.String(),
					stream: id,
				})
				if total.Add(1) > maxSamples {
					errs[i] = errFanoutTooLarge
					cancel()
					return
				}
			}
			if err := wr.errOf(); err != nil {
				errs[i] = err
				cancel()
				return
			}
			if st != nil && kept > 0 {
				st.AddPostFilterLines(kept)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if errors.Is(err, errFanoutTooLarge) {
			return nil, 0, errFanoutTooLarge
		}
	}
	for _, err := range errs {
		if err != nil {
			return nil, 0, err
		}
	}
	samples := mergeSamples(perStream)
	// Intern the label strings: the distinct sets are few, and the slice
	// would otherwise hold a copy of each per sample.
	interned := make(map[string]string, len(ids))
	for i := range samples {
		if s, ok := interned[samples[i].lbls]; ok {
			samples[i].lbls = s
		} else {
			interned[samples[i].lbls] = samples[i].lbls
		}
	}
	return samples, int(opened.Load()), nil
}

// mergeSamples merges the per-stream sample slices in the global (ts,
// stream_id) order — the order of the single-query read — preserving each
// stream's order within one timestamp.
func mergeSamples(perStream [][]drainedSample) []drainedSample {
	var total int
	for _, ss := range perStream {
		total += len(ss)
	}
	out := make([]drainedSample, 0, total)
	pos := make([]int, len(perStream))
	for {
		best := -1
		var bestSample drainedSample
		for i, ss := range perStream {
			if pos[i] >= len(ss) {
				continue
			}
			if best < 0 || sampleKeyLess(ss[pos[i]], bestSample) {
				best, bestSample = i, ss[pos[i]]
			}
		}
		if best < 0 {
			return out
		}
		out = append(out, bestSample)
		pos[best]++
	}
}

// sampleKeyLess orders samples by (ts, stream_id): the global sample order.
func sampleKeyLess(a, b drainedSample) bool {
	if a.sample.Timestamp != b.sample.Timestamp {
		return a.sample.Timestamp < b.sample.Timestamp
	}
	return a.stream < b.stream
}

// sliceSampleIter serves the samples a fanned-out read drained (drainFanout).
// The read is over when the iterator is created: Next replays the slice and
// Close is a no-op. opened is the number of windows the fan-out opened, for
// tests.
type sliceSampleIter struct {
	samples []drainedSample
	opened  int
	pos     int
	cur     drainedSample
}

func newSliceSampleIter(samples []drainedSample, opened int) *sliceSampleIter {
	return &sliceSampleIter{samples: samples, opened: opened}
}

func (it *sliceSampleIter) Next() bool {
	if it.pos >= len(it.samples) {
		return false
	}
	it.cur = it.samples[it.pos]
	it.pos++
	return true
}

func (it *sliceSampleIter) At() logproto.Sample { return it.cur.sample }
func (it *sliceSampleIter) Labels() string      { return it.cur.lbls }
func (it *sliceSampleIter) StreamHash() uint64  { return it.cur.stream }
func (it *sliceSampleIter) Err() error          { return nil }
func (it *sliceSampleIter) Close() error        { return nil }
