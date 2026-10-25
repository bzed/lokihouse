package chstore

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	querierdeletion "github.com/grafana/loki/v3/pkg/querier/deletion"
	utildeletion "github.com/grafana/loki/v3/pkg/util/deletion"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/semaphore"
)

// Querier executes LogQL queries against ClickHouse. It implements logql.Querier
// for both log and sample queries.
type Querier struct {
	Conn driver.Conn
	// Deletes, when set, is queried for pending delete requests and the
	// matching lines are hidden from log and sample results, as Loki's querier
	// does. Nil = no deletes.
	Deletes querierdeletion.DeleteGetter
	// WindowRows is the target number of rows per read window; 0 means
	// defaultWindowRows.
	WindowRows uint64
	// Workers is the number of sample-extraction goroutines per metric query;
	// 0 means runtime.GOMAXPROCS(0) read at query time. The effective count is
	// capped at the number of matching streams.
	Workers int
	// FanoutMax is the maximum number of streams a sample query reads in
	// parallel, one goroutine and one ClickHouse query per stream; 0 means
	// defaultFanoutMax (16). A query over more streams reads all of them in
	// one query.
	FanoutMax int
	// FanoutConns bounds the ClickHouse connections the sample fan-out holds
	// at once, across concurrent queries: a fanned-out query reserves one
	// connection per stream, all or nothing, and reads all streams in one
	// query when the reservation fails. It must not exceed the connection
	// pool size (clickhouse.max_open_conns); 0 means FanoutMax. Production
	// wires half the pool, so the log path, tail, ruler and stats keep
	// headroom.
	FanoutConns int
	// FanoutMaxSamples caps the samples a fanned-out read buffers before it
	// serves them: 1<<20 samples at 48 bytes each (sample, labels, stream)
	// is 48 MiB, the same order as the driver blocks a streaming read
	// buffers, and at the cap the per-stream slices and the merged slice
	// coexist, so up to about 100 MiB. A query that would produce more reads
	// all streams in one query instead. 0 means 1<<20.
	FanoutMaxSamples int64
	// Logger, when set, receives one line per sample query that reads all
	// streams in one query instead of fanning out (the connection
	// reservation failed or the drained result exceeded the sample cap);
	// nil disables the log line. The fallbacks are counted in
	// lokihouse_chstore_fanout_fallbacks_total either way. Pass a
	// concurrency-safe logger (dskit's is).
	Logger kitlog.Logger

	fanoutOnce sync.Once
	fanoutSem  *semaphore.Weighted
}

// fanoutLimit is the maximum number of streams a sample query fans out.
func (q *Querier) fanoutLimit() int {
	if q.FanoutMax <= 0 {
		return defaultFanoutMax
	}
	return q.FanoutMax
}

// fanoutMaxSamples is the sample cap of a fanned-out read.
func (q *Querier) fanoutMaxSamples() int64 {
	if q.FanoutMaxSamples > 0 {
		return q.FanoutMaxSamples
	}
	return 1 << 20
}

// fanoutSemaphore bounds the connections concurrent fan-outs hold. It is
// built once; a zero or negative FanoutConns means the default budget of
// fanoutLimit connections.
func (q *Querier) fanoutSemaphore() *semaphore.Weighted {
	q.fanoutOnce.Do(func() {
		n := q.FanoutConns
		if n <= 0 {
			n = q.fanoutLimit()
		}
		q.fanoutSem = semaphore.NewWeighted(int64(n))
	})
	return q.fanoutSem
}

// noteFanoutFallback records that a sample query reads all streams in one
// query instead of fanning out: counted in fanoutFallbacks and, when the
// querier has a logger, logged. A rising count is the signal that the
// connection budget (or the pool behind it) is the limit.
func (q *Querier) noteFanoutFallback(ctx context.Context, reason string, streams int) {
	fanoutFallbacks.WithLabelValues(reason).Inc()
	if q.Logger == nil {
		return
	}
	tid, err := tenant.TenantID(ctx)
	if err != nil {
		tid = "" // no tenant in the context (unit tests); the line stays useful
	}
	_ = level.Info(q.Logger).Log("msg", "sample query reads all streams in one query instead of fanning out",
		"reason", reason, "streams", streams, "tenant", tid) // a logging error is not actionable
}

var _ logql.Querier = (*Querier)(nil)

// rowSource is a row source shared by the single sample cursor and the
// keyset-chunked log reader, so entryIter can read from either.
type rowSource interface {
	next() bool
	cur() row
	errOf() error
	close() error
}

// resolved is the outcome of resolving a selector: the streams, the context
// carrying their IDs as an external table, and the row query to render per window.
type resolved struct {
	rq    rowQuery
	byID  map[uint64]Stream
	ids   []uint64
	from  time.Time
	until time.Time
}

// openRows resolves streams and builds the row query for the selector.
func (q *Querier) openRows(ctx context.Context, sel syntax.LogSelectorExpr, req interface {
	GetStart() time.Time
	GetEnd() time.Time
}, dir logproto.Direction) (resolved, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return resolved{}, err
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, sel.Matchers(), req.GetStart(), req.GetEnd())
	if err != nil || len(streams) == 0 {
		return resolved{}, err
	}
	ids := sortedIDs(streams)
	byID := make(map[uint64]Stream, len(streams))
	for _, s := range streams {
		byID[s.ID] = s
	}
	return resolved{
		byID: byID, ids: ids, from: req.GetStart(), until: req.GetEnd(),
		rq: rowQuery{tenant: tenantID, filter: and([]Fragment{LeadingLineFilterSQL(sel), MetadataFilterSQL(sel, streams), ParsedFilterSQL(sel, streams)}), dir: dir},
	}, nil
}

// readWindows renders the read windows of a resolved global read. Ranges of
// at most statsFreeRange read in one window, without the hourly-stats round
// trip.
func (q *Querier) readWindows(ctx context.Context, r resolved) ([]window, error) {
	return q.windows(ctx, r.rq.tenant, r.ids, r.from, r.until, r.rq.dir, statsFreeRange)
}

// SelectLogs executes a log query and returns an iterator over the matching entries.
func (q *Querier) SelectLogs(ctx context.Context, p logql.SelectLogParams) (iter.EntryIterator, error) {
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	pipeline, err := sel.Pipeline()
	if err != nil {
		return nil, err
	}
	// Fetch the user's pending deletes (Loki's tail querier already puts them
	// on the request, so only fetch when the field is empty) and filter lines
	// matching them out, exactly as Loki's querier does.
	if q.Deletes != nil && len(p.Deletes) == 0 {
		if p.Deletes, err = querierdeletion.DeletesForUserQuery(ctx, p.Start, p.End, q.Deletes); err != nil {
			return nil, err
		}
	}
	if pipeline, err = utildeletion.SetupPipeline(p, pipeline); err != nil {
		return nil, err
	}
	r, err := q.openRows(ctx, sel, p.QueryRequest, p.Direction)
	if err != nil || len(r.byID) == 0 {
		return iter.NoopEntryIterator, err
	}
	// One stream: read its rows in the part's physical order (rowQuery.single),
	// restricted to the stream by id so the shard analysis can route the query
	// to the owning shard (perStreamContext).
	r.rq.single = len(r.ids) == 1
	if r.rq.single {
		r.rq.streamID = &r.ids[0]
		ctx = perStreamContext(ctx)
	}
	ws, err := q.readWindows(ctx, r)
	if err != nil {
		return iter.NoopEntryIterator, err
	}
	// cancel the readers with the request context so a cancelled query stops the read
	sctx := ctx
	if !r.rq.single {
		// A multi-stream read restricts the query with the external id set.
		sctx, err = withStreamIDs(ctx, r.ids)
		if err != nil {
			return iter.NoopEntryIterator, err
		}
	}
	rctx, cancel := context.WithCancel(sctx)
	wr := &windowReader{ws: ws, cancel: cancel, open: func(w window) (rowSource, error) {
		return &keysetReader{ctx: rctx, conn: q.Conn, rq: r.rq, w: w, chunk: chunkSize(p.Limit)}, nil
	}}
	return &entryIter{rr: wr, byID: r.byID, pipeline: pipeline, sp: map[uint64]log.StreamPipeline{}, stats: stats.FromContext(ctx)}, nil
}

type entryIter struct {
	rr       rowSource
	byID     map[uint64]Stream
	pipeline log.Pipeline
	sp       map[uint64]log.StreamPipeline
	stats    *stats.Context
	cur      logproto.Entry
	lbls     string
	hash     uint64
}

func (it *entryIter) Next() bool {
	for it.rr.next() {
		r := it.rr.cur()
		sp, ok := it.sp[r.streamID]
		if !ok {
			sp = it.pipeline.ForStream(it.byID[r.streamID].Labels)
			it.sp[r.streamID] = sp
		}
		line, res, matches := sp.ProcessString(r.ts.UnixNano(), r.line, labels.FromMap(r.md))
		if !matches {
			continue
		}
		it.stats.AddPostFilterLines(1)
		it.cur = logproto.Entry{
			Timestamp:          r.ts,
			Line:               line,
			StructuredMetadata: mdAdapters(res.StructuredMetadata()),
			Parsed:             mdAdapters(res.Parsed()),
		}
		it.lbls, it.hash = res.String(), r.streamID
		return true
	}
	return false
}

func (it *entryIter) At() logproto.Entry { return it.cur }
func (it *entryIter) Labels() string     { return it.lbls }
func (it *entryIter) StreamHash() uint64 { return it.hash }
func (it *entryIter) Err() error         { return it.rr.errOf() }
func (it *entryIter) Close() error       { return it.rr.close() }

// SelectSamples executes a sample (metrics) query and returns an iterator over the
// matching samples.
func (q *Querier) SelectSamples(ctx context.Context, p logql.SelectSampleParams) (iter.SampleIterator, error) {
	expr, err := p.Expr()
	if err != nil {
		return nil, err
	}
	extractor, err := expr.Extractor()
	if err != nil {
		return nil, err
	}
	if extractor == nil {
		return iter.NoopSampleIterator, nil
	}
	// Fetch the user's pending deletes (Loki's tail querier already puts them
	// on the request, so only fetch when the field is empty) and filter samples
	// matching them out, exactly as Loki's querier does.
	if q.Deletes != nil && len(p.Deletes) == 0 {
		if p.Deletes, err = querierdeletion.DeletesForUserQuery(ctx, p.Start, p.End, q.Deletes); err != nil {
			return nil, err
		}
	}
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	r, err := q.openRows(ctx, sel, p.SampleQueryRequest, logproto.FORWARD)
	if err != nil || len(r.byID) == 0 {
		return iter.NoopSampleIterator, err
	}
	// Loki's extractors are not safe for concurrent use: one per extraction
	// goroutine — the fan-out's streams, the global read's workers.
	makeExtractors := func(n int) ([]log.SampleExtractor, error) {
		exs := make([]log.SampleExtractor, n)
		for i := range exs {
			ex, err := expr.Extractor()
			if err != nil {
				return nil, err
			}
			if exs[i], err = utildeletion.SetupExtractor(p, ex); err != nil {
				return nil, err
			}
		}
		return exs, nil
	}
	// A fanned-out read (one goroutine and one ClickHouse query per stream,
	// in the part's physical order) needs no window to bound a sort, so it
	// reads the whole range in one window and never consults the hourly
	// stats; the global read keeps the statsFreeRange threshold, where
	// windows bound the sort.
	fanout := len(r.ids) <= q.fanoutLimit()
	var reserve *semaphore.Weighted
	if fanout {
		// Reserve one connection per stream, all or nothing: the drain below
		// holds them for its duration, and the reservation keeps concurrent
		// fan-outs within the budget. When it cannot be taken in full, read
		// all streams in one query instead.
		reserve = q.fanoutSemaphore()
		if !reserve.TryAcquire(int64(len(r.ids))) {
			reserve = nil
			fanout = false
			q.noteFanoutFallback(ctx, "budget", len(r.ids))
		}
	}
	rq := r.rq
	if fanout {
		// Per-stream reads follow the part order, so duplicates of one stream
		// arrive adjacent and the row reader's dedup (Loki merge semantics)
		// removes them. FINAL is not needed, and it would cost the text index:
		// under FINAL ClickHouse keeps the skip index exact only by reading
		// several times more granules of unmerged parts.
		//
		// The fan-out is drained before the query returns (drainFanout): the
		// leg holds its connections only while its streams drain — they wait
		// on nothing — so a request whose engine is blocked on a sibling leg
		// (a binary operation, a multi-tenant merge) can never hold
		// connections that sibling needs, and neither can a stream whose
		// query is still waiting for a connection. The engine materialises a
		// range query's full result before it responds, so draining first
		// costs no end-to-end latency. The drained result is capped; a query
		// over the cap reads all streams in one query below.
		ws := []window{{r.from.UnixNano(), r.until.UnixNano()}}
		exs, err := makeExtractors(len(r.ids))
		if err != nil {
			reserve.Release(int64(len(r.ids)))
			return nil, err
		}
		samples, opened, err := q.drainFanout(ctx, rq, r.ids, ws, reserve, r.byID, exs, stats.FromContext(ctx))
		if err == nil {
			return newSliceSampleIter(samples, opened), nil
		}
		if !errors.Is(err, errFanoutTooLarge) {
			return nil, err
		}
		// Over the sample cap: fall through to the global read below.
		q.noteFanoutFallback(ctx, "cap", len(r.ids))
	}
	// The global read: one query over all streams, in the global order, with
	// FINAL (above the fan-out limit, or when the reservation or the sample
	// cap sent the query here). Metric queries read everything anyway: one
	// unchunked cursor per window.
	ws, err := q.readWindows(ctx, r)
	if err != nil {
		return iter.NoopSampleIterator, err
	}
	workers := q.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	workers = max(1, min(workers, len(r.byID)))
	exs, err := makeExtractors(workers)
	if err != nil {
		return nil, err
	}
	rq.final = true
	sctx, err := withStreamIDs(ctx, r.ids)
	if err != nil {
		return iter.NoopSampleIterator, err
	}
	// Cancel the read with the iterator: Close stops the producer and the ClickHouse query.
	rctx, cancel := context.WithCancel(sctx)
	// The producer closes wr when it ends; only Close cancels rctx, as workers may still be extracting.
	wr := &windowReader{ws: ws, open: func(w window) (rowSource, error) {
		sql, args := rq.sql(w)
		return openCursor(rctx, q.Conn, sql, args...)
	}}
	return newSampleIter(rctx, cancel, wr, r.byID, exs, stats.FromContext(ctx)), nil
}
