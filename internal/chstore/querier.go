package chstore

import (
	"context"
	"runtime"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// readWindows renders the read windows of a resolved query.
func (q *Querier) readWindows(ctx context.Context, r resolved) ([]window, error) {
	return q.windows(ctx, r.rq.tenant, r.ids, r.from, r.until, r.rq.dir)
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
	ws, err := q.readWindows(ctx, r)
	if err != nil {
		return iter.NoopEntryIterator, err
	}
	// cancel the readers with the request context so a cancelled query stops the read
	sctx, err := withStreamIDs(ctx, r.ids)
	if err != nil {
		return iter.NoopEntryIterator, err
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
	// Loki's extractors are not safe for concurrent use: one per worker.
	workers := q.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	workers = max(1, min(workers, len(r.byID)))
	extractors := make([]log.SampleExtractor, workers)
	for i := range extractors {
		if i > 0 {
			if extractor, err = expr.Extractor(); err != nil {
				return nil, err
			}
		}
		if extractors[i], err = utildeletion.SetupExtractor(p, extractor); err != nil {
			return nil, err
		}
	}
	ws, err := q.readWindows(ctx, r)
	if err != nil {
		return iter.NoopSampleIterator, err
	}
	sctx, err := withStreamIDs(ctx, r.ids)
	if err != nil {
		return iter.NoopSampleIterator, err
	}
	// Cancel the read with the iterator: Close stops the producer and the ClickHouse query.
	rctx, cancel := context.WithCancel(sctx)
	// Metric queries read everything anyway: one unchunked cursor per window.
	rq := r.rq
	rq.final = true
	// The producer closes wr when it ends; only Close cancels rctx, as workers may still be extracting.
	wr := &windowReader{ws: ws, open: func(w window) (rowSource, error) {
		sql, args := rq.sql(w)
		return openCursor(rctx, q.Conn, sql, args...)
	}}
	return newSampleIter(rctx, cancel, wr, r.byID, extractors, stats.FromContext(ctx)), nil
}
