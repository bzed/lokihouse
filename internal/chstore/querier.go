package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

type Querier struct {
	Conn driver.Conn
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

// openRows resolves streams and builds the base WHERE clause for the selector.
func (q *Querier) openRows(ctx context.Context, sel syntax.LogSelectorExpr, req interface {
	GetStart() time.Time
	GetEnd() time.Time
}, dir logproto.Direction) (base string, args []any, byID map[uint64]Stream, err error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, sel.Matchers(), req.GetStart(), req.GetEnd())
	if err != nil || len(streams) == 0 {
		return "", nil, nil, err
	}
	byID = make(map[uint64]Stream, len(streams))
	for _, s := range streams {
		byID[s.ID] = s
	}
	base = "SELECT stream_id, ts, line, structured_metadata, entry_hash FROM logs WHERE tenant = ? AND stream_id IN ? AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts < fromUnixTimestamp64Nano(?, 'UTC')"
	// Bounds are bound as Unix nanoseconds: clickhouse-go formats a time.Time
	// parameter at second precision.
	args = []any{tenantID, sortedIDs(streams), req.GetStart().UnixNano(), req.GetEnd().UnixNano()}
	if lf := LeadingLineFilterSQL(sel); !lf.Empty() {
		base += " AND " + lf.SQL
		args = append(args, lf.Args...)
	}
	order := "ts ASC"
	if dir == logproto.BACKWARD {
		order = "ts DESC"
	}
	// The base includes the ORDER BY (before LIMIT): a continuation predicate is
	// appended after the WHERE clause by the keyset reader.
	return base + " ORDER BY " + order + ", stream_id ASC, entry_hash", args, byID, nil
}

func (q *Querier) SelectLogs(ctx context.Context, p logql.SelectLogParams) (iter.EntryIterator, error) {
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	pipeline, err := sel.Pipeline()
	if err != nil {
		return nil, err
	}
	base, args, byID, err := q.openRows(ctx, sel, p.QueryRequest, p.Direction)
	if err != nil || len(byID) == 0 {
		return iter.NoopEntryIterator, err
	}
	k := &keysetReader{
		ctx: ctx, conn: q.Conn, base: base, args: args,
		dir: p.Direction, chunk: chunkSize(p.QueryRequest.Limit),
	}
	// cancel the keyset reader with the request context so a cancelled query stops the read
	k.ctx, k.cancel = context.WithCancel(ctx)
	return &entryIter{rr: k, byID: byID, pipeline: pipeline, sp: map[uint64]log.StreamPipeline{}}, nil
}

type entryIter struct {
	rr       rowSource
	byID     map[uint64]Stream
	pipeline log.Pipeline
	sp       map[uint64]log.StreamPipeline
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
		line, res, matches := sp.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !matches {
			continue
		}
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
	sel, err := p.LogSelector()
	if err != nil {
		return nil, err
	}
	base, args, byID, err := q.openRows(ctx, sel, p.SampleQueryRequest, logproto.FORWARD)
	if err != nil || len(byID) == 0 {
		return iter.NoopSampleIterator, err
	}
	// Metric queries read everything anyway: a single unchunked cursor.
	rr, err := openCursor(ctx, q.Conn, base, args...)
	if err != nil {
		return iter.NoopSampleIterator, err
	}
	return &sampleIter{rr: rr, byID: byID, extractor: extractor, ex: map[uint64]log.StreamSampleExtractor{}}, nil
}
