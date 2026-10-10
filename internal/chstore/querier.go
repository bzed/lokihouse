package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cespare/xxhash/v2"
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

// openRows resolves streams and opens an ordered row cursor for the selector.
func (q *Querier) openRows(ctx context.Context, sel syntax.LogSelectorExpr, req interface {
	GetStart() time.Time
	GetEnd() time.Time
}, dir logproto.Direction) (*rowReader, map[uint64]Stream, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, sel.Matchers(), req.GetStart(), req.GetEnd())
	if err != nil || len(streams) == 0 {
		return nil, nil, err
	}
	byID := make(map[uint64]Stream, len(streams))
	for _, s := range streams {
		byID[s.ID] = s
	}
	order := "ts ASC"
	if dir == logproto.BACKWARD {
		order = "ts DESC"
	}
	sql := "SELECT stream_id, ts, line, structured_metadata FROM logs WHERE tenant = ? AND stream_id IN ? AND ts >= ? AND ts < ?"
	args := []any{tenantID, sortedIDs(streams), req.GetStart().UTC(), req.GetEnd().UTC()}
	if lf := LeadingLineFilterSQL(sel); !lf.Empty() {
		sql += " AND " + lf.SQL
		args = append(args, lf.Args...)
	}
	sql += " ORDER BY " + order + ", stream_id ASC, entry_hash"
	rr, err := openCursor(ctx, q.Conn, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("select logs: %w", err)
	}
	return rr, byID, nil
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
	rr, byID, err := q.openRows(ctx, sel, p.QueryRequest, p.Direction)
	if err != nil || rr == nil {
		return iter.NoopEntryIterator, err
	}
	return &entryIter{rr: rr, byID: byID, pipeline: pipeline, sp: map[uint64]log.StreamPipeline{}}, nil
}

type entryIter struct {
	rr       *rowReader
	byID     map[uint64]Stream
	pipeline log.Pipeline
	sp       map[uint64]log.StreamPipeline
	cur      logproto.Entry
	lbls     string
	hash     uint64
}

func (it *entryIter) Next() bool {
	for it.rr.next() {
		r := it.rr.cur
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
func (it *entryIter) Err() error         { return it.rr.err }
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
	rr, byID, err := q.openRows(ctx, sel, p.SampleQueryRequest, logproto.FORWARD)
	if err != nil || rr == nil {
		return iter.NoopSampleIterator, err
	}
	defer rr.close()
	series := map[string]*logproto.Series{}
	ex := map[uint64]log.StreamSampleExtractor{}
	for rr.next() {
		r := rr.cur
		se, ok := ex[r.streamID]
		if !ok {
			se = extractor.ForStream(byID[r.streamID].Labels)
			ex[r.streamID] = se
		}
		s, ok := se.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !ok {
			continue
		}
		key := s.Labels.String()
		ser, found := series[key]
		if !found {
			ser = &logproto.Series{Labels: key, StreamHash: r.streamID}
			series[key] = ser
		}
		ser.Samples = append(ser.Samples, logproto.Sample{
			Timestamp: r.ts.UnixNano(), Value: s.Value, Hash: xxhash.Sum64String(key + r.line),
		})
	}
	if rr.err != nil {
		return nil, rr.err
	}
	out := make([]logproto.Series, 0, len(series))
	for _, s := range series {
		out = append(out, *s)
	}
	return iter.NewTimestampFirstMultiSeriesIterator(out), nil
}
