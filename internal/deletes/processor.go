package deletes

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	logq "github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	utildeletion "github.com/grafana/loki/v3/pkg/util/deletion"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/bzed/lokihouse/internal/chstore"
)

// Processor physically deletes delete requests that are past their cancel
// period. Query-time filtering already hides the matching lines; the processor
// makes the deletion permanent and marks the request processed (which also
// stops query-time filtering for it).
type Processor struct {
	Store        *Store
	Conn         driver.Conn
	DB           string
	Topo         chstore.Topology
	CancelPeriod time.Duration
	Interval     time.Duration
	Logger       log.Logger
	// Now returns the "current time" for the cancel-period check (default time.Now).
	Now func() time.Time

	// maskPending is set after a pass deleted rows and cleared once
	// ApplyDeletedMask succeeded; only the goroutine calling ProcessDue uses it.
	maskPending bool
}

// ProcessDue physically deletes every received request older than CancelPeriod,
// then marks it processed. lightweight_deletes_sync=2 makes the rows invisible
// on every replica before the call returns. Requests are fault-isolated: a
// failing request is logged, retried on the next pass (the rows are still
// hidden by query-time filtering) and does not stop the others. After a pass
// that deleted rows it runs APPLY DELETED MASK so the rows leave the disk too;
// a failed mask step is retried on the next pass.
func (p *Processor) ProcessDue(ctx context.Context, now time.Time) error {
	reqs, err := p.Store.Unprocessed(ctx)
	if err != nil {
		return err
	}
	var first error
	for _, r := range reqs {
		if r.CreatedAt.Time().Add(p.CancelPeriod).After(now) {
			continue
		}
		if err := p.process(ctx, r); err != nil {
			if p.Logger != nil {
				level.Error(p.Logger).Log("msg", "delete request failed; will retry", "request", r.RequestID, "step", "process", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			if first == nil {
				first = fmt.Errorf("delete request %s: %w", r.RequestID, err)
			}
			continue
		}
		p.maskPending = true
		if err := p.Store.MarkProcessed(ctx, r); err != nil {
			if p.Logger != nil {
				level.Error(p.Logger).Log("msg", "delete request failed; will retry", "request", r.RequestID, "step", "mark processed", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			if first == nil {
				first = fmt.Errorf("delete request %s: %w", r.RequestID, err)
			}
		}
	}
	if p.maskPending {
		if err := chstore.ApplyDeletedMask(ctx, p.Conn, p.DB, p.Topo, "logs"); err != nil {
			if p.Logger != nil {
				level.Error(p.Logger).Log("msg", "removing deleted rows from disk failed; will retry", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			if first == nil {
				first = err
			}
		} else {
			p.maskPending = false
		}
	}
	return first
}

// process physically removes the rows a single delete request hides. A bare
// selector (no pipeline stage of any kind) deletes the whole range in SQL;
// anything with a stage scans the rows and deletes exactly the ones the
// query-time filter hides.
func (p *Processor) process(ctx context.Context, r deletionproto.DeleteRequest) error {
	expr, err := syntax.ParseLogSelector(r.Query, true)
	if err != nil {
		return err
	}
	from, through := r.StartTime.UnixNano(), r.EndTime.UnixNano()
	streams, err := chstore.ResolveStreams(ctx, p.Conn, r.UserID, expr.Matchers(), time.Unix(0, from), time.Unix(0, through))
	if err != nil || len(streams) == 0 {
		return err
	}
	ids := make([]uint64, 0, len(streams))
	byID := make(map[uint64]labels.Labels, len(streams))
	for _, s := range streams {
		ids, byID[s.ID] = append(ids, s.ID), s.Labels
	}
	// A bare selector (no pipeline stage of any kind) deletes the whole range in SQL.
	if _, bare := expr.(*syntax.MatchersExpr); bare {
		return chstore.DeleteRange(ctx, p.Conn, p.DB, p.Topo, r.UserID, ids, from, through)
	}
	// Rows the query-time filter hides are exactly the rows to delete.
	filter, err := utildeletion.SetupPipeline(logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Deletes: []*logproto.Delete{{Selector: r.Query, Start: from, End: through}},
	}}, logq.NewNoopPipeline())
	if err != nil {
		return err
	}
	sp := map[uint64]logq.StreamPipeline{}
	var keys []chstore.RowKey
	flush := func() error {
		err := chstore.DeleteRows(ctx, p.Conn, p.DB, p.Topo, r.UserID, keys)
		keys = keys[:0]
		return err
	}
	err = chstore.ScanRows(ctx, p.Conn, r.UserID, ids, from, through, func(row chstore.Row) error {
		s := sp[row.StreamID]
		if s == nil {
			s = filter.ForStream(byID[row.StreamID])
			sp[row.StreamID] = s
		}
		if _, _, keep := s.ProcessString(row.TS, row.Line, row.MD); keep {
			return nil
		}
		keys = append(keys, row.RowKey)
		if len(keys) == chstore.DeleteBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// Run calls ProcessDue every Interval until ctx ends; errors are logged.
func (p *Processor) Run(ctx context.Context) {
	now := p.Now
	if now == nil {
		now = time.Now
	}
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		if err := p.ProcessDue(ctx, now()); err != nil && ctx.Err() == nil {
			if p.Logger != nil {
				level.Error(p.Logger).Log("msg", "processing delete requests", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
