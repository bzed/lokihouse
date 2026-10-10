package chstore

import (
	"context"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

type row struct {
	streamID uint64
	ts       time.Time
	line     string
	md       labels.Labels
}

// rowReader streams rows from ClickHouse, dropping exact duplicates (Loki merge-iterator semantics).
type rowReader struct {
	rows      driver.Rows
	cancel    context.CancelFunc
	cur       row
	groupTS   time.Time
	groupSID  uint64
	groupSeen map[string]struct{}
	err       error
}

func (r *rowReader) next() bool {
	for r.rows.Next() {
		var md map[string]string
		if err := r.rows.Scan(&r.cur.streamID, &r.cur.ts, &r.cur.line, &md); err != nil {
			r.err = err
			return false
		}
		r.cur.md = labels.FromMap(md)
		if !r.cur.ts.Equal(r.groupTS) || r.cur.streamID != r.groupSID || r.groupSeen == nil {
			r.groupTS, r.groupSID, r.groupSeen = r.cur.ts, r.cur.streamID, map[string]struct{}{}
		}
		key := r.cur.line + "\xff" + r.cur.md.String()
		if _, dup := r.groupSeen[key]; dup {
			continue
		}
		r.groupSeen[key] = struct{}{}
		return true
	}
	r.err = r.rows.Err()
	return false
}

// openCursor runs a query returning (stream_id, ts, line, structured_metadata) rows.
func openCursor(ctx context.Context, conn driver.Conn, sql string, args ...any) (*rowReader, error) {
	ctx, cancel := context.WithCancel(ctx)
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &rowReader{rows: rows, cancel: cancel}, nil
}

// close cancels the query first: clickhouse-go's Rows.Close otherwise drains the
// whole remaining result, which a log query stopping at its limit does not need.
func (r *rowReader) close() error {
	r.cancel()
	_ = r.rows.Close()
	return nil
}

func mdAdapters(ls labels.Labels) []logproto.LabelAdapter {
	return logproto.FromLabelsToLabelAdapters(ls)
}

// sortedIDs is used to build deterministic IN lists.
func sortedIDs(ss []Stream) []uint64 {
	ids := make([]uint64, len(ss))
	for i, s := range ss {
		ids[i] = s.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
