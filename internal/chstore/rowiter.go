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
	streamID  uint64
	ts        time.Time
	line      string
	md        labels.Labels
	entryHash uint64
}

// rowReader streams rows from ClickHouse, dropping exact duplicates (Loki merge-iterator semantics).
type rowReader struct {
	rows      driver.Rows
	cancel    context.CancelFunc
	curRow    row
	lastRow   row // last raw row read from ClickHouse (before dedup), for keyset continuation
	groupTS   time.Time
	groupSID  uint64
	groupSeen map[string]struct{}
	err       error
	count     int // rows yielded by next() since the chunk was opened
}

func (r *rowReader) next() bool {
	for r.rows.Next() {
		var md map[string]string
		if err := r.rows.Scan(&r.curRow.streamID, &r.curRow.ts, &r.curRow.line, &md, &r.curRow.entryHash); err != nil {
			r.err = err
			return false
		}
		r.count++
		r.curRow.md = labels.FromMap(md)
		r.lastRow = r.curRow
		if !r.curRow.ts.Equal(r.groupTS) || r.curRow.streamID != r.groupSID || r.groupSeen == nil {
			r.groupTS, r.groupSID, r.groupSeen = r.curRow.ts, r.curRow.streamID, map[string]struct{}{}
		}
		key := r.curRow.line + "\xff" + r.curRow.md.String()
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
	ctx = WithQueryStats(ctx)
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

func (r *rowReader) cur() row     { return r.curRow }
func (r *rowReader) errOf() error { return r.err }

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
