package chstore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
)

// defaultWindowRows is the row target of one read window: about 85 MiB of sort
// state at the measured 340 B per row.
const defaultWindowRows = 250_000

// perStreamContext routes a query that reads exactly one stream to the shard
// owning it: the Distributed table shards by cityHash64(tenant, stream_id),
// and with the default optimize_skip_unused_shards = 0 the query still goes
// to every shard, where the others read nothing.
func perStreamContext(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"optimize_skip_unused_shards": 1}))
}

const hourNs = int64(3600) * 1_000_000_000

// window is a half-open time range [lo, hi) in Unix nanoseconds.
type window struct{ lo, hi int64 }

// planWindows cuts [from, through) into contiguous windows holding about
// target entries each, using the per-hour entry counts in hours (all streams).
// Windows always cover the whole range, whatever the stats say. An hour holding
// more than target entries is split into equal sub-windows. The result is in
// query direction.
func planWindows(hours []HourStat, from, through int64, target uint64, dir logproto.Direction) []window {
	counts := map[int64]uint64{}
	for _, h := range hours {
		start := h.MinTS - h.MinTS%hourNs
		if start < through && start+hourNs > from {
			counts[start] += h.Entries
		}
	}
	starts := make([]int64, 0, len(counts))
	for s := range counts {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	if target == 0 {
		target = defaultWindowRows
	}
	var ws []window
	lo, acc := from, uint64(0)
	for _, s := range starts {
		count := counts[s]
		switch {
		case count > target:
			if acc > 0 {
				ws = append(ws, window{lo, s})
			}
			a, b := max(s, from), min(s+hourNs, through)
			if acc == 0 {
				a = lo // absorb the statless gap before this hour
			}
			n := int64((count + target - 1) / target) //nolint:gosec // count/target is bounded by the stored row count
			for i := range n {
				ws = append(ws, window{a + (b-a)*i/n, a + (b-a)*(i+1)/n})
			}
			lo, acc = b, 0
		case acc > 0 && acc+count > target:
			ws = append(ws, window{lo, s})
			lo, acc = max(s, from), count
		default:
			acc += count
		}
	}
	ws = append(ws, window{lo, through})
	ws = slices.DeleteFunc(ws, func(w window) bool { return w.lo >= w.hi })
	if dir == logproto.BACKWARD {
		slices.Reverse(ws)
	}
	return ws
}

// rowQuery renders the row SELECT of one read window.
type rowQuery struct {
	tenant string
	filter Fragment
	dir    logproto.Direction
	// final lets ClickHouse remove duplicate rows (FINAL). Only unchunked
	// metric reads set it: FINAL defeats top-N sorting on unmerged parts, which
	// made a 1000-row keyset chunk 75 ms instead of 6 ms, and keysetReader
	// inserts a predicate before ORDER BY and appends LIMIT, which a trailing
	// SETTINGS clause would break.
	final bool
	// single is set when the selector resolved to exactly one stream. The
	// rows of a part are then physically ordered by (tenant, stream_id, ts,
	// entry_hash), so the read follows that order and ClickHouse reads it in
	// order (stopping at the chunk LIMIT) instead of sorting the range by
	// global timestamp. Multi-stream reads must keep the global timestamp
	// order: the engine and the limit need the newest rows overall, not per
	// stream.
	single bool
	// streamID restricts the query to one stream by id (a single-stream read)
	// instead of the external id set, so the shard analysis can route it to
	// the owning shard (perStreamContext); nil reads the streams of the set.
	streamID *uint64
}

// sql renders the query for w, without LIMIT. The ORDER BY comes last: the
// keyset reader inserts its continuation predicate before it. Bounds are bound
// as Unix nanoseconds: clickhouse-go formats a time.Time parameter at second
// precision.
func (q rowQuery) sql(w window) (string, []any) {
	from := "logs"
	if q.final {
		from = "logs FINAL"
	}
	streams := streamIDsIn
	args := []any{q.tenant}
	if q.streamID != nil {
		streams = "stream_id = ?"
		args = append(args, *q.streamID)
	}
	s := "SELECT stream_id, ts, line, structured_metadata, entry_hash FROM " + from + " WHERE tenant = ? AND " + streams +
		" AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts < fromUnixTimestamp64Nano(?, 'UTC')"
	args = append(args, w.lo, w.hi)
	if !q.filter.Empty() {
		s += " AND " + q.filter.SQL
		args = append(args, q.filter.Args...)
	}
	s += " ORDER BY " + q.orderBy()
	if q.final {
		// Duplicates share ts, hence the day partition, so merging per partition is exact.
		s += " SETTINGS do_not_merge_across_partitions_select_final = 1"
	}
	return s, args
}

// orderBy returns the ORDER BY clause: the part sorting key, in query
// direction, for a single stream; the global timestamp order otherwise.
func (q rowQuery) orderBy() string {
	if q.single {
		if q.dir == logproto.BACKWARD {
			return "stream_id DESC, ts DESC, entry_hash DESC"
		}
		return "stream_id ASC, ts ASC, entry_hash ASC"
	}
	if q.dir == logproto.BACKWARD {
		return "ts DESC, stream_id ASC, entry_hash"
	}
	return "ts ASC, stream_id ASC, entry_hash"
}

// windows returns the read windows of [from, through): one for ranges of at
// most an hour, otherwise planned from the hourly stream stats.
func (q *Querier) windows(ctx context.Context, tenant string, ids []uint64, from, through time.Time, dir logproto.Direction) ([]window, error) {
	lo, hi := from.UnixNano(), through.UnixNano()
	if hi-lo <= hourNs {
		return []window{{lo, hi}}, nil
	}
	hours, err := HourlyStats(ctx, q.Conn, tenant, ids, from, through)
	if err != nil {
		return nil, fmt.Errorf("plan read windows: %w", err)
	}
	return planWindows(hours, lo, hi, q.WindowRows, dir), nil
}

// windowReader is a rowSource reading one window at a time, in query
// direction. It opens a window only when the previous one is exhausted and
// closes each before opening the next.
type windowReader struct {
	ws     []window
	open   func(window) (rowSource, error)
	cancel context.CancelFunc // optional, called on close
	opened int
	cursor rowSource
	err    error
	// primed is set by prime: the first next() already ran and its result,
	// primedOK, is returned by the next call to next().
	primed, primedOK bool
}

// prime runs the first next() ahead of time, so the first window's read starts
// now, and remembers its result for the following next(). Call it at most
// once, before any next().
func (r *windowReader) prime() {
	r.primedOK = r.next()
	r.primed = true
}

func (r *windowReader) next() bool {
	if r.primed {
		r.primed = false
		return r.primedOK
	}
	for r.err == nil {
		if r.cursor == nil {
			if r.opened >= len(r.ws) {
				return false
			}
			c, err := r.open(r.ws[r.opened])
			r.opened++
			if err != nil {
				r.err = err
				return false
			}
			r.cursor = c
		}
		if r.cursor.next() {
			return true
		}
		r.err = r.cursor.errOf()
		_ = r.cursor.close() // close error is not actionable after the window ended
		r.cursor = nil
	}
	return false
}

func (r *windowReader) cur() row     { return r.cursor.cur() }
func (r *windowReader) errOf() error { return r.err }

func (r *windowReader) close() error {
	if r.cursor != nil {
		_ = r.cursor.close() // best effort: the caller is abandoning the read
		r.cursor = nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}
