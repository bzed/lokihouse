package chstore

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
)

var logChunkSize = 1000

func chunkSize(limit uint32) int { return max(int(limit), logChunkSize) }

// keysetReader reads rows in keyset chunks so ClickHouse can use top-N sorting
// instead of sorting the whole range: each chunk is a LIMIT query continuing
// from the last row's key. Rows with an identical key are unmerged exact
// duplicates, so the strict key comparison at a chunk boundary dedups them.
// It has the same shape as rowReader.
type keysetReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   driver.Conn
	base   string // full SELECT ... WHERE (no ORDER/LIMIT)
	args   []any
	dir    logproto.Direction
	chunk  int

	curRow row
	err    error
	done   bool

	rr     *rowReader // current chunk, nil between chunks
	last   row        // last row read from ClickHouse (before dedup), for keyset continuation
	hasRow bool       // a row has been yielded, so the next chunk needs a keyset predicate
}

// continuation returns the keyset predicate for the next chunk, or ("", nil)
// for the first chunk.
func (k *keysetReader) continuation() (string, []any) {
	if !k.hasRow {
		return "", nil
	}
	last := k.continuationKey()
	if k.dir == logproto.BACKWARD {
		return " AND (ts < fromUnixTimestamp64Nano(?, 'UTC') OR (ts = fromUnixTimestamp64Nano(?, 'UTC') AND (stream_id, entry_hash) > (?, ?)))",
			[]any{last[0], last[0], last[1], last[2]}
	}
	return " AND (ts, stream_id, entry_hash) > (fromUnixTimestamp64Nano(?, 'UTC'), ?, ?)", last
}

func (k *keysetReader) openChunk() bool {
	cont, contArgs := k.continuation()
	args := append(append(append([]any{}, k.args...), contArgs...), k.chunk)
	sql := k.base + " LIMIT ?"
	if cont != "" {
		// Insert the continuation into the WHERE clause, before the ORDER BY.
		idx := strings.Index(sql, " ORDER BY ")
		sql = sql[:idx] + " " + cont + sql[idx:]
	}
	rr, err := openCursor(k.ctx, k.conn, sql, args...)
	if err != nil {
		k.err = err
		return false
	}
	k.rr = rr
	return true
}

func (k *keysetReader) next() bool {
	for {
		if k.rr == nil {
			if k.done || !k.openChunk() {
				return false
			}
		}
		if k.rr.next() {
			k.curRow = k.rr.cur()
			k.last = k.rr.lastRow
			k.hasRow = true
			return true
		}
		if k.rr.err != nil {
			k.err = k.rr.err
			return false
		}
		// chunk exhausted: a short chunk ends the read
		if k.rr.count < k.chunk {
			k.done = true
			return false
		}
		_ = k.rr.close()
		k.rr = nil
	}
}

// continuationKey returns the key of the last row read from ClickHouse. The
// timestamp is Unix nanoseconds: clickhouse-go formats a time.Time parameter at
// second precision, which would re-read (or skip) the rest of that second.
func (k *keysetReader) continuationKey() []any {
	return []any{k.last.ts.UnixNano(), k.last.streamID, k.last.entryHash}
}

func (k *keysetReader) close() error {
	if k.rr != nil {
		_ = k.rr.close()
	}
	if k.cancel != nil {
		k.cancel()
	}
	return nil
}

func (k *keysetReader) cur() row     { return k.curRow }
func (k *keysetReader) errOf() error { return k.err }
