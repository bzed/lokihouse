package chstore

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

// The keyset continuation must follow the query's ORDER BY: the part's
// physical key for a single stream, the global timestamp order otherwise.
func TestKeysetContinuation(t *testing.T) {
	last := row{ts: time.Unix(0, 123456789), streamID: 7, entryHash: 9}
	ts := int64(123456789)
	for _, tc := range []struct {
		name     string
		rq       rowQuery
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "multi-stream backward",
			rq:       rowQuery{dir: logproto.BACKWARD},
			wantSQL:  " AND (ts < fromUnixTimestamp64Nano(?, 'UTC') OR (ts = fromUnixTimestamp64Nano(?, 'UTC') AND (stream_id, entry_hash) > (?, ?)))",
			wantArgs: []any{ts, ts, uint64(7), uint64(9)},
		},
		{
			name:     "multi-stream forward",
			rq:       rowQuery{dir: logproto.FORWARD},
			wantSQL:  " AND (ts, stream_id, entry_hash) > (fromUnixTimestamp64Nano(?, 'UTC'), ?, ?)",
			wantArgs: []any{ts, uint64(7), uint64(9)},
		},
		{
			// stream_id is constant under single: the prunable (ts,
			// entry_hash) form, which the primary-key analysis can use to
			// skip the granules the earlier chunks already read.
			name:     "single-stream backward",
			rq:       rowQuery{dir: logproto.BACKWARD, single: true},
			wantSQL:  " AND (ts < fromUnixTimestamp64Nano(?, 'UTC') OR (ts = fromUnixTimestamp64Nano(?, 'UTC') AND entry_hash < ?))",
			wantArgs: []any{ts, ts, uint64(9)},
		},
		{
			name:     "single-stream forward",
			rq:       rowQuery{dir: logproto.FORWARD, single: true},
			wantSQL:  " AND (ts > fromUnixTimestamp64Nano(?, 'UTC') OR (ts = fromUnixTimestamp64Nano(?, 'UTC') AND entry_hash > ?))",
			wantArgs: []any{ts, ts, uint64(9)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &keysetReader{rq: tc.rq, last: last, hasRow: true}
			sql, args := k.continuation()
			require.Equal(t, tc.wantSQL, sql)
			require.Equal(t, tc.wantArgs, args)

			// no row read yet: no continuation
			k = &keysetReader{rq: tc.rq}
			sql, args = k.continuation()
			require.Empty(t, sql)
			require.Nil(t, args)
		})
	}
}
