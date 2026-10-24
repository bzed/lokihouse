package chstore

import (
	"context"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

var hr = time.Hour.Nanoseconds()

func hs(start int64, entries uint64) HourStat {
	return HourStat{StreamID: 1, Entries: entries, MinTS: start, MaxTS: start + hr - 1}
}

func TestPlanWindows(t *testing.T) {
	m30 := (30 * time.Minute).Nanoseconds()
	for _, tc := range []struct {
		name          string
		hours         []HourStat
		from, through int64
		target        uint64
		want          []window
	}{
		{"no stats", nil, 0, 3 * hr, 10, []window{{0, 3 * hr}}},
		{"cut at hours", []HourStat{hs(0, 100), hs(hr, 100), hs(2*hr, 100)}, 0, 3 * hr, 150,
			[]window{{0, hr}, {hr, 2 * hr}, {2 * hr, 3 * hr}}},
		{"merge small hours", []HourStat{hs(0, 10), hs(hr, 10), hs(2*hr, 10)}, 0, 3 * hr, 150, []window{{0, 3 * hr}}},
		{"split big hour", []HourStat{hs(0, 1000)}, 0, hr, 300,
			[]window{{0, hr / 4}, {hr / 4, hr / 2}, {hr / 2, 3 * hr / 4}, {3 * hr / 4, hr}}},
		{"unaligned bounds", []HourStat{hs(0, 100), hs(hr, 100)}, m30 + 1, hr + m30, 150,
			[]window{{m30 + 1, hr}, {hr, hr + m30}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, planWindows(tc.hours, tc.from, tc.through, tc.target, logproto.FORWARD))
			back := planWindows(tc.hours, tc.from, tc.through, tc.target, logproto.BACKWARD)
			slices.Reverse(back)
			require.Equal(t, tc.want, back)
		})
	}
}

func TestPlanWindows_Invariants(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for range 500 {
		var hours []HourStat
		for h := range int64(rnd.Intn(8)) {
			for range rnd.Intn(3) { // several streams per hour
				hours = append(hours, hs(h*hr, uint64(rnd.Intn(500))))
			}
		}
		from := rnd.Int63n(2 * hr)
		through := from + 1 + rnd.Int63n(6*hr)
		target := uint64(1 + rnd.Intn(400))
		ws := planWindows(hours, from, through, target, logproto.FORWARD)
		require.NotEmpty(t, ws)
		require.Equal(t, from, ws[0].lo)
		require.Equal(t, through, ws[len(ws)-1].hi, "%v %v %v %d", hours, ws, from, target)
		for i, w := range ws {
			require.Less(t, w.lo, w.hi)
			if i > 0 {
				require.Equal(t, ws[i-1].hi, w.lo, "%v %v %v %d %d", hours, ws, from, through, target)
			}
		}
	}
}

func TestPlanWindows_ZeroTargetUsesDefault(t *testing.T) {
	ws := planWindows([]HourStat{hs(0, 2*defaultWindowRows)}, 0, 2*hr, 0, logproto.FORWARD)
	require.NotEmpty(t, ws)
	require.Equal(t, window{0, 2 * hr}, window{ws[0].lo, ws[len(ws)-1].hi})
}

func TestRowQuery_FinalRendering(t *testing.T) {
	sql, _ := rowQuery{tenant: "t", final: true}.sql(window{1, 2})
	require.Contains(t, sql, "FROM logs FINAL WHERE")
	require.True(t, strings.HasSuffix(sql, " SETTINGS do_not_merge_across_partitions_select_final = 1"), sql)

	sql, _ = rowQuery{tenant: "t"}.sql(window{1, 2})
	require.NotContains(t, sql, "FINAL")
	require.NotContains(t, sql, "SETTINGS")
}

// One resolved stream: the read follows the part sorting key
// (tenant, stream_id, ts, entry_hash), so ClickHouse reads the rows in
// physical order and stops at the chunk LIMIT instead of sorting the range.
// Several streams keep the global timestamp order: the engine and the limit
// need the newest rows overall, not per stream.
func TestRowQuery_SingleStreamPartOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		rq   rowQuery
		want string
	}{
		{"single backward", rowQuery{tenant: "t", single: true, dir: logproto.BACKWARD}, " ORDER BY stream_id DESC, ts DESC, entry_hash DESC"},
		{"single forward", rowQuery{tenant: "t", single: true, dir: logproto.FORWARD}, " ORDER BY stream_id ASC, ts ASC, entry_hash ASC"},
		{"multi backward", rowQuery{tenant: "t", dir: logproto.BACKWARD}, " ORDER BY ts DESC, stream_id ASC, entry_hash"},
		{"multi forward", rowQuery{tenant: "t", dir: logproto.FORWARD}, " ORDER BY ts ASC, stream_id ASC, entry_hash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql, _ := tc.rq.sql(window{1, 2})
			require.Contains(t, sql, tc.want, sql)
		})
	}
}

// A per-stream read (the sample fan-out) restricts one stream by ID instead of
// the external stream set; the stream ID binds after the tenant, before the
// window bounds and the filter.
func TestRowQuery_PerStreamRendering(t *testing.T) {
	id := uint64(42)
	rq := rowQuery{tenant: "t", single: true, streamID: &id, dir: logproto.FORWARD, final: true,
		filter: Fragment{SQL: "line LIKE ?", Args: []any{"%x%"}}}
	sql, args := rq.sql(window{1, 2})
	require.Contains(t, sql, "FROM logs FINAL WHERE tenant = ? AND stream_id = ? AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts < fromUnixTimestamp64Nano(?, 'UTC')")
	require.Contains(t, sql, " AND line LIKE ?")
	require.NotContains(t, sql, "lh_stream_ids")
	require.Contains(t, sql, " ORDER BY stream_id ASC, ts ASC, entry_hash ASC")
	require.True(t, strings.HasSuffix(sql, " SETTINGS do_not_merge_across_partitions_select_final = 1"), sql)
	require.Equal(t, []any{"t", id, int64(1), int64(2), "%x%"}, args)

	// Without a stream ID the query reads the external stream set.
	sql, args = rowQuery{tenant: "t"}.sql(window{1, 2})
	require.Contains(t, sql, "WHERE tenant = ? AND "+streamIDsIn)
	require.Equal(t, []any{"t", int64(1), int64(2)}, args)
}

// Ranges of at most noStats read in one window; the hourly stats are not
// consulted (the test would fail on the nil connection otherwise).
func TestQuerierWindows_NoStatsRange(t *testing.T) {
	q := &Querier{}
	from := time.Unix(0, 0)
	for _, tc := range []struct {
		name          string
		span, noStats time.Duration
		want          []window
	}{
		{"within", 90 * time.Minute, 2 * time.Hour, []window{{0, (90 * time.Minute).Nanoseconds()}}},
		{"at the threshold", 2 * time.Hour, 2 * time.Hour, []window{{0, (2 * time.Hour).Nanoseconds()}}},
		{"one hour", time.Hour, time.Hour, []window{{0, hourNs}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws, err := q.windows(context.Background(), "t", []uint64{1}, from, from.Add(tc.span), logproto.FORWARD, tc.noStats)
			require.NoError(t, err)
			require.Equal(t, tc.want, ws)
		})
	}
}
