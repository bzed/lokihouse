package chstore

import (
	"slices"
	"sort"

	"github.com/grafana/loki/v3/pkg/logproto"
)

// defaultWindowRows is the row target of one read window: about 85 MiB of sort
// state at the measured 340 B per row.
const defaultWindowRows = 250_000

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
