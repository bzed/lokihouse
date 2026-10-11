//go:build oracle

package oracle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
)

// Compare normalises and diffs two Loki JSON responses, failing t on any
// difference. data.stats and data.encodingFlags are dropped. Within each stream,
// entries sharing a timestamp are sorted (spec §7.1: tie order inside one stream
// is undefined).
func Compare(t *testing.T, name string, a, b []byte) {
	t.Helper()
	an, aerr := normalise(a)
	bn, berr := normalise(b)
	if aerr != nil || berr != nil {
		if aerr == nil {
			aerr = berr
		}
		t.Fatalf("%s: normalise failed: %v\nloki=%s\nhouse=%s", name, aerr, a, b)
	}
	if !bytes.Equal(an, bn) {
		if !floatTolerantEqual(an, bn) {
			dir := t.TempDir()
			_ = os.WriteFile(filepath.Join(dir, "loki.json"), an, 0o644)
			_ = os.WriteFile(filepath.Join(dir, "lokihouse.json"), bn, 0o644)
			diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				A:        difflib.SplitLines(string(an)),
				B:        difflib.SplitLines(string(bn)),
				FromFile: "loki.json",
				ToFile:   "lokihouse.json",
			})
			t.Errorf("%s: responses differ (full dumps in %s)\n%s", name, dir, diff)
		}
	}
}

// floatTolerantEqual returns true when two normalised JSON documents differ
// only in the trailing digits of floating-point values (the two stores
// accumulate sums/products in different orders, spec §7.2). It walks both
// documents in parallel and compares every leaf; numeric leaves within a
// relative tolerance of 1e-9 are treated as equal.
func floatTolerantEqual(a, b []byte) bool {
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return bytes.Equal(a, b)
	}
	return floatsEqual(va, vb)
}

func floatsEqual(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, av := range x {
			bv, ok := y[k]
			if !ok || !floatsEqual(av, bv) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !floatsEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case float64:
		y, ok := b.(float64)
		if !ok {
			return false
		}
		if x == y {
			return true
		}
		d := x - y
		if d < 0 {
			d = -d
		}
		abs := x
		if abs < 0 {
			abs = -abs
		}
		if abs == 0 {
			return d == 0
		}
		return d/abs < 1e-9
	case string:
		s := a.(string)
		t := b.(string)
		if s == t {
			return true
		}
		fa, ea := parseFloatLenient(s)
		fb, eb := parseFloatLenient(t)
		if ea == nil && eb == nil {
			d := fa - fb
			if d < 0 {
				d = -d
			}
			abs := fa
			if abs < 0 {
				abs = -abs
			}
			if abs == 0 {
				return d == 0
			}
			return d/abs < 1e-9
		}
		return false
	default:
		return a == b
	}
}

func parseFloatLenient(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// normalise parses a Loki query/label/series response into a canonical JSON
// form. It returns the canonical bytes plus any parse error. When the body is
// not JSON (e.g. a plain-text error), it is normalised to a single trimmed line
// so identical errors compare equal.
func normalise(raw []byte) ([]byte, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return []byte(strings.TrimSpace(string(raw))), nil
	}
	data, _ := root["data"].(map[string]any)
	if data != nil {
		delete(data, "stats")
		delete(data, "encodingFlags")
		if res, ok := data["result"].([]any); ok {
			for _, item := range res {
				obj, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if vals, ok := obj["values"].([]any); ok {
					sortStable(vals)
				}
			}
		}
	} else if arr, ok := root["data"].([]any); ok {
		sortSeries(arr)
	}
	out, err := json.MarshalIndent(root, "", " ")
	if err != nil {
		return nil, err
	}
	return out, nil
}

// sortStable sorts entries within one stream by (timestamp, line, metadata)
// so that same-timestamp entries compare as a sorted set. Per spec §7.1, the
// order of entries that share a timestamp inside one stream is undefined, so
// the suite compares such groups as a sorted set.
func sortStable(vals []any) {
	sort.Slice(vals, func(i, j int) bool {
		ei, ej := vals[i].([]any), vals[j].([]any)
		// Matrix points carry a numeric timestamp and are unique per series:
		// order by time, never by value (values may differ in the last float
		// bits, which floatTolerantEqual accepts).
		if ni, ok := ei[0].(float64); ok {
			nj, _ := ej[0].(float64)
			return ni < nj
		}
		si, _ := ei[0].(string)
		sj, _ := ej[0].(string)
		if si != sj {
			return si < sj
		}
		li, _ := ei[1].(string)
		lj, _ := ej[1].(string)
		if li != lj {
			return li < lj
		}
		return entryMeta(ei) < entryMeta(ej)
	})
}

func entryMeta(e []any) string {
	if len(e) < 3 {
		return ""
	}
	if m, ok := e[2].(map[string]any); ok {
		b, _ := json.Marshal(m)
		return string(b)
	}
	return fmt.Sprintf("%v", e[2])
}

// sortSeries orders series by their canonical label-set string so the two
// stores compare as the same set regardless of iteration order.
func sortSeries(ser []any) {
	sort.Slice(ser, func(i, j int) bool {
		return seriesKey(ser[i]) < seriesKey(ser[j])
	})
}

func seriesKey(s any) string {
	m, ok := s.(map[string]any)
	if !ok {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(fmt.Sprintf("%v", m[k]))
		b.WriteByte(';')
	}
	return b.String()
}
