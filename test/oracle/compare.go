//go:build oracle

package oracle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "loki.json"), an, 0o644)
		_ = os.WriteFile(filepath.Join(dir, "lokihouse.json"), bn, 0o644)
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:       difflib.SplitLines(string(an)),
			B:       difflib.SplitLines(string(bn)),
			FromFile: "loki.json",
			ToFile:   "lokihouse.json",
		})
		t.Errorf("%s: responses differ (full dumps in %s)\n%s", name, dir, diff)
	}
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
		// For streams results, sort entries that share a timestamp within each stream.
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
		// For series results, data is a bare array. The cross-store series order is
		// not contractually defined (Loki uses index order, lokihouse label order),
		// so compare as a sorted set.
		sortSeries(arr)
	}
	out, err := json.MarshalIndent(root, "", " ")
	if err != nil {
		return nil, err
	}
	return out, nil
}

// sortStable sorts entries within one stream by (timestamp, line, metadata)
// so that same-timestamp entries compare as a sorted set.
func sortStable(vals []any) {
	sort.SliceStable(vals, func(i, j int) bool {
		ei, ej := vals[i].([]any), vals[j].([]any)
		si, _ := ei[0].(string)
		sj, _ := ej[0].(string)
		if si != sj {
			return si < sj
		}
		li, _ := ei[1].(string)
		lj, _ := ej[1].(string)
		return li < lj
	})
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
