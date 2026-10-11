package patterns_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/patterns"
)

var t0 = time.Now().UTC().Truncate(24 * time.Hour).Add(-12 * time.Hour)

func seed(t *testing.T, w *chstore.Writer, tenant string, streams ...logproto.Stream) {
	require.NoError(t, w.Write(context.Background(), tenant, streams, nil))
}

func mk(t *testing.T, tenant string, n int, base time.Time, format string, lvl string) logproto.Stream {
	var entries []logproto.Entry
	var md []logproto.LabelAdapter
	if lvl != "" {
		md = []logproto.LabelAdapter{{Name: "detected_level", Value: lvl}}
	}
	for i := 0; i < n; i++ {
		e := logproto.Entry{Timestamp: base.Add(time.Duration(i) * 10 * time.Second)}
		switch format {
		case "info":
			e.Line = "level=info msg=\"user " + itoa(i) + " logged in\""
		case "error":
			e.Line = "level=error msg=\"db timeout after " + itoa(i) + "ms\""
		default:
			e.Line = "level=debug msg=\"tick " + itoa(i) + "\""
		}
		if md != nil {
			e.StructuredMetadata = md
		}
		entries = append(entries, e)
	}
	return logproto.Stream{Labels: `{app="w"}`, Entries: entries}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for n := i; n > 0; n /= 10 {
		s = string(rune('0'+n%10)) + s
	}
	return s
}

func totalLevels(t *testing.T, resp *logproto.QueryPatternsResponse) map[string]int64 {
	t.Helper()
	tot := map[string]int64{}
	var patterns []string
	for _, s := range resp.Series {
		var n int64
		for _, p := range s.Samples {
			n += p.Value
		}
		tot[s.Level] += n
		patterns = append(patterns, s.Pattern)
	}
	for _, p := range patterns {
		require.Contains(t, p, "<_>", "pattern must contain a parameter: %q", p)
	}
	return tot
}

func TestPatterns_TwoLevels(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		mk(t, "t1", 40, t0, "info", "info"),
		mk(t, "t1", 40, t0.Add(2*time.Minute), "error", "error"),
	)
	p := &patterns.Querier{Q: &chstore.Querier{Conn: conn}, Limits: nil}
	resp, err := p.Query(user.InjectOrgID(context.Background(), "t1"), &logproto.QueryPatternsRequest{
		Query: `{app="w"}`, Start: t0, End: t0.Add(10 * time.Minute), Step: int64(time.Minute),
	})
	require.NoError(t, err)
	tot := totalLevels(t, resp)
	// Two levels: info (40) and error (40), both above minClusterSize (30).
	require.Equal(t, map[string]int64{"info": 40, "error": 40}, tot)
}

func TestPatterns_PrunesSmallCluster(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}
	seed(t, w, "t1",
		mk(t, "t1", 40, t0, "info", "info"),
		mk(t, "t1", 40, t0.Add(2*time.Minute), "error", "error"),
		// A third pattern with only 5 lines — below minClusterSize (30), so pruned.
		mk(t, "t1", 5, t0.Add(4*time.Minute), "debug", "debug"),
	)
	p := &patterns.Querier{Q: &chstore.Querier{Conn: conn}, Limits: nil}
	resp, err := p.Query(user.InjectOrgID(context.Background(), "t1"), &logproto.QueryPatternsRequest{
		Query: `{app="w"}`, Start: t0, End: t0.Add(10 * time.Minute), Step: int64(time.Minute),
	})
	require.NoError(t, err)
	tot := totalLevels(t, resp)
	// The debug pattern (total 5) must be pruned; info and error remain.
	require.Equal(t, map[string]int64{"info": 40, "error": 40}, tot)
}
