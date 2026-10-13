package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// The pushed-down line filters must be in a form ClickHouse can answer from the
// line_text index; otherwise every granule of the selected streams is read.
func TestLineFilterSQL_UsesTextIndex(t *testing.T) {
	ctx := context.Background()
	conn, _ := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	require.NoError(t, conn.Exec(ctx, "INSERT INTO "+logs+" (tenant, stream_id, ts, line) VALUES ('t1', 1, ?, 'a needle here'), ('t1', 1, ?, 'nothing')",
		time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC()))

	explain := func(f chstore.Fragment) string {
		t.Helper()
		rows, err := conn.Query(ctx, "EXPLAIN indexes = 1 SELECT count() FROM "+logs+" WHERE tenant = 't1' AND "+f.SQL, f.Args...)
		require.NoError(t, err)
		defer rows.Close()
		var plan string
		for rows.Next() {
			var l string
			require.NoError(t, rows.Scan(&l))
			plan += l + "\n"
		}
		require.NoError(t, rows.Err())
		return plan
	}
	for _, q := range []string{`{a="b"} |= "needle"`, `{a="b"} |~ "needle"`, `{a="b"} |~ "needle(s|d)?"`, `{a="b"} |= "needle" or "haystack"`} {
		sel, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		require.Contains(t, explain(chstore.LeadingLineFilterSQL(sel)), "line_text", "leading: %s", q)
		f, ok := chstore.ExactLineFilterSQL(sel)
		require.True(t, ok, q)
		require.Contains(t, explain(f), "line_text", "exact: %s", q)
	}
}
