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
	const rows = 50000
	conn, db := chtest.New(t)
	logs := chtest.Topology().Local("logs")
	require.NoError(t, conn.Exec(ctx, "INSERT INTO "+logs+" (tenant, stream_id, ts, line) SELECT 't1', 1, toDateTime64(1, 9, 'UTC') + number, concat('row ', toString(number), if(number = 31337, ' needle here', '')) FROM numbers(?)", rows))
	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE "+logs+" FINAL"))

	count := func(f chstore.Fragment) {
		t.Helper()
		var n uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM logs WHERE tenant = 't1' AND "+f.SQL, f.Args...).Scan(&n))
	}
	check := func(q string, f chstore.Fragment) {
		t.Helper()
		since := time.Now()
		count(f)
		require.Less(t, chtest.LogsRowsRead(t, conn, db, since), uint64(rows/2), q)
	}
	for _, q := range []string{`{a="b"} |= "needle"`, `{a="b"} |~ "needle"`, `{a="b"} |~ "needle(s|d)?"`, `{a="b"} |= "needle" or "haystack"`} {
		sel, err := syntax.ParseLogSelector(q, true)
		require.NoError(t, err)
		check("leading: "+q, chstore.LeadingLineFilterSQL(sel))
		f, ok := chstore.ExactLineFilterSQL(sel)
		require.True(t, ok, q)
		check("exact: "+q, f)
	}
}
