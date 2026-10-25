package pushdown

import (
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// TestSplitter_ConcurrentMultiLegQueriesNeverDeadlockPool runs C concurrent
// requests of 36 legs, each leg needing 3 connections, on a 44-connection
// pool. The budget (22) allows 7 reserved legs; the other 22 connections serve
// the requests' inline legs: C inline legs need at most C·(3−1)+1 = 21 ≤ 22,
// so inline work alone cannot hit the pre-existing pool wait (REVIEW.md §000)
// and the test fails only if pieces exceed the budget.
func TestSplitter_ConcurrentMultiLegQueriesNeverDeadlockPool(t *testing.T) {
	const (
		clients = 10
		pool    = 44
		query   = `count_over_time({app="web"} | logfmt [5m]) / count_over_time({app="api"} | logfmt [5m])`
	)
	seedConn, db := chtest.New(t)
	base := seedSplitData(t, chstoreWriter(seedConn))

	opts := chtest.Options(db)
	opts.MaxOpenConns = pool
	opts.MaxIdleConns = pool
	opts.DialTimeout = 5 * time.Second
	conn, err := clickhouse.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ref := rangeParams(t, query, base, base.Add(6*time.Hour-time.Minute), time.Minute)
	_, raw := newSplitter(t, conn, nil, nil)
	want, err := raw.Query(ref).Exec(tctx())
	require.NoError(t, err)

	s, _ := newSplitter(t, conn, NewConnBudget(pool), func(l *validation.Limits) {
		l.QuerySplitDuration = model.Duration(10 * time.Minute)
		l.MaxQueryParallelism = 32
	})
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		errs := make([]error, clients)
		res := make([]logqlmodel.Result, clients)
		for c := range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res[c], errs[c] = s.Query(ref).Exec(tctx())
			}()
		}
		wg.Wait()
		for c := range clients {
			require.NoError(t, errs[c], "round %d client %d", round, c)
			requireDataEqual(t, want.Data, res[c].Data)
			require.EqualValues(t, 36, res[c].Statistics.Summary.Splits)
		}
	}
}
