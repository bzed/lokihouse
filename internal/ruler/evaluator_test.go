package ruler_test

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ruler"
)

// overrides returns Loki's default per-tenant limits.
func overrides(t *testing.T) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("limits", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

// engine returns Loki's engine over the ClickHouse querier (Tier B).
func engine(t *testing.T, conn driver.Conn) ruler.Engine {
	return api.NewPlainQuerier(logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides(t), log.NewNopLogger()))
}

// writeLines stores n lines of {app="a"} for tenant, one second apart, ending
// a minute before now.
func writeLines(t *testing.T, conn driver.Conn, tenant string, n int) {
	t.Helper()
	var es []logproto.Entry
	for i := 0; i < n; i++ {
		es = append(es, logproto.Entry{Timestamp: time.Now().Add(-time.Minute - time.Duration(i)*time.Second), Line: "line"})
	}
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), tenant, []logproto.Stream{{Labels: `{app="a"}`, Entries: es}}))
}

func TestEvaluator_InstantQueryAtNow(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	writeLines(t, conn, "t2", 1)
	ev := &ruler.Evaluator{Engine: engine(t, conn)}
	now := time.Now()

	for tenant, want := range map[string]float64{"t1": 3, "t2": 1} {
		res, err := ev.Eval(user.InjectOrgID(context.Background(), tenant), `sum by (app) (count_over_time({app="a"}[5m]))`, now)
		require.NoError(t, err)
		vec, ok := res.Data.(promql.Vector)
		require.True(t, ok, "got %T", res.Data)
		require.Len(t, vec, 1)
		require.Equal(t, want, vec[0].F, tenant)
		require.Equal(t, `{app="a"}`, vec[0].Metric.String())
		require.Equal(t, now.UnixMilli(), vec[0].T)
	}
}

func TestEvaluator_ParseError(t *testing.T) {
	conn, _ := chtest.New(t)
	ev := &ruler.Evaluator{Engine: engine(t, conn)}
	_, err := ev.Eval(user.InjectOrgID(context.Background(), "t1"), `sum(`, time.Now())
	require.Error(t, err)
}
