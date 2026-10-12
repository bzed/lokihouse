package multitenant_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/multitenant"
)

var errLastTenant = errors.New("last tenant fails")

// failLastStore opens a tracked iterator for every tenant but the last one it
// is asked for, which fails; map iteration order does not matter.
type failLastStore struct {
	tenants int
	calls   int
	opened  []*closeTracker
}

type closeTracker struct{ closed bool }

func (c *closeTracker) Close() error { c.closed = true; return nil }

type trackedEntries struct {
	iter.EntryIterator
	*closeTracker
}

func (t trackedEntries) Close() error { return t.closeTracker.Close() }

type trackedSamples struct {
	iter.SampleIterator
	*closeTracker
}

func (t trackedSamples) Close() error { return t.closeTracker.Close() }

func (s *failLastStore) open() (*closeTracker, error) {
	s.calls++
	if s.calls == s.tenants {
		return nil, errLastTenant
	}
	c := &closeTracker{}
	s.opened = append(s.opened, c)
	return c, nil
}

func (s *failLastStore) SelectLogs(context.Context, logql.SelectLogParams) (iter.EntryIterator, error) {
	c, err := s.open()
	if err != nil {
		return nil, err
	}
	return trackedEntries{iter.NoopEntryIterator, c}, nil
}

func (s *failLastStore) SelectSamples(context.Context, logql.SelectSampleParams) (iter.SampleIterator, error) {
	c, err := s.open()
	if err != nil {
		return nil, err
	}
	return trackedSamples{iter.NoopSampleIterator, c}, nil
}

func (s *failLastStore) Label(context.Context, *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	return &logproto.LabelResponse{}, nil
}

func (s *failLastStore) Series(context.Context, *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	return &logproto.SeriesResponse{}, nil
}

func requireAllClosed(t *testing.T, s *failLastStore) {
	t.Helper()
	require.Len(t, s.opened, s.tenants-1)
	for i, c := range s.opened {
		require.True(t, c.closed, "iterator %d opened before the failing tenant is closed", i)
	}
}

// TestSelect_ClosesOpenedIteratorsOnError: when one tenant's select fails, the
// iterators already opened for the other tenants are closed (each holds a
// ClickHouse cursor).
func TestSelect_ClosesOpenedIteratorsOnError(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "a|b|c")
	now := time.Now()

	t.Run("logs", func(t *testing.T) {
		s := &failLastStore{tenants: 3}
		expr, err := syntax.ParseLogSelector(`{app="x"}`, true)
		require.NoError(t, err)
		_, err = (&multitenant.Querier{Next: s}).SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
			Selector: expr.String(), Start: now.Add(-time.Hour), End: now, Direction: logproto.FORWARD, Plan: &plan.QueryPlan{AST: expr},
		}})
		require.ErrorIs(t, err, errLastTenant)
		requireAllClosed(t, s)
	})

	t.Run("samples", func(t *testing.T) {
		s := &failLastStore{tenants: 3}
		expr, err := syntax.ParseSampleExpr(`count_over_time({app="x"}[1m])`)
		require.NoError(t, err)
		_, err = (&multitenant.Querier{Next: s}).SelectSamples(ctx, logql.SelectSampleParams{SampleQueryRequest: &logproto.SampleQueryRequest{
			Selector: expr.String(), Start: now.Add(-time.Hour), End: now, Plan: &plan.QueryPlan{AST: expr},
		}})
		require.ErrorIs(t, err, errLastTenant)
		requireAllClosed(t, s)
	})
}
