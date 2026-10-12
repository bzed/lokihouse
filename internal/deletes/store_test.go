package deletes_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/deletes"
)

var t0 = time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)

func newStore(t *testing.T) *deletes.Store {
	conn, _ := chtest.New(t)
	return &deletes.Store{Conn: conn, Now: func() time.Time { return t0 }}
}

func TestStore_AddThenList(t *testing.T) {
	s := newStore(t)
	id, err := s.Add(context.Background(), "t1", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)
	require.NotEmpty(t, id)

	reqs, err := s.GetAllDeleteRequestsForUser(context.Background(), "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	r := reqs[0]
	require.Equal(t, id, r.RequestID)
	require.Equal(t, `{app="x"}`, r.Query)
	require.Equal(t, model.TimeFromUnixNano(t0.UnixNano()), r.StartTime)
	require.Equal(t, model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()), r.EndTime)
	require.Equal(t, model.TimeFromUnixNano(t0.UnixNano()), r.CreatedAt)
	require.Equal(t, deletionproto.StatusReceived, r.Status)
}

func TestStore_CancelHidesRequest(t *testing.T) {
	s := newStore(t)
	id, err := s.Add(context.Background(), "t1", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)

	require.NoError(t, s.Cancel(context.Background(), "t1", id))

	reqs, err := s.GetAllDeleteRequestsForUser(context.Background(), "t1", false, nil)
	require.NoError(t, err)
	require.Empty(t, reqs)
	_, err = s.Get(context.Background(), "t1", id)
	require.ErrorIs(t, err, deletion.ErrDeleteRequestNotFound)
}

func TestStore_MarkProcessed(t *testing.T) {
	s := newStore(t)
	id, err := s.Add(context.Background(), "t1", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)

	r, err := s.Get(context.Background(), "t1", id)
	require.NoError(t, err)
	require.NoError(t, s.MarkProcessed(context.Background(), r))

	// In the full (non query-time) list it stays, now processed.
	full, err := s.GetAllDeleteRequestsForUser(context.Background(), "t1", false, nil)
	require.NoError(t, err)
	require.Len(t, full, 1)
	require.Equal(t, deletionproto.StatusProcessed, full[0].Status)
	// The query-time list only has received requests.
	qt, err := s.GetAllDeleteRequestsForUser(context.Background(), "t1", true, nil)
	require.NoError(t, err)
	require.Empty(t, qt)
	// And it is no longer unprocessed.
	un, err := s.Unprocessed(context.Background())
	require.NoError(t, err)
	require.Empty(t, un)
}

func TestStore_TimeRangeFilter(t *testing.T) {
	s := newStore(t)
	_, err := s.Add(context.Background(), "t1", `{app="a"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)
	_, err = s.Add(context.Background(), "t1", `{app="b"}`, model.TimeFromUnixNano(t0.Add(10*time.Minute).UnixNano()), model.TimeFromUnixNano(t0.Add(11*time.Minute).UnixNano()))
	require.NoError(t, err)

	// A range overlapping only the first request.
	reqs, err := s.GetAllDeleteRequestsForUser(context.Background(), "t1", true, &deletion.TimeRange{
		Start: model.TimeFromUnixNano(t0.Add(-time.Minute).UnixNano()),
		End:   model.TimeFromUnixNano(t0.Add(2 * time.Minute).UnixNano()),
	})
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	require.Equal(t, `{app="a"}`, reqs[0].Query)
}

func TestStore_TenantIsolation(t *testing.T) {
	s := newStore(t)
	id, err := s.Add(context.Background(), "a", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)

	// Tenant b sees nothing.
	reqs, err := s.GetAllDeleteRequestsForUser(context.Background(), "b", false, nil)
	require.NoError(t, err)
	require.Empty(t, reqs)
	_, err = s.Get(context.Background(), "b", id)
	require.ErrorIs(t, err, deletion.ErrDeleteRequestNotFound)
	require.Equal(t, deletion.ErrDeleteRequestNotFound, s.Cancel(context.Background(), "b", id))
}

// With a clock that does not advance (or steps back), a later write must still
// supersede an earlier one: the ReplacingMergeTree version may never tie.
func TestStore_LaterWriteWinsUnderFrozenClock(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for range 40 {
		_, err := s.Add(ctx, "t1", `{app="x"}`, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
		require.NoError(t, err)
	}
	pending, err := s.Unprocessed(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 40)
	for _, r := range pending {
		require.NoError(t, s.MarkProcessed(ctx, r))
	}
	pending, err = s.Unprocessed(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "every request was marked processed after it was added")
}
