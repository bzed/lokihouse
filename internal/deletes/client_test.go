package deletes_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/deletes"
)

// modeLimits maps a tenant to its deletion_mode; "" means filter-and-delete.
type modeLimits map[string]string

func (m modeLimits) DeletionMode(u string) string {
	if v := m[u]; v != "" {
		return v
	}
	return "filter-and-delete"
}
func (modeLimits) RetentionPeriod(string) time.Duration                { return 0 }
func (modeLimits) StreamRetention(string) []validation.StreamRetention { return nil }

func newQueryClient(t *testing.T, s *deletes.Store, l modeLimits, opts ...deletion.DeleteRequestsStoreOption) deletion.DeleteRequestsClient {
	t.Helper()
	c := deletes.NewQueryClient(s, l, deletion.NewDeleteRequestClientMetrics(prometheus.NewRegistry()), opts...)
	t.Cleanup(c.Stop)
	return c
}

func addRequest(t *testing.T, s *deletes.Store, tenant, query string) {
	t.Helper()
	_, err := s.Add(context.Background(), tenant, query, model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Minute).UnixNano()))
	require.NoError(t, err)
}

// The first lookup of a tenant reads the store; later ones are served from
// the cache until it refreshes, so a request added in between is not seen.
func TestQueryClient_CachesPerTenant(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	addRequest(t, s, "t1", `{app="x"}`)
	c := newQueryClient(t, s, modeLimits{})

	got, err := c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	addRequest(t, s, "t1", `{app="y"}`)
	got, err = c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1, "served from the cache")
}

// With a short refresh interval the cache picks up the new request.
func TestQueryClient_Refreshes(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	addRequest(t, s, "t1", `{app="x"}`)
	c := newQueryClient(t, s, modeLimits{}, deletion.WithRequestClientCacheDuration(100*time.Millisecond))

	got, err := c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	addRequest(t, s, "t1", `{app="y"}`)
	require.Eventually(t, func() bool {
		got, err := c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
		return err == nil && len(got) == 2
	}, 10*time.Second, 50*time.Millisecond)
}

func TestQueryClient_DisabledTenantFiltersNothing(t *testing.T) {
	s := newStore(t)
	addRequest(t, s, "t1", `{app="x"}`)
	c := newQueryClient(t, s, modeLimits{"t1": "disabled"})

	got, err := c.GetAllDeleteRequestsForUser(context.Background(), "t1", true, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

// A failed first lookup is an error, not an empty list cached for the
// refresh interval.
func TestQueryClient_ErrorIsNotCached(t *testing.T) {
	ctx := context.Background()
	conn, db := chtest.New(t)
	s := &deletes.Store{Conn: conn, Now: func() time.Time { return t0 }}
	addRequest(t, s, "t1", `{app="x"}`)
	bad, err := clickhouse.Open(chtest.Options(db))
	require.NoError(t, err)
	require.NoError(t, bad.Close())
	s.Conn = bad
	c := newQueryClient(t, s, modeLimits{})

	_, err = c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.Error(t, err)

	s.Conn = conn
	got, err := c.GetAllDeleteRequestsForUser(ctx, "t1", true, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestQueryClient_ConcurrentLookups(t *testing.T) {
	s := newStore(t)
	addRequest(t, s, "t1", `{app="x"}`)
	c := newQueryClient(t, s, modeLimits{})

	const n = 16
	errs := make([]error, n)
	lens := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.GetAllDeleteRequestsForUser(context.Background(), "t1", true, nil)
			errs[i], lens[i] = err, len(got)
		}()
	}
	wg.Wait()
	for i := range n {
		require.NoError(t, errs[i])
		require.Equal(t, 1, lens[i])
	}
}
