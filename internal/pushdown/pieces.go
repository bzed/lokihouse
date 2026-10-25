package pushdown

import (
	"context"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/util/validation"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// NewConnBudget returns the process-wide budget of ClickHouse connections that
// parallel pieces may reserve: half of the pool, and never less than one.
func NewConnBudget(maxOpenConns int) *semaphore.Weighted {
	return semaphore.NewWeighted(int64(max(1, maxOpenConns/2)))
}

type parallelismLimits interface {
	MaxQueryParallelism(ctx context.Context, userID string) int
}

type requestSlotsKey struct{}

// withRequestSlots installs the request's slot semaphore, sized by the
// tenants' max_query_parallelism, unless the context already has one. All the
// splitters of one request (both sides of a binary operation, nested queries)
// then share the same slots.
func withRequestSlots(ctx context.Context, l parallelismLimits) context.Context {
	if requestSlots(ctx) != nil {
		return ctx
	}
	tenants, _ := tenant.TenantIDs(ctx) // no tenant: the limits' default applies
	n := validation.SmallestPositiveIntPerTenant(tenants, func(id string) int {
		return l.MaxQueryParallelism(ctx, id)
	})
	return context.WithValue(ctx, requestSlotsKey{}, semaphore.NewWeighted(int64(max(0, n))))
}

// requestSlots returns the request's slot semaphore, or nil.
func requestSlots(ctx context.Context) *semaphore.Weighted {
	s, _ := ctx.Value(requestSlotsKey{}).(*semaphore.Weighted)
	return s
}

// runPieces runs run(ctx, i) for i in [0, n) in order and returns after every
// one has finished. A piece runs on its own goroutine when one request slot
// and need units of budget are free right now; otherwise the calling
// goroutine runs it inline. Nothing ever waits on a semaphore, so a piece
// that holds connections never waits for another piece's reservation: the
// only waits are on ClickHouse connections that running pieces hold and
// release. A nil slots or budget means every piece runs inline. The first
// error cancels the context passed to the pieces and is returned.
func runPieces(ctx context.Context, n int, need int64, slots, budget *semaphore.Weighted, run func(ctx context.Context, i int) error) error {
	g, gctx := errgroup.WithContext(ctx)
	stopped := false
	for i := range n {
		if gctx.Err() != nil {
			stopped = true
			break
		}
		if slots != nil && budget != nil && slots.TryAcquire(1) {
			if budget.TryAcquire(need) {
				g.Go(func() error {
					defer slots.Release(1)
					defer budget.Release(need)
					return run(gctx, i)
				})
				continue
			}
			slots.Release(1)
		}
		if err := run(gctx, i); err != nil {
			g.Go(func() error { return err }) // cancels gctx and is reported by Wait
			stopped = true
			break
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if stopped {
		return ctx.Err()
	}
	return nil
}
