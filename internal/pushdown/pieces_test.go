package pushdown

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type inflight struct{ cur, max atomic.Int64 }

func (f *inflight) enter() {
	c := f.cur.Add(1)
	for {
		m := f.max.Load()
		if c <= m || f.max.CompareAndSwap(m, c) {
			return
		}
	}
}
func (f *inflight) leave() { f.cur.Add(-1) }

func TestNewConnBudget(t *testing.T) {
	b := NewConnBudget(100)
	require.True(t, b.TryAcquire(50))
	require.False(t, b.TryAcquire(1))
	for _, n := range []int{1, 0, -3} {
		b := NewConnBudget(n)
		require.True(t, b.TryAcquire(1), n)
		require.False(t, b.TryAcquire(1), n)
	}
}

func countRuns(t *testing.T, n int, need int64, slots, budget *semaphore.Weighted) (*inflight, []int32) {
	t.Helper()
	f := &inflight{}
	ran := make([]int32, n)
	err := runPieces(context.Background(), n, need, slots, budget, func(_ context.Context, i int) error {
		f.enter()
		defer f.leave()
		atomic.AddInt32(&ran[i], 1)
		return nil
	})
	require.NoError(t, err)
	for i, r := range ran {
		require.EqualValues(t, 1, r, "piece %d", i)
	}
	return f, ran
}

func TestRunPieces_ManyPiecesBoundedConcurrency(t *testing.T) {
	f, _ := countRuns(t, 10000, 2, semaphore.NewWeighted(4), semaphore.NewWeighted(100))
	require.LessOrEqual(t, f.max.Load(), int64(5))
}

func TestRunPieces_ExhaustedBudgetRunsInlineWithoutWaiting(t *testing.T) {
	budget := semaphore.NewWeighted(4)
	require.NoError(t, budget.Acquire(context.Background(), 4))
	f, _ := countRuns(t, 50, 2, semaphore.NewWeighted(32), budget)
	require.EqualValues(t, 1, f.max.Load())
}

func TestRunPieces_ZeroSlotsRunsInline(t *testing.T) {
	f, _ := countRuns(t, 20, 2, semaphore.NewWeighted(0), semaphore.NewWeighted(100))
	require.EqualValues(t, 1, f.max.Load())
}

func TestRunPieces_NeedAboveBudgetRunsInline(t *testing.T) {
	f, _ := countRuns(t, 20, 3, semaphore.NewWeighted(8), semaphore.NewWeighted(2))
	require.EqualValues(t, 1, f.max.Load())
}

func TestRunPieces_NilSemaphoresRunInline(t *testing.T) {
	f, _ := countRuns(t, 20, 1, nil, nil)
	require.EqualValues(t, 1, f.max.Load())
}

func TestRunPieces_FirstErrorCancelsRest(t *testing.T) {
	errBoom := errors.New("boom")
	var started atomic.Int64
	var sawCancel atomic.Int64
	const n = 4
	err := runPieces(context.Background(), n, 1, semaphore.NewWeighted(n), semaphore.NewWeighted(n),
		func(ctx context.Context, i int) error {
			if i == 0 {
				// Fail only after the other pieces are running.
				for started.Load() < n-1 {
					if ctx.Err() != nil {
						return ctx.Err()
					}
				}
				return errBoom
			}
			started.Add(1)
			<-ctx.Done()
			sawCancel.Add(1)
			return ctx.Err()
		})
	require.ErrorIs(t, err, errBoom)
	require.EqualValues(t, n-1, sawCancel.Load())
}

func TestRunPieces_CancelStopsAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- runPieces(ctx, 100, 1, semaphore.NewWeighted(3), semaphore.NewWeighted(3),
			func(ctx context.Context, _ int) error {
				started.Add(1)
				<-ctx.Done()
				return ctx.Err()
			})
	}()
	require.Eventually(t, func() bool { return started.Load() >= 3 }, 10*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type fixedParallelism int

func (f fixedParallelism) MaxQueryParallelism(context.Context, string) int { return int(f) }

func TestWithRequestSlots(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "t1")
	c := withRequestSlots(ctx, fixedParallelism(3))
	s := requestSlots(c)
	require.NotNil(t, s)
	require.True(t, s.TryAcquire(3))
	require.False(t, s.TryAcquire(1))
	require.Same(t, s, requestSlots(withRequestSlots(c, fixedParallelism(9))))
	require.Nil(t, requestSlots(ctx))

	z := requestSlots(withRequestSlots(ctx, fixedParallelism(0)))
	require.False(t, z.TryAcquire(1))
}
