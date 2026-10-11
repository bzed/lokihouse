package tail_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	lt "github.com/bzed/lokihouse/internal/tail"
)

var t0 = time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)

// countingConn wraps a driver.Conn and counts SELECT queries issued through it.
type countingConn struct {
	driver.Conn
	mu    sync.Mutex
	count int
}

func (c *countingConn) Query(ctx context.Context, q string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
	return c.Conn.Query(ctx, q, args...)
}

func (c *countingConn) queries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// newIngester builds an Ingester over a fresh database. The returned conn is
// the counting wrapper actually used by the Ingester, so tests can count
// ClickHouse queries.
func newIngester(t *testing.T, poll, look time.Duration) (*lt.Ingester, *countingConn) {
	conn, _ := chtest.New(t)
	cc := &countingConn{Conn: conn}
	q := &chstore.Querier{Conn: cc}
	return &lt.Ingester{Q: q, Poll: poll, Lookback: look}, cc
}

// write inserts lines into the {app="t"} stream and returns its entries.
func write(t *testing.T, conn driver.Conn, entries []logproto.Entry) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{{
		Labels:  `{app="t"}`,
		Entries: entries,
	}}, nil))
}

func tailReq(query string, start time.Time) *logproto.TailRequest {
	return &logproto.TailRequest{Query: query, Start: start}
}

func orgCtx() context.Context {
	return user.InjectOrgID(context.Background(), "t1")
}

func TestTail_DeliversNewEntries(t *testing.T) {
	i, cc := newIngester(t, 20*time.Millisecond, 2*time.Second)
	now := time.Now().UTC().Truncate(time.Second)
	start := now // tail starts at "now"; entries land in the next second

	ctx, cancel := context.WithCancel(orgCtx())
	defer cancel()
	clients, err := i.Tail(ctx, tailReq(`{app="t"}`, start))
	require.NoError(t, err)
	require.Len(t, clients, 1)
	c := clients["clickhouse"]

	// Write 3 entries just after the tail start.
	base := start.Add(50 * time.Millisecond)
	write(t, cc, []logproto.Entry{
		{Timestamp: base, Line: "a"},
		{Timestamp: base.Add(time.Millisecond), Line: "b"},
		{Timestamp: base.Add(2 * time.Millisecond), Line: "c"},
	})

	got := collect(t, c, 3, 10*time.Second)
	require.Len(t, got, 3)
	// All delivered entries carry the {app="t"} stream labels.
	for _, e := range got {
		require.Equal(t, `{app="t"}`, e.Labels)
	}
	lines := map[string]bool{}
	for _, e := range got {
		lines[e.Line] = true
	}
	require.True(t, lines["a"] && lines["b"] && lines["c"])
}

// recvResp blocks up to timeout for one TailResponse, returning the error if
// none arrives (so a bounded window can assert "no entry").
func recvResp(c logproto.Querier_TailClient, timeout time.Duration) (*logproto.TailResponse, error) {
	type res struct {
		r   *logproto.TailResponse
		err error
	}
	ch := make(chan res, 1)
	go func() {
		r, err := c.Recv()
		ch <- res{r, err}
	}()
	select {
	case x := <-ch:
		return x.r, x.err
	case <-time.After(timeout):
		return nil, context.DeadlineExceeded
	}
}

// collect receives entries from c until want entries arrive or the deadline.
func collect(t *testing.T, c logproto.Querier_TailClient, want int, timeout time.Duration) []tailEntry {
	t.Helper()
	var out []tailEntry
	deadline := time.Now().Add(timeout)
	for len(out) < want && time.Now().Before(deadline) {
		resp, err := recvResp(c, deadline.Sub(time.Now()))
		if err != nil {
			if len(out) == 0 {
				t.Fatalf("Recv: %v", err)
			}
			break
		}
		s := resp.Stream
		if s == nil {
			continue
		}
		for _, e := range s.Entries {
			out = append(out, tailEntry{Labels: s.Labels, TS: e.Timestamp, Line: e.Line})
		}
	}
	return out
}

type tailEntry struct {
	Labels string
	TS     time.Time
	Line   string
}

func TestTail_LateEntryWithinLookback(t *testing.T) {
	i, cc := newIngester(t, 50*time.Millisecond, 5*time.Second)
	start := time.Now().UTC().Truncate(time.Second)
	ctx, cancel := context.WithCancel(orgCtx())
	defer cancel()
	clients, err := i.Tail(ctx, tailReq(`{app="t"}`, start))
	require.NoError(t, err)
	c := clients["clickhouse"]

	// A arrives at +2s.
	a := start.Add(2 * time.Second)
	write(t, cc, []logproto.Entry{{Timestamp: a, Line: "A"}})
	got := collect(t, c, 1, 10*time.Second)
	require.Equal(t, "A", got[0].Line)

	// B is older than A but inside the lookback window.
	b := start.Add(1 * time.Second)
	write(t, cc, []logproto.Entry{{Timestamp: b, Line: "B"}})
	got2 := collect(t, c, 1, 10*time.Second)
	require.Equal(t, "B", got2[0].Line)

	// Over the next ~1.5s nothing may arrive again (no duplicate of A or B).
	_, err = recvResp(c, 1500*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded, "no further entries within the lookback-free window")
}

func TestTail_NoHistoryDuplicates(t *testing.T) {
	i, cc := newIngester(t, 20*time.Millisecond, 5*time.Second)
	start := time.Now().UTC().Truncate(time.Second)

	// History: written before the tail starts, ts < start.
	write(t, cc, []logproto.Entry{{Timestamp: start.Add(-1 * time.Hour), Line: "history"}})

	ctx, cancel := context.WithCancel(orgCtx())
	defer cancel()
	clients, err := i.Tail(ctx, tailReq(`{app="t"}`, start))
	require.NoError(t, err)
	c := clients["clickhouse"]

	// The live client must not send the history entry. Wait a bit, expect nothing.
	_, err = recvResp(c, 600*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded, "history entry must not be sent by the live client")
}

func TestTail_CloseStopsPolling(t *testing.T) {
	i, cc := newIngester(t, 20*time.Millisecond, 5*time.Second)
	start := time.Now().UTC().Truncate(time.Second)
	ctx, cancel := context.WithCancel(orgCtx())
	defer cancel() // the test closes the client via CloseSend; this only releases ctx
	clients, err := i.Tail(ctx, tailReq(`{app="t"}`, start))
	require.NoError(t, err)
	c := clients["clickhouse"]

	// The client polls inside Recv. Write entries and drive Recv so a few
	// real polls happen (each Recv triggers a poll; the seen-set prevents
	// duplicates, so later Recvs block until cancelled).
	write(t, cc, []logproto.Entry{
		{Timestamp: start.Add(20 * time.Millisecond), Line: "p1"},
		{Timestamp: start.Add(40 * time.Millisecond), Line: "p2"},
	})
	recvResp(c, 5*time.Second) // consumes at least one poll's output
	got := cc.queries()
	require.Greater(t, got, 0)

	// Canceling the client stops it.
	require.NoError(t, c.CloseSend())
	_, err = c.Recv()
	require.Error(t, err, "Recv must error after CloseSend")

	// TailersCount drops to zero (the decrement runs in a goroutine).
	require.Eventually(t, func() bool {
		n, e := i.TailersCount(context.Background())
		return e == nil && n[0] == 0
	}, 2*time.Second, 20*time.Millisecond)

	// No further ClickHouse queries after the close.
	require.Eventually(t, func() bool {
		// Give any in-flight poll a moment to finish, then the count must freeze.
		return cc.queries() <= got
	}, 2*time.Second, 20*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	require.LessOrEqual(t, cc.queries(), got+1, "no sustained polls after the client is closed")
}

// An entry stamped in the future (Loki accepts up to creation_grace_period
// ahead) must not hide entries with current timestamps that arrive after it.
func TestTail_FutureEntryDoesNotHideCurrent(t *testing.T) {
	i, cc := newIngester(t, 20*time.Millisecond, 2*time.Second)
	start := time.Now().UTC()
	ctx, cancel := context.WithCancel(orgCtx())
	defer cancel()
	clients, err := i.Tail(ctx, tailReq(`{app="t"}`, start))
	require.NoError(t, err)
	c := clients["clickhouse"]

	write(t, cc, []logproto.Entry{{Timestamp: start.Add(5 * time.Minute), Line: "future"}})
	require.Equal(t, "future", collect(t, c, 1, 10*time.Second)[0].Line)

	write(t, cc, []logproto.Entry{{Timestamp: time.Now().UTC(), Line: "current"}})
	require.Equal(t, "current", collect(t, c, 1, 10*time.Second)[0].Line)
}
