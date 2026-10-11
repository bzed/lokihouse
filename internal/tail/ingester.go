// Package tail adapts ClickHouse to Loki's tail.Querier: one in-process
// "ingester" whose tail client polls for new rows.
package tail

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"google.golang.org/grpc/metadata"

	"github.com/bzed/lokihouse/internal/chstore"
)

const addr = "clickhouse"

// Ingester implements tail.Ingester with a single in-process "ingester". Its
// tail client polls ClickHouse for rows newer than the previous poll (minus a
// lookback for late arrivals) and deduplicates them.
type Ingester struct {
	Q        *chstore.Querier
	Poll     time.Duration // default 1s
	Lookback time.Duration // default 10s

	mu     sync.Mutex
	active int
}

func (i *Ingester) Tail(ctx context.Context, req *logproto.TailRequest) (map[string]logproto.Querier_TailClient, error) {
	poll, look := i.Poll, i.Lookback
	if poll == 0 {
		poll = time.Second
	}
	if look == 0 {
		look = 10 * time.Second
	}
	sel, err := syntax.ParseLogSelector(req.Query, true)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &client{
		ctx:    ctx,
		cancel: cancel,
		i:      i,
		req:    req,
		poll:   poll,
		look:   look,
		start:  req.Start,
		sel:    sel,
		plan:   &plan.QueryPlan{AST: sel},
		seen:   map[seenKey]int64{},
	}
	i.addActive(1)
	go func() {
		<-ctx.Done()
		i.addActive(-1)
	}()
	return map[string]logproto.Querier_TailClient{addr: c}, nil
}

func (i *Ingester) addActive(n int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.active += n
}

func (i *Ingester) TailDisconnectedIngesters(ctx context.Context, req *logproto.TailRequest, connected []string) (map[string]logproto.Querier_TailClient, error) {
	if slices.Contains(connected, addr) {
		return map[string]logproto.Querier_TailClient{}, nil
	}
	return i.Tail(ctx, req)
}

func (i *Ingester) TailersCount(context.Context) ([]uint32, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return []uint32{uint32(i.active)}, nil
}

type seenKey struct {
	labels string
	ts     int64
	line   string
}

// client is a logproto.Querier_TailClient. Each poll reads rows with
// ts > max(start, now-lookback), skips those already sent (seen-set),
// and forgets sent rows once they are older than the window. It is
// single-consumer: Loki's Tailer drives each client from exactly one
// readTailClient goroutine, and all state is guarded by mu so a second
// consumer could never corrupt it.
type client struct {
	ctx    context.Context
	cancel context.CancelFunc
	i      *Ingester
	req    *logproto.TailRequest
	poll   time.Duration
	look   time.Duration
	start  time.Time
	sel    syntax.LogSelectorExpr
	plan   *plan.QueryPlan

	mu      sync.Mutex
	seen    map[seenKey]int64
	pending []*logproto.TailResponse
}

func (c *client) Recv() (*logproto.TailResponse, error) {
	// With a bounded context (e.g. a test deadline) use a short tick so the
	// deadline is honored promptly instead of waiting a full poll interval.
	step := c.poll
	if _, ok := c.ctx.Deadline(); ok {
		step = min(step, 10*time.Millisecond)
	}
	t := time.NewTicker(step)
	defer t.Stop()
	for {
		c.mu.Lock()
		if n := len(c.pending); n > 0 {
			r := c.pending[0]
			c.pending = c.pending[1:]
			c.mu.Unlock()
			return r, nil
		}
		c.mu.Unlock()
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-t.C:
		}
		if err := c.pollOnce(time.Now()); err != nil {
			return nil, err
		}
	}
}

// pollOnce reads the window (lower, now+24h] and enqueues every row not yet
// seen. lower is max(start, now-lookback): the lookback catches late arrivals.
// It follows the wall clock, not the newest entry sent, so an entry stamped in
// the future cannot hide current ones.
func (c *client) pollOnce(now time.Time) error {
	lower := now.Add(-c.look)
	if lower.Before(c.start) {
		lower = c.start
	}
	it, err := c.i.Q.SelectLogs(c.ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: c.req.Query,
		Start:    lower.Add(time.Nanosecond).UTC(),
		End:      now.Add(24 * time.Hour).UTC(),
		Plan:     c.plan,
	}})
	if err != nil {
		return err
	}
	defer it.Close()
	byLabels := map[string]*logproto.Stream{}
	var order []string
	for it.Next() {
		e := it.At()
		k := seenKey{it.Labels(), e.Timestamp.UnixNano(), e.Line}
		if _, ok := c.seen[k]; ok {
			continue
		}
		c.seen[k] = k.ts
		s, ok := byLabels[k.labels]
		if !ok {
			s = &logproto.Stream{Labels: k.labels}
			byLabels[k.labels] = s
			order = append(order, k.labels)
		}
		s.Entries = append(s.Entries, e)
	}
	if err := it.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	for k, ts := range c.seen {
		if ts <= lower.UnixNano() {
			delete(c.seen, k)
		}
	}
	for _, l := range order {
		c.pending = append(c.pending, &logproto.TailResponse{Stream: byLabels[l]})
	}
	c.mu.Unlock()
	return nil
}

func (c *client) Header() (metadata.MD, error) { return nil, nil }
func (c *client) Trailer() metadata.MD         { return nil }
func (c *client) CloseSend() error             { c.cancel(); return nil }
func (c *client) Context() context.Context     { return c.ctx }
func (c *client) SendMsg(any) error            { return nil }
func (c *client) RecvMsg(any) error            { return nil }
