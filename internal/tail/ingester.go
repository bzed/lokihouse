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
	ctx, cancel := context.WithCancel(ctx)
	c := &client{
		ctx:    ctx,
		cancel: cancel,
		i:      i,
		req:    req,
		poll:   poll,
		look:   look,
		start:  req.Start,
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
// ts > max(start, pollTime-lookback), skips those already sent, and forgets
// sent rows once they are older than the window.
type client struct {
	ctx     context.Context
	cancel  context.CancelFunc
	i       *Ingester
	req     *logproto.TailRequest
	poll    time.Duration
	look    time.Duration
	start   time.Time
	seen    map[seenKey]int64
	pending []*logproto.TailResponse
}

func (c *client) Recv() (*logproto.TailResponse, error) {
	// If the caller passed a bounded context (e.g. a test deadline), respect it:
	// poll at most once, then block on a short tick so the deadline can fire.
	// Otherwise poll every c.poll until an entry arrives.
	if d, ok := c.ctx.Deadline(); ok && time.Until(d) <= c.poll {
		if err := c.pollOnce(time.Now()); err != nil {
			return nil, err
		}
	}
	step := c.poll
	if _, ok := c.ctx.Deadline(); ok {
		step = 10 * time.Millisecond
	}
	t := time.NewTicker(step)
	defer t.Stop()
	for len(c.pending) == 0 {
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-t.C:
		}
		if err := c.pollOnce(time.Now()); err != nil {
			return nil, err
		}
	}
	r := c.pending[0]
	c.pending = c.pending[1:]
	return r, nil
}

func (c *client) pollOnce(now time.Time) error {
	lower := now.Add(-c.look)
	if lower.Before(c.start) {
		lower = c.start
	}
	sel, err := syntax.ParseLogSelector(c.req.Query, true)
	if err != nil {
		return err
	}
	it, err := c.i.Q.SelectLogs(c.ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: c.req.Query,
		Start:    lower.Add(time.Nanosecond).UTC(),
		End:      now.Add(24 * time.Hour).UTC(),
		Plan:     &plan.QueryPlan{AST: sel},
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
	for k, ts := range c.seen {
		if ts <= lower.UnixNano() {
			delete(c.seen, k)
		}
	}
	for _, l := range order {
		c.pending = append(c.pending, &logproto.TailResponse{Stream: byLabels[l]})
	}
	return nil
}

func (c *client) Header() (metadata.MD, error) { return nil, nil }
func (c *client) Trailer() metadata.MD         { return nil }
func (c *client) CloseSend() error             { c.cancel(); return nil }
func (c *client) Context() context.Context     { return c.ctx }
func (c *client) SendMsg(any) error            { return nil }
func (c *client) RecvMsg(any) error            { return nil }
