package chstore

import (
	"context"
	"slices"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// streamSpeculations counts log queries by the outcome of the speculative
// read. It is a package-level collector, as AGENTS.md allows.
var streamSpeculations = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "lokihouse_query_stream_speculations_total",
	Help: "Log queries by outcome of the speculative read on a cached stream set.",
}, []string{"result"})

type resolvedStreams struct {
	streams []Stream
	err     error
}

// speculateLogs starts the first log read on the cached stream set while the
// stream lookup runs, then keeps the read only if the fresh set has the same
// stream ids (equal ids mean equal labels: StreamIdentity). The fresh lookup
// validates every answer, so the cache never affects results.
func (q *Querier) speculateLogs(ctx context.Context, sel syntax.LogSelectorExpr, p logql.SelectLogParams, pipeline log.Pipeline, tenantID, key string, cached []Stream) (iter.EntryIterator, error) {
	ms := sel.Matchers()
	fresh := make(chan resolvedStreams, 1)
	go func() {
		s, err := ResolveStreams(ctx, q.Conn, tenantID, ms, p.Start, p.End)
		fresh <- resolvedStreams{s, err}
	}()
	it, specErr := q.logIter(ctx, sel, p, pipeline, tenantID, cached, true)
	r := <-fresh // always received: the goroutine never outlives the call
	switch {
	case r.err != nil:
		closeIter(it)
		return iter.NoopEntryIterator, r.err
	case slices.Equal(sortedIDs(r.streams), sortedIDs(cached)):
		streamSpeculations.WithLabelValues("hit").Inc()
		return it, specErr
	default:
		closeIter(it)
		streamSpeculations.WithLabelValues("mismatch").Inc()
		q.StreamCache.put(key, r.streams)
		return q.logIter(ctx, sel, p, pipeline, tenantID, r.streams, false)
	}
}

// closeIter stops a discarded speculative iterator, which cancels its query.
func closeIter(it iter.EntryIterator) {
	if it != nil {
		_ = it.Close() // best effort: the iterator is being thrown away
	}
}
