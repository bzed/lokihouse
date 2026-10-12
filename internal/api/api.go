// Package api provides the HTTP API handlers for lokihouse, serving Loki's
// endpoints (push, query, series, labels, tail, deletes, rules) over ClickHouse.
package api

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/patterns"
	"github.com/bzed/lokihouse/internal/pushdown"
)

// API is the lokihouse HTTP API server. It wires together the query, push,
// delete, tail and ruler handlers into a single http.Handler.
type API struct {
	// Queries executes LogQL queries, choosing between the plain engine and
	// the push-down engine.
	Queries Querier
	Q       *chstore.Querier
	// Meta answers label/series for the metadata endpoints. It is nil unless
	// multi-tenant queries are enabled; then it is the multi-tenant querier.
	Meta MetaQuerier
	Push http.Handler
	// Limits configures per-tenant limits.
	Limits      *validation.Overrides
	AuthEnabled bool
	Patterns    *patterns.Querier
	Deletes     *deletes.Handler
	Tail        http.Handler
	Rules       *rulerbase.API // Loki's ruler API; nil = ruler disabled
}

// Querier executes a LogQL query. Both the plain *logql.QueryEngine and the
// *pushdown.Engine satisfy it via the adapters below.
type Querier interface {
	Query(ctx context.Context, p logql.Params) logql.Query
}

// MetaQuerier is what the metadata endpoints call: label names/values and
// series. The plain *chstore.Querier and the multi-tenant querier both
// satisfy it.
type MetaQuerier interface {
	Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)
	Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error)
}

// plainQuerier adapts *logql.QueryEngine (Query(params)) to Querier (Query(ctx, params)).
type plainQuerier struct{ e *logql.QueryEngine }

func (p plainQuerier) Query(_ context.Context, params logql.Params) logql.Query {
	return p.e.Query(params)
}

// NewPlainQuerier adapts a *logql.QueryEngine to the api.Querier interface.
func NewPlainQuerier(e *logql.QueryEngine) Querier { return plainQuerier{e: e} }

// Router returns the http.Handler that serves all lokihouse API endpoints.
func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	t := a.tenant
	mux.Handle("POST /loki/api/v1/push", t(a.Push))
	mux.Handle("POST /api/prom/push", t(a.Push))
	mux.Handle("POST /otlp/v1/logs", t(a.Push))
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/query", t(http.HandlerFunc(a.instantQuery)))
		mux.Handle(m+" /loki/api/v1/query_range", t(http.HandlerFunc(a.rangeQuery)))
		mux.Handle(m+" /loki/api/v1/series", t(http.HandlerFunc(a.series)))
	}
	mux.Handle("GET /loki/api/v1/labels", t(http.HandlerFunc(a.labels)))
	mux.Handle("GET /loki/api/v1/label/{name}/values", t(http.HandlerFunc(a.labelValues)))
	if a.Deletes != nil {
		g := func(h http.HandlerFunc) http.Handler { return t(a.Deletes.Gate(h)) }
		mux.Handle("PUT /loki/api/v1/delete", g(a.Deletes.Add))
		mux.Handle("POST /loki/api/v1/delete", g(a.Deletes.Add))
		mux.Handle("GET /loki/api/v1/delete", g(a.Deletes.List))
		mux.Handle("DELETE /loki/api/v1/delete", g(a.Deletes.Cancel))
	}
	if a.Tail != nil {
		for _, m := range []string{"GET", "POST"} {
			mux.Handle(m+" /loki/api/v1/tail", t(a.Tail))
			mux.Handle(m+" /api/prom/tail", t(a.Tail))
		}
	}
	if a.Rules != nil {
		a.ruleRoutes(mux)
	}
	a.lokiRoutes(mux)
	// format_query is not tenant-wrapped: Loki registers it without auth.
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/format_query", FormatQueryHandler())
	}
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /loki/api/v1/status/buildinfo", buildInfo)

	// Debug endpoint: Tier A leaf counter (only when LOKIHOUSE_DEBUG=1)
	if os.Getenv("LOKIHOUSE_DEBUG") == "1" {
		mux.HandleFunc("GET /debug/lokihouse/tiera_leaves", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strconv.FormatInt(pushdown.TierALeaves.Load(), 10)))
		})
	}

	if a.Rules != nil {
		return decodedRulePaths(mux)
	}
	return mux
}

func (a *API) tenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Scope-OrgID")
		if id == "" {
			if a.AuthEnabled {
				http.Error(w, "no org id", http.StatusUnauthorized)
				return
			}
			id = "fake"
		}
		next.ServeHTTP(w, r.WithContext(user.InjectOrgID(r.Context(), id)))
	})
}

func writeError(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}
