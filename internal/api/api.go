package api

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/patterns"
	"github.com/bzed/lokihouse/internal/pushdown"
)

type API struct {
	// Queries executes LogQL queries, choosing between the plain engine and
	// the push-down engine.
	Queries     Querier
	Q           *chstore.Querier
	Push        http.Handler
	Limits      *validation.Overrides
	AuthEnabled bool
	Patterns    *patterns.Querier
	Deletes     *deletes.Handler
	Tail        http.Handler
}

// Querier executes a LogQL query. Both the plain *logql.QueryEngine and the
// *pushdown.Engine satisfy it via the adapters below.
type Querier interface {
	Query(ctx context.Context, p logql.Params) logql.Query
}

// plainQuerier adapts *logql.QueryEngine (Query(params)) to Querier (Query(ctx, params)).
type plainQuerier struct{ e *logql.QueryEngine }

func (p plainQuerier) Query(_ context.Context, params logql.Params) logql.Query {
	return p.e.Query(params)
}

// NewPlainQuerier adapts a *logql.QueryEngine to the api.Querier interface.
func NewPlainQuerier(e *logql.QueryEngine) Querier { return plainQuerier{e: e} }

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
	a.lokiRoutes(mux)
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /loki/api/v1/status/buildinfo", buildInfo)

	// Debug endpoint: Tier A leaf counter (only when LOKIHOUSE_DEBUG=1)
	if os.Getenv("LOKIHOUSE_DEBUG") == "1" {
		mux.HandleFunc("GET /debug/lokihouse/tiera_leaves", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strconv.FormatInt(pushdown.TierALeaves.Load(), 10)))
		})
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

// statusFor reuses Loki's exact error -> status mapping.
func statusFor(err error) int {
	code, _ := serverutil.ClientHTTPStatusAndError(err)
	return code
}
