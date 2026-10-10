package api

import (
	"net/http"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"

	"github.com/bzed/lokihouse/internal/chstore"
)

type API struct {
	Engine      *logql.QueryEngine
	Q           *chstore.Querier
	Push        http.Handler
	AuthEnabled bool
}

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
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /loki/api/v1/status/buildinfo", buildInfo)
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
