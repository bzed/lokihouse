package api

import (
	"net/http"
	"strings"

	gmux "github.com/gorilla/mux"
	"github.com/grafana/loki/v3/pkg/util/constants"
)

// ruleRoutes registers Loki's ruler API as pkg/loki/modules.go initRuler does:
// the Prometheus-compatible rules and alerts endpoints, the Loki rule-group
// routes and the legacy /api/prom ones, all tenant-wrapped. Loki's handlers
// read {namespace} and {groupName} with gorilla's mux.Vars.
func (a *API) ruleRoutes(mux *http.ServeMux) {
	h := func(f http.HandlerFunc) http.Handler {
		return a.tenant(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			vars := map[string]string{}
			for _, k := range []string{"namespace", "groupName"} {
				if v := r.PathValue(k); v != "" {
					vars[k] = v
				}
			}
			f(w, gmux.SetURLVars(r, vars))
		}))
	}
	mux.Handle("GET "+constants.PathPrometheusRules, h(a.Rules.PrometheusRules))
	mux.Handle("GET "+constants.PathPrometheusAlerts, h(a.Rules.PrometheusAlerts))
	for _, p := range []struct{ rules, ns, group string }{
		{constants.PathLokiRules, constants.PathLokiRulesNamespace, constants.PathLokiRulesNamespaceGroup},
		{constants.PathPromRules, constants.PathPromRulesNamespace, constants.PathPromRulesNamespaceGroup},
	} {
		mux.Handle("GET "+p.rules, h(a.Rules.ListRules))
		mux.Handle("GET "+p.ns, h(a.Rules.ListRules))
		mux.Handle("POST "+p.ns, h(a.Rules.CreateRuleGroup))
		mux.Handle("DELETE "+p.ns, h(a.Rules.DeleteNamespace))
		mux.Handle("GET "+p.group, h(a.Rules.GetRuleGroup))
		mux.Handle("DELETE "+p.group, h(a.Rules.DeleteRuleGroup))
	}
}

// decodedRulePaths makes the rules API route on the decoded URL path, as
// Loki's gorilla router does (Loki does not set UseEncodedPath): an encoded
// slash separates the namespace from the group, so /rules/ns%2Fg1 is group g1
// in namespace ns. ServeMux would otherwise match the escaped path and keep
// "ns/g1" as one namespace. Loki's handlers unescape the variables once more.
func decodedRulePaths(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawPath != "" &&
			(strings.HasPrefix(r.URL.Path, constants.PathLokiRules+"/") || strings.HasPrefix(r.URL.Path, constants.PathPromRules+"/")) {
			u := *r.URL
			u.RawPath = ""
			r2 := *r
			r2.URL = &u
			r = &r2
		}
		next.ServeHTTP(w, r)
	})
}
