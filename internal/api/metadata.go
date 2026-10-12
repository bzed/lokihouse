package api

import (
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/util/marshal"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"
)

func (a *API) labels(w http.ResponseWriter, r *http.Request) { a.label(w, r) }

func (a *API) labelValues(w http.ResponseWriter, r *http.Request) { a.label(w, r) }

func (a *API) label(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	if name := r.PathValue("name"); name != "" {
		req.Name, req.Values = name, true
	}
	resp, err := a.meta().Label(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteLabelResponseJSON(resp.Values, w)
}

func (a *API) series(w http.ResponseWriter, r *http.Request) {
	// loghttp.ParseSeriesQuery reads r.Form["match"], which the net/http server
	// does not populate; parse it first. The []-less "match" key is the one
	// Loki's own docs show, and without this it is silently dropped.
	_ = r.ParseForm()
	req, err := loghttp.ParseAndValidateSeriesQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	resp, err := a.meta().Series(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteSeriesResponseJSON(resp.Series, w)
}

// meta returns the metadata querier: the multi-tenant querier when set, else
// the plain single-tenant querier.
func (a *API) meta() MetaQuerier {
	if a.Meta != nil {
		return a.Meta
	}
	return a.Q
}
