package api

import (
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"
	"github.com/grafana/loki/v3/pkg/util/marshal"
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
	resp, err := a.Q.Label(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteLabelResponseJSON(resp.Values, w)
}

func (a *API) series(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseAndValidateSeriesQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	resp, err := a.Q.Series(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteSeriesResponseJSON(resp.Series, w)
}
