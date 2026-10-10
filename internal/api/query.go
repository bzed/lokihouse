package api

import (
	"net/http"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"
	"github.com/grafana/loki/v3/pkg/util/marshal"
)

func (a *API) rangeQuery(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	rq, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	p, err := logql.NewLiteralParams(rq.Query, rq.Start, rq.End, rq.Step, rq.Interval, rq.Direction, rq.Limit, rq.Shards, nil)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	a.exec(w, r, p)
}

func (a *API) instantQuery(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	iq, err := loghttp.ParseInstantQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	// Match Loki's doInstantRequest guard: a log (non-sample) expression is not
	// valid as an instant query.
	if expr, perr := syntax.ParseExpr(iq.Query); perr == nil {
		if _, isSample := expr.(syntax.SampleExpr); !isSample {
			serverutil.WriteError(logqlmodel.ErrUnsupportedSyntaxForInstantQuery, w)
			return
		}
	}
	p, err := logql.NewLiteralParams(iq.Query, iq.Ts, iq.Ts, 0, 0, iq.Direction, iq.Limit, iq.Shards, nil)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	a.exec(w, r, p)
}

func (a *API) exec(w http.ResponseWriter, r *http.Request, p logql.Params) {
	res, err := a.Engine.Query(p).Exec(r.Context())
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := marshal.WriteQueryResponseJSON(res.Data, res.Warnings, res.Statistics, w, httpreq.ExtractEncodingFlags(r)); err != nil {
		serverutil.WriteError(err, w)
	}
}
