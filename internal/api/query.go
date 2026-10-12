package api

import (
	"net/http"
	"time"

	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	"github.com/grafana/loki/v3/pkg/util/marshal"
	serverutil "github.com/grafana/loki/v3/pkg/util/server"
)

func (a *API) rangeQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	rq, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	p, err := params(rq.Query, rq.Start, rq.End, rq.Step, rq.Interval, rq.Direction, rq.Limit, rq.Shards)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	expr := p.GetExpression()
	start, empty, err := a.checkQuery(r.Context(), expr, rq.Start, rq.End, rq.Limit)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	if empty {
		a.writeEmpty(w, r, expr, false)
		return
	}
	if !start.Equal(rq.Start) {
		rq.Start = start
		// The query parsed above; only the start changed, so this cannot fail.
		p, _ = params(rq.Query, rq.Start, rq.End, rq.Step, rq.Interval, rq.Direction, rq.Limit, rq.Shards)
	}
	a.exec(w, r, p)
}

func (a *API) instantQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	iq, err := loghttp.ParseInstantQuery(r)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	p, err := params(iq.Query, iq.Ts, iq.Ts, 0, 0, iq.Direction, iq.Limit, iq.Shards)
	if err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}
	expr := p.GetExpression()
	// Match Loki's doInstantRequest guard: a log (non-sample) expression is not
	// valid as an instant query.
	if _, isSample := expr.(syntax.SampleExpr); !isSample {
		serverutil.WriteError(logqlmodel.ErrUnsupportedSyntaxForInstantQuery, w)
		return
	}
	if _, empty, err := a.checkQuery(r.Context(), expr, iq.Ts, iq.Ts, iq.Limit); err != nil {
		serverutil.WriteError(err, w)
		return
	} else if empty {
		a.writeEmpty(w, r, expr, true)
		return
	}
	a.exec(w, r, p)
}

// params wraps logql.NewLiteralParams and reports its parse error; a nil
// expression must never reach the engine (it would yield a 500, not a 400).
func params(q string, start, end time.Time, step, interval time.Duration, dir logproto.Direction, limit uint32, shards []string) (logql.Params, error) {
	return logql.NewLiteralParams(q, start, end, step, interval, dir, limit, shards, nil)
}

func (a *API) writeEmpty(w http.ResponseWriter, r *http.Request, expr syntax.Expr, instant bool) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = marshal.WriteQueryResponseJSON(emptyResult(expr, instant), nil, stats.Result{}, w, httpreq.ExtractEncodingFlags(r))
}

func (a *API) exec(w http.ResponseWriter, r *http.Request, p logql.Params) {
	res, err := a.Queries.Query(r.Context(), p).Exec(r.Context())
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := marshal.WriteQueryResponseJSON(res.Data, res.Warnings, res.Statistics, w, httpreq.ExtractEncodingFlags(r)); err != nil {
		serverutil.WriteError(err, w)
	}
}
