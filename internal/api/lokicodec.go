package api

import (
	"context"
	"net/http"

	gmux "github.com/gorilla/mux"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/querier/queryrange"
	"github.com/grafana/loki/v3/pkg/querier/queryrange/queryrangebase"
)

// lokiHandler answers the requests Loki's query-frontend codec decodes for the
// index and drilldown endpoints. Requests are decoded and responses encoded by
// queryrange.DefaultCodec, so the wire format is Loki's own.
type lokiHandler struct{ a *API }

func (h lokiHandler) Do(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	switch r := req.(type) {
	case *logproto.IndexStatsRequest:
		res, err := h.a.index().IndexStats(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.IndexStatsResponse{Response: res}, nil
	case *logproto.VolumeRequest:
		res, err := h.a.index().Volume(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.VolumeResponse{Response: res}, nil
	case *queryrange.DetectedLabelsRequest:
		res, err := h.a.index().DetectedLabels(ctx, &r.DetectedLabelsRequest)
		if err != nil {
			return nil, err
		}
		return &queryrange.DetectedLabelsResponse{Response: res}, nil
	case *queryrange.DetectedFieldsRequest:
		logs := queryrangebase.HandlerFunc(h.logQuery)
		return queryrange.NewDetectedFieldsHandler(logs, logs, h.a.Limits).Do(ctx, r)
	case *queryrange.LokiRequest:
		return h.logQuery(ctx, r)
	case *logproto.QueryPatternsRequest:
		res, err := h.a.index().Patterns(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.QueryPatternsResponse{Response: res}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "unsupported request type %T", req)
	}
}

// logQuery runs a log/range query on lokihouse's engine and returns Loki's
// queryrange response, as the querier does for the frontend.
func (h lokiHandler) logQuery(ctx context.Context, req queryrangebase.Request) (queryrangebase.Response, error) {
	params, err := queryrange.ParamsFromRequest(req)
	if err != nil {
		return nil, err
	}
	res, err := h.a.Queries.Query(ctx, params).Exec(ctx)
	if err != nil {
		return nil, err
	}
	return queryrange.ResultToResponse(res, params)
}

// lokiRoutes registers the endpoints served through Loki's codec.
func (a *API) lokiRoutes(mux *http.ServeMux) {
	var h queryrangebase.Handler = lokiHandler{a: a}
	h = queryrange.NewDetectedLabelsCardinalityFilter(h)
	h = queryrange.NewVolumeMiddleware().Wrap(h)
	codec := a.tenant(queryrange.NewSerializeHTTPHandler(h, queryrange.DefaultCodec))
	// Plain (no tenant wrapper) codec handler, for the {name} URL-var route.
	plain := queryrange.NewSerializeHTTPHandler(h, queryrange.DefaultCodec)
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/index/stats", codec)
		mux.Handle(m+" /loki/api/v1/index/volume", codec)
		mux.Handle(m+" /loki/api/v1/index/volume_range", codec)
		mux.Handle(m+" /loki/api/v1/detected_labels", codec)
		mux.Handle(m+" /loki/api/v1/detected_fields", codec)
		mux.Handle(m+" /loki/api/v1/patterns", codec)
		// loghttp reads {name} with gorilla mux.Vars.
		mux.Handle(m+" /loki/api/v1/detected_field/{name}/values", a.tenant(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			plain.ServeHTTP(w, gmux.SetURLVars(r, map[string]string{"name": r.PathValue("name")}))
		})))
	}
}
