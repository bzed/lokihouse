package api

import (
	"context"
	"net/http"

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
		res, err := h.a.Q.IndexStats(ctx, r)
		if err != nil {
			return nil, err
		}
		return &queryrange.IndexStatsResponse{Response: res}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "unsupported request type %T", req)
	}
}

// lokiRoutes registers the endpoints served through Loki's codec.
func (a *API) lokiRoutes(mux *http.ServeMux) {
	var h queryrangebase.Handler = lokiHandler{a: a}
	codec := a.tenant(queryrange.NewSerializeHTTPHandler(h, queryrange.DefaultCodec))
	for _, m := range []string{"GET", "POST"} {
		mux.Handle(m+" /loki/api/v1/index/stats", codec)
	}
}
