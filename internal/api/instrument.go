package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/felixge/httpsnoop"
	dskitinstrument "github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Loki's (dskit server's) HTTP metrics, same names, labels and buckets.
var (
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "loki",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   dskitinstrument.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
	inflightRequests = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "loki",
		Name:      "inflight_requests",
		Help:      "Current number of inflight requests.",
	}, []string{"method", "route"})
)

// instrument records Loki's request metrics for every request the mux serves,
// routing them through next. httpsnoop keeps http.Hijacker working for the
// tail WebSocket.
func instrument(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := routeName(routeOf(mux, r))
		g := inflightRequests.WithLabelValues(r.Method, route)
		g.Inc()
		defer g.Dec()
		m := httpsnoop.CaptureMetrics(next, w, r)
		requestDuration.WithLabelValues(r.Method, route, strconv.Itoa(m.Code),
			strconv.FormatBool(strings.EqualFold(r.Header.Get("Upgrade"), "websocket"))).Observe(m.Duration.Seconds())
	})
}

// routeOf returns the pattern mux matched r against, or "" if none.
func routeOf(mux *http.ServeMux, r *http.Request) string {
	_, pattern := mux.Handler(r)
	return pattern
}

// routeName turns a ServeMux pattern ("GET /loki/api/v1/query") into dskit's
// route label ("loki_api_v1_query"); unmatched requests are "other".
func routeName(pattern string) string {
	if pattern == "" {
		return "other"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		pattern = path
	}
	return middleware.MakeLabelValue(pattern)
}
