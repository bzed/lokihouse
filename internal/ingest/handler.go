package ingest

import (
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/ingest/distcopy"
)

const (
	maxRecvMsgSize      = 100 << 20 // Loki's distributor default (-distributor.max-recv-msg-size)
	maxDecompressedSize = 5000 << 20
)

type Handler struct {
	Writer *chstore.Writer
	Limits *validation.Overrides
	Logger log.Logger
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	parser, format := push.RequestParser(push.ParseLokiRequest), "loki"
	if strings.HasPrefix(r.URL.Path, "/otlp/") {
		parser, format = push.NewOTLPRequestParser(false), "otlp"
	}
	req, _, err := push.ParseRequest(h.Logger, tenantID, maxRecvMsgSize, maxDecompressedSize, r,
		h.Limits, nil, parser, nil, nil, "", format)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	streams, verr := distcopy.Prepare(r.Context(), h.Limits, tenantID, req, time.Now())
	if len(streams) > 0 {
		if err := h.Writer.Write(r.Context(), tenantID, streams, h.retentionDays(tenantID)); err != nil {
			level.Error(h.Logger).Log("msg", "write failed", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if verr != nil {
		http.Error(w, verr.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// retentionDays maps the tenant's retention_period onto the row TTL in whole days,
// rounded up; 0 (Loki's default) keeps rows forever. Per-stream retention is not applied yet.
func (h *Handler) retentionDays(tenantID string) func(labels.Labels) uint16 {
	d := min((h.Limits.RetentionPeriod(tenantID)+24*time.Hour-1)/(24*time.Hour), math.MaxUint16)
	return func(labels.Labels) uint16 { return uint16(d) }
}
