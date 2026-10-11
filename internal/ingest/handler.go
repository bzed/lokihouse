// Package ingest handles Loki push requests, validating and writing them to
// ClickHouse via the chstore.Writer.
package ingest

import (
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/compactor/retention"
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

// Handler handles Loki push requests and writes them to ClickHouse.
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
			level.Error(h.Logger).Log("msg", "write failed", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
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

// retentionDays returns the row TTL in whole days (rounded up) for each stream:
// Loki's compactor rule (retention_stream, highest priority wins, shorter period
// on a tie, else the tenant retention_period). 0 keeps rows forever.
func (h *Handler) retentionDays(tenantID string) func(labels.Labels) uint16 {
	snap := retention.NewTenantRetentionSnapshot(h.Limits, tenantID)
	return func(ls labels.Labels) uint16 {
		d := (snap.RetentionPeriodFor(ls) + 24*time.Hour - 1) / (24 * time.Hour)
		if d < 0 {
			d = 0
		}
		if d > math.MaxUint16 {
			d = math.MaxUint16
		}
		return uint16(d)
	}
}
