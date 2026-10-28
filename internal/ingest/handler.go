// Package ingest handles Loki push requests, validating and writing them to
// ClickHouse via the chstore.Writer.
package ingest

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/limiter"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/ingest/distcopy"
)

// reasonTooManyPartitions labels pushes rejected for spanning more than
// chstore.MaxPushPartitions partitions in Loki's discarded-data metrics.
const reasonTooManyPartitions = "too_many_partitions"

const (
	maxRecvMsgSize      = 100 << 20 // Loki's distributor default (-distributor.max-recv-msg-size)
	maxDecompressedSize = 5000 << 20
)

// Handler handles Loki push requests and writes them to ClickHouse.
// RateLimiter is nil when the rate limit is disabled.
type Handler struct {
	Writer      *chstore.Writer
	Limits      *validation.Overrides
	Logger      log.Logger
	RateLimiter *limiter.RateLimiter

	// MaxRecvMsgSize and MaxDecompressedSize bound a push body; zero selects
	// the package defaults.
	MaxRecvMsgSize      int
	MaxDecompressedSize int64
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
	recvSize, decompSize := h.MaxRecvMsgSize, h.MaxDecompressedSize
	if recvSize == 0 {
		recvSize = maxRecvMsgSize
	}
	if decompSize == 0 {
		decompSize = maxDecompressedSize
	}
	req, err := parsePush(r, h.Logger, tenantID, h.Limits, parser, format, recvSize, decompSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, verr := distcopy.Prepare(r.Context(), h.Limits, tenantID, req, time.Now())
	if h.RateLimiter != nil && len(res.Streams) > 0 && !h.RateLimiter.AllowN(time.Now(), tenantID, res.Bytes) {
		// Rejected before the write, as in Loki's distributor: nothing is
		// stored, and the push consumes no tokens.
		distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, validation.RateLimited, res.Streams, time.Now())
		http.Error(w, fmt.Sprintf(validation.RateLimitedErrorMsg, tenantID, int(h.RateLimiter.Limit(time.Now(), tenantID)), res.Lines, res.Bytes), http.StatusTooManyRequests)
		return
	}
	if len(res.Streams) > 0 {
		if err := h.Writer.Write(r.Context(), tenantID, res.Streams); err != nil {
			if errors.Is(err, chstore.ErrTooManyPartitions) {
				distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, reasonTooManyPartitions, res.Streams, time.Now())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
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
