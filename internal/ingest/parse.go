// Copied from grafana/loki@39c256e50a804c045d5eb4e3c817a31653b535fe pkg/distributor/http.go (the parse part of pushHandler, recordParsedRequest) and pkg/distributor/metrics.go (bytesIngested, expandedBytesIngested, structuredMetadataBytesIngested, linesIngested, distributorLagByUserAgent, streamsPerPushRequest); modifications: a function instead of Distributor methods, collectors are package-level as they were in pkg/loghttp/push before #24739, errors are returned as push.ParseRequest returned them (400, not the distributor's 413), no tenant configs, usage tracker, stream resolver or presumed agent IP (lokihouse passed nil/"" before), no analytics counters, push-stats-count, OTLP attribute reporter or push-stream logging.

package ingest

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Package-level Prometheus collectors, which AGENTS.md allows. They carry
// Loki's distributor metric names so dashboards keep working; Loki keeps them
// in its Distributor, which lokihouse does not construct.
var (
	bytesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.Loki,
		Name:      "distributor_bytes_received_total",
		Help:      "The total number of uncompressed bytes received per tenant. Includes structured metadata bytes. For OTLP, resource and scope attributes are considered only once per request.",
	}, []string{"tenant", "retention_hours", "is_internal_stream", "policy", "format"})
	expandedBytesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.Loki,
		Name:      "distributor_expanded_bytes_received_total",
		Help:      "The total number of uncompressed bytes received per tenant. Includes structured metadata bytes. For OTLP, all attributes added as structured metadata are considered.",
	}, []string{"tenant", "format"})
	structuredMetadataBytesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.Loki,
		Name:      "distributor_structured_metadata_bytes_received_total",
		Help:      "The total number of uncompressed bytes received per tenant for entries' structured metadata",
	}, []string{"tenant", "retention_hours", "is_internal_stream", "policy", "format"})
	linesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.Loki,
		Name:      "distributor_lines_received_total",
		Help:      "The total number of lines received per tenant",
	}, []string{"tenant", "is_internal_stream", "policy", "format"})
	distributorLagByUserAgent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: constants.Loki,
		Name:      "distributor_lag_ms_total",
		Help:      "The difference in time (in millis) between when a distributor receives a push request and the most recent log timestamp in that request",
	}, []string{"tenant", "userAgent", "format"})
	streamsPerPushRequest = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: constants.Loki,
		Name:      "distributor_streams_per_push_request",
		Help:      "The number of streams in a single push request.",
		Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 512, 2048},
	}, []string{"format"})
)

// parsePush validates the backfill shard header, parses the request body with
// parser and records Loki's push metrics. A push whose lines were all filtered
// is a success: its metrics are recorded and the request is returned.
func parsePush(r *http.Request, logger log.Logger, tenantID string, limits push.Limits, parser push.RequestParser, format string, maxRecvMsgSize int, maxDecompressedSize int64) (*logproto.InternalPushRequest, error) {
	shard, ok, err := push.ExtractAndValidateBackfillShard(r)
	if err != nil {
		return nil, err
	}
	if ok {
		r = r.Clone(push.InjectBackfillShardContext(r.Context(), shard))
	}

	req, stats, err := parser(tenantID, r, limits, nil, maxRecvMsgSize, maxDecompressedSize, nil, nil, logger)
	if err != nil && !errors.Is(err, push.ErrAllLogsFiltered) {
		if errors.Is(err, util.ErrMessageSizeTooLarge) || errors.Is(err, util.ErrMessageDecompressedSizeTooLarge) {
			return nil, fmt.Errorf("%w: %s", push.ErrRequestBodyTooLarge, err.Error())
		}
		return nil, err
	}
	recordParsedRequest(r, logger, tenantID, format, req, stats)
	return req, nil
}

// lineBreaks removes the characters that let a client-controlled value forge
// additional log lines.
var lineBreaks = strings.NewReplacer("\r", "", "\n", "")

func logSafe(s string) string { return lineBreaks.Replace(s) }

func recordParsedRequest(r *http.Request, logger log.Logger, tenantID, format string, req *logproto.InternalPushRequest, stats *push.Stats) {
	// A parser need not populate req/stats on every error it returns.
	if req == nil || stats == nil {
		return
	}

	var entriesSize, structuredMetadataSize int64
	hasInternalStreams := fmt.Sprintf("%t", stats.HasInternalStreams)

	for policyName, retentionToSize := range stats.LogLinesBytes {
		for retentionPeriod, size := range retentionToSize {
			retentionHours := push.RetentionPeriodToString(retentionPeriod)
			// Negative values must not reach Prometheus counters.
			if size >= 0 {
				bytesIngested.WithLabelValues(tenantID, retentionHours, hasInternalStreams, policyName, format).Add(float64(size))
			} else {
				level.Error(logger).Log("msg", "negative log lines bytes received", "userID", tenantID, "retentionHours", retentionHours, "hasInternalStreams", hasInternalStreams, "policyName", policyName, "size", size) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			entriesSize += size
		}
	}
	for policyName, retentionToSize := range stats.StructuredMetadataBytes {
		for retentionPeriod, size := range retentionToSize {
			retentionHours := push.RetentionPeriodToString(retentionPeriod)
			if size >= 0 {
				structuredMetadataBytesIngested.WithLabelValues(tenantID, retentionHours, hasInternalStreams, policyName, format).Add(float64(size))
				bytesIngested.WithLabelValues(tenantID, retentionHours, hasInternalStreams, policyName, format).Add(float64(size))
			} else {
				level.Error(logger).Log("msg", "negative structured metadata bytes received", "userID", tenantID, "retentionHours", retentionHours, "hasInternalStreams", hasInternalStreams, "policyName", policyName, "size", size) //nolint:gosec // G104: go-kit logger errors are not actionable
			}
			entriesSize += size
			structuredMetadataSize += size
		}
	}
	expandedBytesIngested.WithLabelValues(tenantID, format).Add(float64(stats.TotalExpandedEntriesSize))

	var totalNumLines int64
	for policy, numLines := range stats.PolicyNumLines {
		if numLines != 0 && tenantID != "" {
			linesIngested.WithLabelValues(tenantID, hasInternalStreams, policy, format).Add(float64(numLines))
		}
		totalNumLines += numLines
	}
	streamsPerPushRequest.WithLabelValues(format).Observe(float64(len(req.Streams)))
	mostRecentLagMs := time.Since(stats.MostRecentEntryTimestamp).Milliseconds()

	logValues := []interface{}{
		"msg", "push request parsed",
		"path", logSafe(r.URL.Path),
		"contentType", stats.ContentType,
		"contentEncoding", stats.ContentEncoding,
		"bodySize", humanize.Bytes(uint64(max(stats.BodySize, 0))),
		"streams", len(req.Streams),
		"entries", totalNumLines,
		"mostRecentLagMs", mostRecentLagMs,
	}
	// Sanitize the User-Agent to valid UTF-8: Prometheus panics on invalid
	// label values.
	userAgent := strings.ToValidUTF8(r.Header.Get("User-Agent"), "")
	if userAgent != "" {
		logValues = append(logValues, "userAgent", logSafe(strings.TrimSpace(userAgent)))
	}
	// Negative lag (timestamps in the future) says nothing about ingestion
	// falling behind, and values over 1e9 come from misbehaving OTLP clients.
	if mostRecentLagMs >= 0 && mostRecentLagMs < 1_000_000_000 {
		distributorLagByUserAgent.WithLabelValues(tenantID, userAgent, format).Add(float64(mostRecentLagMs))
	}
	level.Debug(logger).Log(logValues...) //nolint:gosec // G104: go-kit logger errors are not actionable
}
