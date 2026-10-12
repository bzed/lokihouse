package ingest

import (
	"time"

	"github.com/grafana/dskit/limiter"
	"github.com/grafana/loki/v3/pkg/validation"
)

// Copied from grafana/loki@bc196286 pkg/distributor/ingestion_rate_strategy.go
// (localStrategy); modifications: tenant-only keys (lokihouse has no policies),
// also used for Loki's "global" strategy (lokihouse has no distributor ring:
// one instance holds the whole limit, see docs/divergences.md).
type localStrategy struct{ limits *validation.Overrides }

func (s localStrategy) Limit(tenant string) float64 { return s.limits.IngestionRateBytes(tenant) }
func (s localStrategy) Burst(tenant string) int     { return s.limits.IngestionBurstSizeBytes(tenant) }

// NewRateLimiter returns Loki's per-tenant ingestion rate limiter
// (ingestion_rate_mb, ingestion_burst_size_mb), recalculated every 10s like
// the distributor's.
func NewRateLimiter(limits *validation.Overrides) *limiter.RateLimiter {
	return limiter.NewRateLimiter(localStrategy{limits: limits}, 10*time.Second)
}
