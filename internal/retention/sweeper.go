// Package retention applies the current retention_period and retention_stream to the
// stored data (Loki's compactor.retention_enabled).
package retention

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/compactor/retention"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/chstore"
)

const day = 24 * time.Hour

// Sweeper removes data older than the retention that is configured now. A UTC day D
// with a retention of N days is removed once D+N <= today (UTC). Every step is
// idempotent, so several instances may sweep at once.
type Sweeper struct {
	Conn     driver.Conn
	DB       string
	Topo     chstore.Topology
	Limits   *validation.Overrides
	Interval time.Duration
	Logger   log.Logger
	// Now returns the current time for Run (default time.Now).
	Now func() time.Time
}

// days rounds a retention period up to whole days, capped at the 16 bit range.
func days(p time.Duration) int {
	d := int((p + day - 1) / day)
	return min(max(d, 0), math.MaxUint16)
}

// group is the set of streams of a tenant that share one retention.
type group struct {
	tenant string
	days   int
}

// Sweep applies the retention configured now, as of now. Errors are collected; a failing
// step does not stop the others.
func (s *Sweeper) Sweep(ctx context.Context, now time.Time) error {
	today := now.UTC().Truncate(day)
	var errs []error
	maxDays := s.maxDays()
	var dropped, deleted int

	if maxDays > 0 {
		for _, table := range chstore.RetentionTables() {
			parts, err := chstore.DayPartitions(ctx, s.Conn, s.DB, s.Topo, table)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			for _, d := range parts {
				if d.AddDate(0, 0, maxDays).After(today) {
					continue
				}
				if err := chstore.DropDay(ctx, s.Conn, s.DB, s.Topo, table, d); err != nil {
					errs = append(errs, err)
					continue
				}
				dropped++
			}
		}
	}

	groups := map[group][]uint64{}
	snaps := map[string]*retention.TenantRetentionSnapshot{}
	err := chstore.StreamsUpTo(ctx, s.Conn, today.AddDate(0, 0, -1), func(r chstore.StreamRef) error {
		snap := snaps[r.Tenant]
		if snap == nil {
			snap = retention.NewTenantRetentionSnapshot(s.Limits, r.Tenant)
			snaps[r.Tenant] = snap
		}
		if d := days(snap.RetentionPeriodFor(r.Labels)); d > 0 && (maxDays == 0 || d < maxDays) {
			g := group{r.Tenant, d}
			groups[g] = append(groups[g], r.ID)
		}
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	touched := map[string]bool{}
	for g, ids := range groups {
		cutoff := today.AddDate(0, 0, -g.days)
		for batch := range slices.Chunk(ids, chstore.StreamBatch) {
			expired, err := chstore.DaysWithRows(ctx, s.Conn, g.tenant, batch, cutoff)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if len(expired) == 0 {
				continue
			}
			for _, table := range chstore.RetentionTables() {
				if err := chstore.DeleteDays(ctx, s.Conn, s.DB, s.Topo, table, g.tenant, batch, expired); err != nil {
					errs = append(errs, err)
					continue
				}
				touched[table] = true
			}
			deleted += len(expired)
		}
	}
	for _, table := range chstore.RetentionTables() {
		if !touched[table] {
			continue
		}
		if err := chstore.ApplyDeletedMask(ctx, s.Conn, s.DB, s.Topo, table); err != nil {
			errs = append(errs, err)
		}
	}
	if (dropped > 0 || deleted > 0) && s.Logger != nil {
		level.Info(s.Logger).Log("msg", "retention sweep", "dropped_days", dropped, "deleted_stream_days", deleted) //nolint:gosec // G104: go-kit logger errors are not actionable
	}
	return errors.Join(errs...)
}

// maxDays is the longest retention in days, 0 for "keep forever" (retention_period 0).
// There are no per-tenant overrides, so any tenant ID yields the global limits.
func (s *Sweeper) maxDays() int {
	const anyTenant = ""
	if s.Limits.RetentionPeriod(anyTenant) <= 0 {
		return 0
	}
	m := days(s.Limits.RetentionPeriod(anyTenant))
	for _, r := range s.Limits.StreamRetention(anyTenant) {
		m = max(m, days(time.Duration(r.Period)))
	}
	return m
}

// Run calls Sweep every Interval until ctx ends; errors are logged.
func (s *Sweeper) Run(ctx context.Context) {
	now := s.Now
	if now == nil {
		now = time.Now
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		if err := s.Sweep(ctx, now()); err != nil && ctx.Err() == nil && s.Logger != nil {
			level.Error(s.Logger).Log("msg", "retention sweep failed", "err", fmt.Sprint(err)) //nolint:gosec // G104: go-kit logger errors are not actionable
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
