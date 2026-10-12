// Package deletes implements Loki's delete API: requests are stored in
// ClickHouse and applied at query time by Loki's own filtering pipeline.
package deletes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/compactor/deletion"
	"github.com/grafana/loki/v3/pkg/compactor/deletion/deletionproto"
	"github.com/prometheus/common/model"
)

// Store keeps delete requests in ClickHouse; it implements Loki's
// querier/deletion.DeleteGetter for query-time filtering.
type Store struct {
	Conn driver.Conn
	Now  func() time.Time // default time.Now

	lastVersion atomic.Uint64 // highest version written by this Store
}

const statusCancelled = "cancelled"

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// nextVersion returns the ReplacingMergeTree version for a write: the clock in
// nanoseconds, but strictly greater than every earlier version of this Store,
// so a frozen or stepped-back clock cannot make a later write tie with, or lose
// to, an earlier one (argMax of a tie is arbitrary).
func (s *Store) nextVersion() uint64 {
	for {
		last := s.lastVersion.Load()
		v := max(uint64(s.now().UnixNano()), last+1)
		if s.lastVersion.CompareAndSwap(last, v) {
			return v
		}
	}
}

func (s *Store) write(ctx context.Context, r deletionproto.DeleteRequest, status string) error {
	return s.Conn.Exec(ctx,
		"INSERT INTO delete_requests (tenant, request_id, query, start_ms, end_ms, created_ms, status, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		r.UserID, r.RequestID, r.Query, int64(r.StartTime), int64(r.EndTime), int64(r.CreatedAt), status, s.nextVersion())
}

// Add stores a new delete request and returns its id. The id is 8 random hex
// characters; Loki's format and ids differ (docs/divergences.md).
func (s *Store) Add(ctx context.Context, tenant, query string, start, end model.Time) (string, error) {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	r := deletionproto.DeleteRequest{RequestID: hex.EncodeToString(b), UserID: tenant, Query: query,
		StartTime: start, EndTime: end, CreatedAt: model.TimeFromUnixNano(s.now().UnixNano())}
	return r.RequestID, s.write(ctx, r, string(deletionproto.StatusReceived))
}

// list returns the latest version of every non-cancelled request matching where.
func (s *Store) list(ctx context.Context, where string, args ...any) ([]deletionproto.DeleteRequest, error) {
	q := `SELECT tenant, request_id, argMax(query, version), argMax(start_ms, version), argMax(end_ms, version),
	argMax(created_ms, version), argMax(status, version) AS st
FROM delete_requests WHERE ` + where + ` GROUP BY tenant, request_id HAVING st != '` + statusCancelled + `'`
	rows, err := s.Conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deletionproto.DeleteRequest
	for rows.Next() {
		var r deletionproto.DeleteRequest
		var st string
		var start, end, created int64
		if err := rows.Scan(&r.UserID, &r.RequestID, &r.Query, &start, &end, &created, &st); err != nil {
			return nil, err
		}
		r.StartTime, r.EndTime, r.CreatedAt = model.Time(start), model.Time(end), model.Time(created)
		r.Status = deletionproto.DeleteRequestStatus(st)
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetAllDeleteRequestsForUser implements querier/deletion.DeleteGetter. With
// forQuerytimeFiltering only pending (received) requests are returned, since
// those are the ones a query must hide lines for; tr, when set, keeps only
// requests overlapping it.
func (s *Store) GetAllDeleteRequestsForUser(ctx context.Context, userID string, forQuerytimeFiltering bool, tr *deletion.TimeRange) ([]deletionproto.DeleteRequest, error) {
	reqs, err := s.list(ctx, "tenant = ?", userID)
	if err != nil {
		return nil, err
	}
	var out []deletionproto.DeleteRequest
	for _, r := range reqs {
		if forQuerytimeFiltering && r.Status != deletionproto.StatusReceived {
			continue
		}
		if tr != nil && (r.StartTime > tr.End || r.EndTime < tr.Start) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// Get returns the latest version of the request, or deletion.ErrDeleteRequestNotFound.
func (s *Store) Get(ctx context.Context, tenant, id string) (deletionproto.DeleteRequest, error) {
	reqs, err := s.list(ctx, "tenant = ? AND request_id = ?", tenant, id)
	if err != nil {
		return deletionproto.DeleteRequest{}, err
	}
	if len(reqs) == 0 {
		return deletionproto.DeleteRequest{}, deletion.ErrDeleteRequestNotFound
	}
	return reqs[0], nil
}

// Cancel marks the request cancelled; cancelled requests are hidden from all
// reads.
func (s *Store) Cancel(ctx context.Context, tenant, id string) error {
	r, err := s.Get(ctx, tenant, id)
	if err != nil {
		return err
	}
	return s.write(ctx, r, statusCancelled)
}

// MarkProcessed records that the request has been processed.
func (s *Store) MarkProcessed(ctx context.Context, r deletionproto.DeleteRequest) error {
	return s.write(ctx, r, string(deletionproto.StatusProcessed))
}

// Unprocessed returns every pending (received) request, all tenants.
func (s *Store) Unprocessed(ctx context.Context) ([]deletionproto.DeleteRequest, error) {
	reqs, err := s.list(ctx, "1 = 1")
	if err != nil {
		return nil, err
	}
	var out []deletionproto.DeleteRequest
	for _, r := range reqs {
		if r.Status == deletionproto.StatusReceived {
			out = append(out, r)
		}
	}
	return out, nil
}
