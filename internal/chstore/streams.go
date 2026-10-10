package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
)

type Stream struct {
	ID     uint64
	Labels labels.Labels
}

// ResolveStreams returns the streams of tenant matching ms that have data on any UTC day in [from, through].
func ResolveStreams(ctx context.Context, conn driver.Conn, tenant string, ms []*labels.Matcher, from, through time.Time) ([]Stream, error) {
	where, goSide := StreamMatcherSQL(ms)
	q := "SELECT stream_id, any(labels) FROM streams WHERE tenant = ? AND day >= toDate(?) AND day <= toDate(?)"
	args := []any{tenant, from.UTC(), through.UTC()}
	if !where.Empty() {
		q += " AND " + where.SQL
		args = append(args, where.Args...)
	}
	q += " GROUP BY stream_id"
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("resolve streams: %w", err)
	}
	defer rows.Close()
	var out []Stream
	for rows.Next() {
		var id uint64
		var m map[string]string
		if err := rows.Scan(&id, &m); err != nil {
			return nil, err
		}
		ls := labels.FromMap(m)
		ok := true
		for _, gm := range goSide {
			if !gm.Matches(ls.Get(gm.Name)) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, Stream{ID: id, Labels: ls})
		}
	}
	return out, rows.Err()
}
