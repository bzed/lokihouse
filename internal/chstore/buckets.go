// Package chstore implements ClickHouse-backed storage for Loki's log data.
// It provides the Querier for log and sample queries, the Writer for ingest,
// and the schema migration and bucketing utilities.
package chstore

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BucketRow holds the deduplicated row count and byte size for one stream in one
// bucket (time slice).
type BucketRow struct {
	StreamID uint64
	MD       map[string]string
	Bucket   int64
	Count    uint64
	Bytes    uint64
}

// BucketCounts groups the deduplicated (FINAL) rows of the given streams in
// (origin, through] into sub-buckets of width g ns. bucket =
// intDiv(toUnixTimestamp64Nano(ts) - origin + g - 1, g), so bucket j holds
// (origin + (j-1)*g, origin + j*g].
func BucketCounts(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, origin, through, g int64, filter Fragment) ([]BucketRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	sql := "SELECT stream_id, structured_metadata, intDiv(toUnixTimestamp64Nano(ts) - ? + ? - 1, ?) AS bucket, count() AS n, sum(length(line)) AS bytes FROM logs FINAL WHERE tenant = ? AND " + streamIDsIn + " AND ts > fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC')"
	args := []any{origin, g, g, tenant, origin, through}
	if !filter.Empty() {
		sql += " AND " + filter.SQL
		args = append(args, filter.Args...)
	}
	sql += " GROUP BY stream_id, structured_metadata, bucket"
	var out []BucketRow
	ctx, err := withStreamIDs(WithQueryStats(ctx), ids)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var br BucketRow
		if err := rows.Scan(&br.StreamID, &br.MD, &br.Bucket, &br.Count, &br.Bytes); err != nil {
			return nil, err
		}
		out = append(out, br)
	}
	return out, rows.Err()
}

var _ = time.Now
