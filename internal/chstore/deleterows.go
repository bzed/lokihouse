package chstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
)

// RowKey identifies one stored row exactly (the logs sorting key).
type RowKey struct {
	StreamID  uint64
	TS        int64
	EntryHash uint64
}

// Row is one raw stored log line plus its identifying key and metadata.
type Row struct {
	RowKey
	Line string
	MD   labels.Labels
}

// ScanRows streams every raw row (no dedup: each copy must be deleted) of the
// given streams with fromNs <= ts <= throughNs (inclusive bounds).
func ScanRows(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, fromNs, throughNs int64, fn func(Row) error) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := conn.Query(ctx, `SELECT stream_id, toUnixTimestamp64Nano(ts), entry_hash, line, structured_metadata
FROM logs WHERE tenant = ? AND stream_id IN ?
  AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC')`,
		tenant, ids, fromNs, throughNs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r Row
		var md map[string]string
		if err := rows.Scan(&r.StreamID, &r.TS, &r.EntryHash, &r.Line, &md); err != nil {
			return err
		}
		r.MD = labels.FromMap(md)
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

// deleteCtx makes lightweight deletes synchronous: the mutation is applied and
// committed on every replica before it returns.
func deleteCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"lightweight_deletes_sync": 2}))
}

// DeleteBatch is the most keys one DELETE statement carries: a worst-case key
// renders to ~64 bytes of SQL, and the statement must stay under the server's
// max_query_size (256 KiB by default).
const DeleteBatch = 2_000

// DeleteRows removes the given rows with lightweight DELETEs on the storage
// table (ON CLUSTER on a cluster), DeleteBatch keys per statement. The tuple
// list is rendered from integers only.
func DeleteRows(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, keys []RowKey) error {
	for len(keys) > 0 {
		n := min(len(keys), DeleteBatch)
		var b strings.Builder
		for i, k := range keys[:n] {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "(%d,%d,%d)", k.StreamID, k.TS, k.EntryHash)
		}
		sql := fmt.Sprintf("DELETE FROM %s.%s %s WHERE tenant = ? AND (stream_id, toUnixTimestamp64Nano(ts), entry_hash) IN (%s)",
			quoteIdent(db), topo.Local("logs"), topo.OnCluster(), b.String())
		if err := conn.Exec(deleteCtx(ctx), sql, tenant); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// DeleteRange removes every row of the given streams with fromNs <= ts <= throughNs.
func DeleteRange(ctx context.Context, conn driver.Conn, db string, topo Topology, tenant string, ids []uint64, fromNs, throughNs int64) error {
	if len(ids) == 0 {
		return nil
	}
	sql := fmt.Sprintf("DELETE FROM %s.%s %s WHERE tenant = ? AND stream_id IN ? AND ts >= fromUnixTimestamp64Nano(?, 'UTC') AND ts <= fromUnixTimestamp64Nano(?, 'UTC')",
		quoteIdent(db), topo.Local("logs"), topo.OnCluster())
	return conn.Exec(deleteCtx(ctx), sql, tenant, ids, fromNs, throughNs)
}
