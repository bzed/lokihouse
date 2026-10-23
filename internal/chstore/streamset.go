package chstore

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

// streamIDsIn is the condition that restricts a query to the streams sent by
// withStreamIDs. GLOBAL makes a cluster initiator ship the set to the shards;
// on a single server it behaves as IN. ClickHouse still uses the set for
// primary key analysis.
const streamIDsIn = "stream_id GLOBAL IN (SELECT id FROM lh_stream_ids)"

// withStreamIDs returns ctx carrying the external table lh_stream_ids (id
// UInt64) holding ids. Stream ID sets can hold tens of thousands of IDs, far
// more than max_query_size allows as SQL literals. Callers return early for an
// empty set: ClickHouse rejects an external table without rows.
func withStreamIDs(ctx context.Context, ids []uint64) (context.Context, error) {
	t, err := ext.NewTable("lh_stream_ids", ext.Column("id", column.Type("UInt64")))
	if err != nil {
		return ctx, fmt.Errorf("stream id table: %w", err)
	}
	for _, id := range ids {
		if err := t.Append(id); err != nil {
			return ctx, fmt.Errorf("stream id table: %w", err)
		}
	}
	return clickhouse.Context(ctx, clickhouse.WithExternalTable(t)), nil
}
