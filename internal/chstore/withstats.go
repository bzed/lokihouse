package chstore

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
)

// WithQueryStats returns ctx with a ClickHouse progress callback that adds the
// rows and uncompressed bytes the server read to Loki's stats context in ctx
// (summary.totalLinesProcessed / totalBytesProcessed). ClickHouse reports
// progress as increments; existing query options in ctx are kept.
func WithQueryStats(ctx context.Context) context.Context {
	st := stats.FromContext(ctx)
	return clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) {
		st.AddDecompressedLines(int64(p.Rows))  //nolint:gosec // G115: row counts fit in int64
		st.AddDecompressedBytes(int64(p.Bytes)) //nolint:gosec // G115: byte counts fit in int64
	}))
}
