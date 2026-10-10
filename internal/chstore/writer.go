package chstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

const defaultRetentionDays = 31

type Writer struct {
	Conn driver.Conn
}

type streamDay struct {
	id  uint64
	day time.Time
}

// Write inserts entries into logs and registers each stream for every UTC day it touches.
func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream, retentionDays func(labels.Labels) uint16) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert": 1, "wait_for_async_insert": 1,
	}))
	logs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO logs (tenant, stream_id, ts, line, structured_metadata, retention_days)")
	if err != nil {
		return fmt.Errorf("prepare logs batch: %w", err)
	}
	defer logs.Abort()
	strs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO streams (tenant, day, stream_id, labels, labels_str)")
	if err != nil {
		return fmt.Errorf("prepare streams batch: %w", err)
	}
	defer strs.Abort()

	seen := map[streamDay]struct{}{}
	for _, s := range streams {
		ls, id, err := StreamIdentity(s.Labels)
		if err != nil {
			return fmt.Errorf("stream labels %q: %w", s.Labels, err)
		}
		ret := uint16(defaultRetentionDays)
		if retentionDays != nil {
			ret = retentionDays(ls)
		}
		lmap := ls.Map()
		lstr := ls.String()
		for _, e := range s.Entries {
			md := make(map[string]string, len(e.StructuredMetadata))
			for _, kv := range e.StructuredMetadata {
				md[kv.Name] = kv.Value
			}
			ts := e.Timestamp.UTC()
			if err := logs.Append(tenant, id, ts, e.Line, md, ret); err != nil {
				return err
			}
			key := streamDay{id: id, day: ts.Truncate(24 * time.Hour)}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				if err := strs.Append(tenant, key.day, id, lmap, lstr); err != nil {
					return err
				}
			}
		}
	}
	if err := logs.Send(); err != nil {
		return fmt.Errorf("insert logs: %w", err)
	}
	return strs.Send()
}
