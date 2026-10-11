package chstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

// maxRegistered bounds the stream registration cache; it is cleared when full.
const maxRegistered = 1 << 20

// Writer inserts pushes into ClickHouse. Batching follows ClickHouse's advice for many
// small concurrent inserts: every push is one async insert per table, which the server
// buffers and flushes as a single part (async_insert_* settings tune the flush).
type Writer struct {
	Conn driver.Conn

	mu         sync.Mutex
	registered map[streamKey]struct{} // (tenant, stream, day) rows known to be in streams
}

type streamKey struct {
	tenant string
	id     uint64
	day    time.Time
}

// Write inserts entries into logs and registers each stream for every UTC day it touches.
// retentionDays returns the row TTL in days for a stream; nil or 0 keeps rows forever.
func (w *Writer) Write(ctx context.Context, tenant string, streams []logproto.Stream, retentionDays func(labels.Labels) uint16) error {
	// wait_for_async_insert makes every Write synchronous: the push API returns 204
	// only once rows are durable, so an immediate query sees them.
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert": 1, "wait_for_async_insert": 1,
	}))
	logs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO logs (tenant, stream_id, ts, line, structured_metadata, retention_days)")
	if err != nil {
		return fmt.Errorf("prepare logs batch: %w", err)
	}
	defer logs.Abort() //nolint:errcheck // G104: ClickHouse batch Abort errors are not actionable

	type newStream struct {
		key  streamKey
		lmap map[string]string
		lstr string
		ret  uint16
	}
	var fresh []newStream
	seen := map[streamKey]struct{}{}
	for _, s := range streams {
		ls, id, err := StreamIdentity(s.Labels)
		if err != nil {
			return fmt.Errorf("stream labels %q: %w", s.Labels, err)
		}
		var ret uint16
		if retentionDays != nil {
			ret = retentionDays(ls)
		}
		var lmap map[string]string
		for _, e := range s.Entries {
			md := make(map[string]string, len(e.StructuredMetadata))
			for _, kv := range e.StructuredMetadata {
				md[kv.Name] = kv.Value
			}
			ts := e.Timestamp.UTC()
			if err := logs.Append(tenant, id, ts, e.Line, md, ret); err != nil {
				return err
			}
			key := streamKey{tenant: tenant, id: id, day: ts.Truncate(24 * time.Hour)}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			w.mu.Lock()
			_, ok := w.registered[key]
			w.mu.Unlock()
			if !ok {
				if lmap == nil {
					lmap = ls.Map()
				}
				fresh = append(fresh, newStream{key, lmap, ls.String(), ret})
			}
		}
	}

	// Register streams before their rows: a failed push leaves at most an empty
	// registration, never rows no query can find. The client's retry re-sends both.
	if len(fresh) > 0 {
		strs, err := w.Conn.PrepareBatch(ctx, "INSERT INTO streams (tenant, day, stream_id, labels, labels_str, retention_days)")
		if err != nil {
			return fmt.Errorf("prepare streams batch: %w", err)
		}
		defer strs.Abort() //nolint:errcheck // G104: ClickHouse batch Abort errors are not actionable
		for _, n := range fresh {
			if err := strs.Append(tenant, n.key.day, n.key.id, n.lmap, n.lstr, n.ret); err != nil {
				return err
			}
		}
		if err := strs.Send(); err != nil {
			return fmt.Errorf("insert streams: %w", err)
		}
		w.mu.Lock()
		if w.registered == nil || len(w.registered)+len(fresh) > maxRegistered {
			w.registered = make(map[streamKey]struct{}, len(fresh))
		}
		for _, n := range fresh {
			w.registered[n.key] = struct{}{}
		}
		w.mu.Unlock()
	}
	if err := logs.Send(); err != nil {
		return fmt.Errorf("insert logs: %w", err)
	}
	return nil
}
