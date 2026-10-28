//go:build oracle && bench

package oracle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// settleParts waits until the store's ClickHouse database has no merge running
// and its active logs part count is the same on 3 consecutive 1 s polls, or
// until timeout. With timeout 0 it polls once. Stores without a database
// (Loki) are skipped. A store that does not settle is reported, not failed.
func settleParts(t *testing.T, s benchStore, timeout time.Duration) (parts uint64, settled bool) {
	t.Helper()
	if s.db == "" {
		return 0, true
	}
	opts := chtest.Options("default")
	opts.Addr = s.ch
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("parts of %s: %v", s.db, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	poll := func() (uint64, uint64) {
		var p, m uint64
		err := conn.QueryRow(context.Background(), `SELECT
			(SELECT count() FROM system.parts WHERE database = ? AND table = 'logs' AND active),
			(SELECT count() FROM system.merges WHERE database = ?)`, s.db, s.db).Scan(&p, &m)
		if err != nil {
			t.Fatalf("parts of %s: %v", s.db, err)
		}
		return p, m
	}
	parts, _ = poll()
	if timeout == 0 {
		return parts, false
	}
	deadline := time.Now().Add(timeout)
	stable := 0
	for {
		time.Sleep(time.Second)
		p, merges := poll()
		if merges == 0 && p == parts {
			stable++
		} else {
			stable = 0
		}
		parts = p
		if stable >= 3 {
			return parts, true
		}
		if time.Now().After(deadline) {
			return parts, false
		}
	}
}

// partsLine is the results-file line naming each ClickHouse store's active logs
// part count, or "" when no store has a database.
func partsLine(stores []benchStore, parts []uint64, settled []bool) string {
	var items []string
	for i, s := range stores {
		if s.db == "" {
			continue
		}
		item := fmt.Sprintf("%s %d", s.name, parts[i])
		if !settled[i] {
			item += " (not settled)"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return ""
	}
	return "ClickHouse active logs parts at timing start: " + strings.Join(items, ", ")
}

func TestPartsLine(t *testing.T) {
	stores := []benchStore{
		{name: "Loki"},
		{name: "lokihouse", db: "a"},
		{name: "lokihouse S3 + cache", db: "b"},
	}
	if got, want := partsLine(stores, []uint64{0, 3, 4}, []bool{true, true, false}),
		"ClickHouse active logs parts at timing start: lokihouse 3, lokihouse S3 + cache 4 (not settled)"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := partsLine(stores[:1], []uint64{0}, []bool{true}); got != "" {
		t.Fatalf("no ClickHouse store: got %q, want empty", got)
	}
}
