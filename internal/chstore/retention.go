package chstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
)

// StreamBatch is the most stream IDs one statement carries; 5 000 IDs render to
// well under the server's max_query_size (256 KiB).
const StreamBatch = 5_000

const (
	dayLayout   = "2006-01-02" // a Date literal
	dayIDLayout = "20060102"   // a partition ID
)

// RetentionTables returns the tables that hold per-day data subject to retention (a
// function because package-level mutable state is not allowed).
func RetentionTables() []string { return []string{"logs", "streams", "stream_stats_hourly"} }

// dayColumn is the expression of a table's UTC day (its partition key).
func dayColumn(table string) (string, error) {
	switch table {
	case "logs":
		return "toDate(ts)", nil
	case "streams":
		return "day", nil
	case "stream_stats_hourly":
		return "toDate(hour)", nil
	}
	return "", fmt.Errorf("table %q has no retention", table)
}

// DayPartitions returns the UTC days that have active parts in the storage table
// (on any replica of a cluster).
func DayPartitions(ctx context.Context, conn driver.Conn, db string, topo Topology, table string) ([]time.Time, error) {
	if _, err := dayColumn(table); err != nil {
		return nil, err
	}
	src := "system.parts"
	if topo.Cluster != "" {
		src = fmt.Sprintf("clusterAllReplicas(%s, system, parts)", sqlString(topo.Cluster))
	}
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT DISTINCT partition_id FROM %s WHERE database = ? AND table = ? AND active ORDER BY partition_id", src),
		db, topo.Local(table))
	if err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", table, err)
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		d, err := time.Parse(dayIDLayout, id)
		if err != nil {
			return nil, fmt.Errorf("partition %q of %s is not a day: %w", id, table, err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DropDay drops the partition of the given UTC day from the storage table. The partition
// ID is rendered from the time, never from a string.
func DropDay(ctx context.Context, conn driver.Conn, db string, topo Topology, table string, day time.Time) error {
	if _, err := dayColumn(table); err != nil {
		return err
	}
	sql := fmt.Sprintf("ALTER TABLE %s.%s %s DROP PARTITION ID '%s'", quoteIdent(db), quoteIdent(topo.Local(table)), topo.OnCluster(), day.UTC().Format(dayIDLayout))
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"replication_alter_partitions_sync": 2}))
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("drop day %s of %s: %w", day.UTC().Format(dayLayout), table, err)
	}
	return nil
}

// StreamRef is a stream of a tenant with its labels.
type StreamRef struct {
	Tenant string
	ID     uint64
	Labels labels.Labels
}

// StreamsUpTo calls fn for every distinct (tenant, stream) registered on a UTC day <= day.
func StreamsUpTo(ctx context.Context, conn driver.Conn, day time.Time, fn func(StreamRef) error) error {
	rows, err := conn.Query(ctx, "SELECT tenant, stream_id, any(labels) FROM streams WHERE day <= toDate(?) GROUP BY tenant, stream_id", day.UTC().Format(dayLayout))
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r StreamRef
		var m map[string]string
		if err := rows.Scan(&r.Tenant, &r.ID, &m); err != nil {
			return err
		}
		r.Labels = labels.FromMap(m)
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

// DaysWithRows returns the UTC days <= upTo on which any of the streams still has rows.
func DaysWithRows(ctx context.Context, conn driver.Conn, tenant string, ids []uint64, upTo time.Time) ([]time.Time, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, "SELECT DISTINCT toDate(ts) AS d FROM logs WHERE tenant = ? AND stream_id IN ? AND toDate(ts) <= toDate(?) ORDER BY d",
		tenant, ids, upTo.UTC().Format(dayLayout))
	if err != nil {
		return nil, fmt.Errorf("find expired days: %w", err)
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var d time.Time
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		// A Date scans as local midnight; keep its calendar day, not its instant.
		y, m, dd := d.Date()
		out = append(out, time.Date(y, m, dd, 0, 0, 0, 0, time.UTC))
	}
	return out, rows.Err()
}

// DeleteDays lightweight-deletes the given UTC days of the streams from the storage table
// (ON CLUSTER on a cluster), StreamBatch stream IDs per statement.
func DeleteDays(ctx context.Context, conn driver.Conn, db string, topo Topology, table, tenant string, ids []uint64, days []time.Time) error {
	col, err := dayColumn(table)
	if err != nil {
		return err
	}
	if len(ids) == 0 || len(days) == 0 {
		return nil
	}
	lits := make([]string, len(days))
	for i, d := range days {
		lits[i] = "toDate('" + d.UTC().Format(dayLayout) + "')"
	}
	sql := fmt.Sprintf("DELETE FROM %s.%s %s WHERE tenant = ? AND stream_id IN ? AND %s IN (%s)",
		quoteIdent(db), quoteIdent(topo.Local(table)), topo.OnCluster(), col, strings.Join(lits, ","))
	for len(ids) > 0 {
		n := min(len(ids), StreamBatch)
		if err := conn.Exec(deleteCtx(ctx), sql, tenant, ids[:n]); err != nil {
			return fmt.Errorf("delete expired days of %s: %w", table, err)
		}
		ids = ids[n:]
	}
	return nil
}
