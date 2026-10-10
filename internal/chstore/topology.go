package chstore

import (
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Topology describes how lokihouse tables are laid out on ClickHouse.
// The zero value is a single server with plain MergeTree tables.
type Topology struct {
	Cluster     string // remote_servers cluster name; empty = single server
	Replicated  bool   // use Replicated*MergeTree engines (needs Keeper)
	ReplicaPath string // optional Keeper path; empty = server's default_replica_path
	ReplicaName string // optional replica name; empty = server's default_replica_name
}

// OnCluster returns the ON CLUSTER clause for DDL and mutations, or "".
func (t Topology) OnCluster() string {
	if t.Cluster == "" {
		return ""
	}
	return "ON CLUSTER " + quoteIdent(t.Cluster)
}

// Local returns the name of the table that physically stores rows.
// On a cluster that is <name>_local; <name> itself is the Distributed table.
func (t Topology) Local(name string) string {
	if t.Cluster == "" {
		return name
	}
	return name + "_local"
}

// Engine returns the storage engine clause for a MergeTree-family engine.
func (t Topology) Engine(base string) string {
	if !t.Replicated {
		return base
	}
	if t.ReplicaPath == "" {
		return "Replicated" + base
	}
	return "Replicated" + base + "(" + sqlString(t.ReplicaPath) + ", " + sqlString(t.ReplicaName) + ")"
}

// Settings returns the session settings every lokihouse connection needs
// for this topology: pushes are acknowledged only once all shards and an
// insert quorum of replicas hold the rows, so a query right after a push sees them.
func (t Topology) Settings() clickhouse.Settings {
	s := clickhouse.Settings{}
	if t.Cluster != "" {
		s["distributed_foreground_insert"] = 1
		s["distributed_ddl_output_mode"] = "throw"
	}
	if t.Replicated {
		s["insert_quorum"] = "auto"
		s["insert_quorum_parallel"] = 1
	}
	return s
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
