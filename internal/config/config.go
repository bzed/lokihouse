package config

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore"
	"github.com/grafana/loki/v3/pkg/validation"
	yaml "go.yaml.in/yaml/v4"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/ruler"
)

type Config struct {
	Server struct {
		HTTPListenAddress           string        `yaml:"http_listen_address"`
		HTTPListenPort              int           `yaml:"http_listen_port"`
		HTTPServerReadTimeout       time.Duration `yaml:"http_server_read_timeout"`        // Loki/dskit default 30s
		HTTPServerReadHeaderTimeout time.Duration `yaml:"http_server_read_header_timeout"` // 0 = use the read timeout
		HTTPServerWriteTimeout      time.Duration `yaml:"http_server_write_timeout"`       // Loki/dskit default 30s
		HTTPServerIdleTimeout       time.Duration `yaml:"http_server_idle_timeout"`        // Loki/dskit default 120s
		GracefulShutdownTimeout     time.Duration `yaml:"graceful_shutdown_timeout"`       // Loki/dskit default 30s
	} `yaml:"server"`
	AuthEnabled bool `yaml:"auth_enabled"`
	ClickHouse  struct {
		Addr        []string       `yaml:"addr"`
		Database    string         `yaml:"database"`
		Username    string         `yaml:"username"`
		Password    string         `yaml:"password"`
		Cluster     string         `yaml:"cluster"`      // remote_servers name; empty = single server
		Replicated  bool           `yaml:"replicated"`   // Replicated*MergeTree on the cluster (default true)
		ReplicaPath string         `yaml:"replica_path"` // optional; default: server's default_replica_path
		ReplicaName string         `yaml:"replica_name"` // optional; default: server's default_replica_name
		Settings    map[string]any `yaml:"settings"`     // extra ClickHouse session settings, override topology defaults
	} `yaml:"clickhouse"`
	Query struct {
		Pushdown string `yaml:"pushdown"` // on | off | downstream; default on
	} `yaml:"query"`
	Compactor struct {
		CompactionInterval        time.Duration `yaml:"compaction_interval"`          // delete processing loop
		DeleteRequestCancelPeriod time.Duration `yaml:"delete_request_cancel_period"` // deletes run after this
		DeleteMaxInterval         time.Duration `yaml:"delete_max_interval"`
	} `yaml:"compactor"`
	Ruler        lokiruler.Config  `yaml:"ruler"`         // Loki's ruler block, Loki's keys and defaults
	RulerStorage rulestore.Config  `yaml:"ruler_storage"` // the ruler runs only when this is set
	LimitsConfig validation.Limits `yaml:"limits_config"`
}

// PushdownModes are the valid values for query.pushdown.
var PushdownModes = map[string]bool{"on": true, "off": true, "downstream": true}

func Load(path string) (*Config, error) {
	var defaults validation.Limits
	defaults.RegisterFlags(flag.NewFlagSet("defaults", flag.PanicOnError))
	validation.SetDefaultLimitsForYAMLUnmarshalling(defaults)

	c := &Config{AuthEnabled: true}
	c.Server.HTTPListenPort = 3100
	c.Server.HTTPServerReadTimeout = 30 * time.Second
	c.Server.HTTPServerWriteTimeout = 30 * time.Second
	c.Server.HTTPServerIdleTimeout = 120 * time.Second
	c.Server.GracefulShutdownTimeout = 30 * time.Second
	c.ClickHouse.Addr = []string{"localhost:9000"}
	c.ClickHouse.Database = "lokihouse"
	c.ClickHouse.Username = "default"
	c.ClickHouse.Replicated = true
	c.Query.Pushdown = "on"
	c.Compactor.CompactionInterval = 10 * time.Minute
	c.Compactor.DeleteRequestCancelPeriod = 24 * time.Hour
	c.Compactor.DeleteMaxInterval = 24 * time.Hour
	c.Ruler, c.RulerStorage = ruler.Defaults()
	c.LimitsConfig = defaults

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(io.NopCloser(bytes.NewReader(b)))
	if err := dec.Decode(c); err != nil {
		return nil, err
	}
	if err := c.LimitsConfig.Validate(); err != nil {
		return nil, fmt.Errorf("limits_config: %w", err)
	}
	if !PushdownModes[c.Query.Pushdown] {
		return nil, fmt.Errorf("query.pushdown: invalid value %q (want on, off or downstream)", c.Query.Pushdown)
	}
	if c.RulerEnabled() {
		if err := ruler.Validate(c.Ruler, c.RulerStorage); err != nil {
			return nil, fmt.Errorf("ruler: %w", err)
		}
	}
	return c, nil
}

// RulerEnabled reports whether ruler_storage is configured (Loki's rule for
// starting the ruler in the single binary).
func (c *Config) RulerEnabled() bool { return ruler.Enabled(c.RulerStorage) }

// Topology returns the ClickHouse table layout selected by the config.
func (c *Config) Topology() chstore.Topology {
	ch := c.ClickHouse
	return chstore.Topology{
		Cluster:     ch.Cluster,
		Replicated:  ch.Cluster != "" && ch.Replicated,
		ReplicaPath: ch.ReplicaPath,
		ReplicaName: ch.ReplicaName,
	}
}

// CHSettings returns the session settings for every ClickHouse connection.
func (c *Config) CHSettings() clickhouse.Settings {
	s := c.Topology().Settings()
	for k, v := range c.ClickHouse.Settings {
		s[k] = v
	}
	return s
}
