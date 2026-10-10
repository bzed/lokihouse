package config

import (
	"bytes"
	"flag"
	"io"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/validation"
	yaml "go.yaml.in/yaml/v4"

	"github.com/bzed/lokihouse/internal/chstore"
)

type Config struct {
	Server struct {
		HTTPListenAddress string `yaml:"http_listen_address"`
		HTTPListenPort    int    `yaml:"http_listen_port"`
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
	LimitsConfig validation.Limits `yaml:"limits_config"`
}

func Load(path string) (*Config, error) {
	var defaults validation.Limits
	defaults.RegisterFlags(flag.NewFlagSet("defaults", flag.PanicOnError))
	validation.SetDefaultLimitsForYAMLUnmarshalling(defaults)

	c := &Config{AuthEnabled: true}
	c.Server.HTTPListenPort = 3100
	c.ClickHouse.Addr = []string{"localhost:9000"}
	c.ClickHouse.Database = "lokihouse"
	c.ClickHouse.Username = "default"
	c.ClickHouse.Replicated = true
	c.LimitsConfig = defaults

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(io.NopCloser(bytes.NewReader(b)))
	if err := dec.Decode(c); err != nil {
		return nil, err
	}
	return c, nil
}

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
