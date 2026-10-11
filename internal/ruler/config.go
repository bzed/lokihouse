// Package ruler runs Loki's ruler (rule storage, evaluation, alert
// notification, recording-rule remote write and the rules HTTP API) over
// lokihouse's query engine.
package ruler

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore"
)

// Defaults returns Loki's ruler and ruler_storage defaults (their flag
// defaults). rule_path moves from Loki's /rules to rules-temp, relative to the
// working directory like Loki's default ruler WAL directory (ruler-wal); Loki's
// single binary moves it under common.path_prefix, which lokihouse lacks.
func Defaults() (lokiruler.Config, rulestore.Config) {
	var rc lokiruler.Config
	rc.RegisterFlags(flag.NewFlagSet("ruler", flag.PanicOnError))
	rc.RulePath = "rules-temp"
	var sc rulestore.Config
	sc.RegisterFlags(flag.NewFlagSet("ruler_storage", flag.PanicOnError))
	return rc, sc
}

// Enabled reports whether the ruler runs: as in Loki's single binary, only
// when ruler_storage is configured.
func Enabled(sc rulestore.Config) bool { return !sc.IsDefaults() }

// Validate runs Loki's ruler config validation and rejects the modes lokihouse
// does not support.
func Validate(rc lokiruler.Config, sc rulestore.Config) error {
	if err := rc.Validate(); err != nil {
		return err
	}
	if err := rc.Evaluation.Validate(); err != nil {
		return err
	}
	if rc.Evaluation.Mode != lokiruler.EvalModeLocal {
		return fmt.Errorf("ruler.evaluation.mode: %q is not supported (lokihouse evaluates rules locally)", rc.Evaluation.Mode)
	}
	if rc.EnableSharding {
		return errors.New("ruler.enable_sharding: not supported (lokihouse has no ruler ring); run the ruler on one instance")
	}
	return checkWALDir(rc, sc)
}

// checkWALDir rejects ruler.wal.dir values that Loki's startup wipe
// (os.RemoveAll in pkg/ruler/registry.go newWALRegistry, run whenever remote
// write is enabled) would turn into data loss: empty, or a directory holding
// the working directory, the rule storage or rule_path.
func checkWALDir(rc lokiruler.Config, sc rulestore.Config) error {
	if !rc.RemoteWrite.Enabled {
		return nil
	}
	if rc.WAL.Dir == "" {
		return errors.New("ruler.wal.dir must be set when remote_write is enabled")
	}
	wal, err := filepath.Abs(rc.WAL.Dir)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	for _, p := range []string{cwd, sc.Filesystem.Directory, sc.Local.Directory, rc.RulePath} {
		if p == "" {
			continue
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return err
		}
		if within(abs, wal) {
			return fmt.Errorf("ruler.wal.dir %q contains %q, and Loki's ruler wipes the WAL directory at startup", rc.WAL.Dir, p)
		}
	}
	return nil
}

// within reports whether path is dir or lies below it.
func within(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
