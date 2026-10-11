// Command lokihouse serves Loki's HTTP API on top of ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-kit/log/level"

	"github.com/bzed/lokihouse/internal/build"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/logging"
	"github.com/bzed/lokihouse/internal/server"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// versionInfo formats the build information as Loki's -version output does
// (see prometheus/common/version.Print), reading from lokihouse's own build
// vars so the -ldflags values survive the linker.
func versionInfo(program string) string {
	info := build.Info()
	var b strings.Builder
	fmt.Fprintf(&b, "%s, version %s (branch: %s, revision: %s)\n", program, info["version"], info["branch"], info["revision"])
	fmt.Fprintf(&b, "  build user:       %s\n", info["buildUser"])
	fmt.Fprintf(&b, "  build date:       %s\n", info["buildDate"])
	fmt.Fprintf(&b, "  go version:       %s\n", info["goVersion"])
	return b.String()
}

// run is main without os.Exit, for tests. Exit codes: 0 ok, 1 runtime or
// config error, 2 usage error.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("lokihouse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config.file", "lokihouse.yaml", "configuration file to load")
	expandEnv := fs.Bool("config.expand-env", false, "expand ${VAR} and ${VAR:-default} in the configuration file")
	showVersion := fs.Bool("version", false, "print version information and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, versionInfo("lokihouse"))
		return 0
	}
	cfg, err := config.Load(*path, *expandEnv)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
	logger, err := logging.New(stderr, cfg.Server.LogLevel, cfg.Server.LogFormat)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
	_ = build.Info() // ensure the build package (and its -ldflags vars) is linked
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // restore default handling: a second signal kills the process
	}()
	if err := server.Run(ctx, cfg, server.Options{Logger: logger}); err != nil {
		level.Error(logger).Log("err", err) //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
		return 1
	}
	return 0
}
