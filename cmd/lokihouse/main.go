// Command lokihouse serves Loki's HTTP API on top of ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/server"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is main without os.Exit, for tests. Exit codes: 0 ok, 1 runtime or
// config error, 2 usage error.
func run(args []string, _ io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("lokihouse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config.file", "lokihouse.yaml", "configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	logger := log.NewLogfmtLogger(log.NewSyncWriter(stderr))
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(stderr, "lokihouse: %v\n", err)
		return 1
	}
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
