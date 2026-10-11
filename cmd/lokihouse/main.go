package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/tail"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/patterns"
	"github.com/bzed/lokihouse/internal/pushdown"
	lhtail "github.com/bzed/lokihouse/internal/tail"
)

func main() {
	path := flag.String("config.file", "lokihouse.yaml", "configuration file")
	flag.Parse()
	logger := log.NewLogfmtLogger(os.Stderr)
	if err := run(*path, logger); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}

func run(path string, logger log.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	ctx := context.Background()
	opts := func(db string) *clickhouse.Options {
		return &clickhouse.Options{
			Addr:     cfg.ClickHouse.Addr,
			Auth:     clickhouse.Auth{Database: db, Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password},
			Settings: cfg.CHSettings(),
		}
	}
	admin, err := clickhouse.Open(opts("default"))
	if err != nil {
		return err
	}
	if err := chstore.Migrate(ctx, admin, cfg.ClickHouse.Database, cfg.Topology()); err != nil {
		return err
	}
	_ = admin.Close()
	conn, err := clickhouse.Open(opts(cfg.ClickHouse.Database))
	if err != nil {
		return err
	}
	limits, err := validation.NewOverrides(cfg.LimitsConfig, nil)
	if err != nil {
		return err
	}
	store := &deletes.Store{Conn: conn}
	q := &chstore.Querier{Conn: conn, Deletes: store}
	a := &api.API{
		Queries: &pushdown.Engine{
			Mode:   pushdown.Mode(cfg.Query.Pushdown),
			TierB:  logql.NewEngine(logql.EngineOpts{}, q, limits, logger),
			TierA:  &pushdown.TierA{Conn: conn, Limits: limits, Deletes: store},
			Limits: limits,
			Logger: logger,
		},
		Q:           q,
		Push:        &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: limits, Logger: logger},
		Limits:      limits,
		Patterns:    &patterns.Querier{Q: q, Limits: limits, Logger: logger},
		Deletes:     &deletes.Handler{Store: store, Limits: limits, MaxInterval: cfg.Compactor.DeleteMaxInterval, CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod},
		AuthEnabled: cfg.AuthEnabled,
	}
	// tail.MaxDuration is Loki's -querier.tail-max-duration default (1h).
	tq := tail.NewQuerier(&lhtail.Ingester{Q: q}, q, store, limits, time.Hour, tail.NewMetrics(nil), logger)
	// loghttp.ParseTailQuery reads the query via r.Form, which the net/http
	// server does not populate automatically; parse it first so the selector is
	// visible (r.URL.Query alone would leave r.Form empty).
	a.Tail = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		tq.TailHandler(w, r)
	})
	// Physical deletion loop: after the cancel period, rewrite the deleted rows.
	go (&deletes.Processor{
		Store: store, Conn: conn, DB: cfg.ClickHouse.Database, Topo: cfg.Topology(),
		CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod,
		Interval:     cfg.Compactor.CompactionInterval,
		Logger:       logger,
	}).Run(ctx)
	addr := fmt.Sprintf("%s:%d", cfg.Server.HTTPListenAddress, cfg.Server.HTTPListenPort)
	level.Info(logger).Log("msg", "lokihouse listening", "addr", addr)
	return http.ListenAndServe(addr, a.Router())
}
