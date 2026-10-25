// Package server wires lokihouse's components together and runs them until
// the context ends, then shuts them down in order: HTTP (draining in-flight
// requests), the ruler, the background loops and the ClickHouse connection.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/querier/tail"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/deletes"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/multitenant"
	"github.com/bzed/lokihouse/internal/patterns"
	"github.com/bzed/lokihouse/internal/pushdown"
	"github.com/bzed/lokihouse/internal/retention"
	lhruler "github.com/bzed/lokihouse/internal/ruler"
	lhtail "github.com/bzed/lokihouse/internal/tail"
)

// Options are the process-level hooks Run needs besides the config.
type Options struct {
	Logger log.Logger
	// Listening, when set, is called with the bound HTTP address before
	// systemd is notified (tests listen on port 0).
	Listening func(net.Addr)
}

// Run starts lokihouse and blocks until ctx is done or the HTTP server fails.
// In-flight requests run on a context that is not cancelled by ctx; they get
// server.graceful_shutdown_timeout to finish.
func Run(ctx context.Context, cfg *config.Config, opts Options) error {
	logger := opts.Logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	conn, err := openClickHouse(ctx, cfg)
	if err != nil {
		return err
	}
	closeConn := func() {
		if err := conn.Close(); err != nil {
			level.Warn(logger).Log("msg", "closing clickhouse", "err", err) //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
		}
		level.Info(logger).Log("msg", "stopped", "component", "clickhouse") //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	}

	limits, err := validation.NewOverrides(cfg.LimitsConfig, nil)
	if err != nil {
		closeConn()
		return err
	}
	// Loki's default registry carries the request, distributor and (after the
	// namespace change) ruler metrics; the ruler's own registry is merged in
	// for /metrics. Both /metrics and /debug/pprof are gated on
	// server.register_instrumentation (Loki/dskit default true).
	rulerReg := prometheus.NewRegistry()
	a, store, q := newAPI(cfg, conn, limits, logger, rulerReg)
	if cfg.Server.RegisterInstrumentation {
		a.Pprof = true
		a.Metrics = promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, rulerReg}, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
		// A second Run in one process (tests) finds the collector registered.
		if err := prometheus.Register(version.NewCollector("lokihouse")); err != nil {
			if are := (prometheus.AlreadyRegisteredError{}); !errors.As(err, &are) {
				closeConn()
				return fmt.Errorf("register version collector: %w", err)
			}
		}
	}

	// Background loops run on their own context, cancelled after HTTP and the
	// ruler have stopped, and are waited for before ClickHouse is closed.
	bg, stopBG := context.WithCancel(context.WithoutCancel(ctx))
	var loops sync.WaitGroup
	loops.Go(func() {
		(&deletes.Processor{
			Store: store, Conn: conn, DB: cfg.ClickHouse.Database, Topo: cfg.Topology(),
			CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod,
			Interval:     cfg.Compactor.CompactionInterval,
			Logger:       logger,
		}).Run(bg)
	})
	if cfg.Compactor.RetentionEnabled {
		loops.Go(func() {
			(&retention.Sweeper{
				Conn: conn, DB: cfg.ClickHouse.Database, Topo: cfg.Topology(), Limits: limits,
				Interval: cfg.Compactor.CompactionInterval, Logger: logger,
			}).Run(bg)
		})
	}
	stopLoops := func() {
		stopBG()
		loops.Wait()
		level.Info(logger).Log("msg", "stopped", "component", "deletes") //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	}
	_ = q // used by later tasks (multi-tenant, metrics)

	var r *rulerService
	if cfg.RulerEnabled() {
		r, err = startRuler(ctx, cfg, a, limits, logger, rulerReg)
		if err != nil {
			stopLoops()
			closeConn()
			return err
		}
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Server.HTTPListenAddress, strconv.Itoa(cfg.Server.HTTPListenPort))) //nolint:noctx // the listener must outlive ctx (shutdown uses it); Loki uses the same
	if err != nil {
		r.stop(logger) //nolint:contextcheck // shutdown path: ctx is already done, no live context to pass
		stopLoops()
		closeConn()
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           a.Router(),
		ReadTimeout:       cfg.Server.HTTPServerReadTimeout,
		ReadHeaderTimeout: cfg.Server.HTTPServerReadHeaderTimeout,
		WriteTimeout:      cfg.Server.HTTPServerWriteTimeout,
		IdleTimeout:       cfg.Server.HTTPServerIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	if opts.Listening != nil {
		opts.Listening(ln.Addr())
	}
	level.Info(logger).Log("msg", "lokihouse listening", "addr", ln.Addr().String()) //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	sdNotify(logger, daemon.SdNotifyReady)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served:
	}

	level.Info(logger).Log("msg", "shutting down") //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	sdNotify(logger, daemon.SdNotifyStopping)
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Server.GracefulShutdownTimeout)
	defer cancel()
	var errs []error
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("http server: %w", serveErr))
	}
	if err := srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		_ = srv.Close() // force-close what did not drain; the error above is reported
	}
	level.Info(logger).Log("msg", "stopped", "component", "http") //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	if err := r.stopCtx(sctx, logger); err != nil {
		errs = append(errs, err)
	}
	stopLoops()
	closeConn()
	return errors.Join(errs...)
}

func openClickHouse(ctx context.Context, cfg *config.Config) (driver.Conn, error) {
	admin, err := clickhouse.Open(cfg.CHOptions("default"))
	if err != nil {
		return nil, err
	}
	if err := chstore.Migrate(ctx, admin, cfg.ClickHouse.Database, cfg.Topology()); err != nil {
		_ = admin.Close() // the migration error is the one to report
		return nil, err
	}
	if err := admin.Close(); err != nil {
		return nil, fmt.Errorf("close admin connection: %w", err)
	}
	return clickhouse.Open(cfg.CHOptions(cfg.ClickHouse.Database))
}

// migrateReadTimeout lets the table copies of MigrateSchema run for hours.
const migrateReadTimeout = 24 * time.Hour

// MigrateSchema migrates the ClickHouse schema to the current version (lokihouse
// -schema.migrate). Every other lokihouse instance must be stopped.
func MigrateSchema(ctx context.Context, cfg *config.Config, logger log.Logger) error {
	opts := cfg.CHOptions("default")
	opts.ReadTimeout = migrateReadTimeout
	admin, err := clickhouse.Open(opts)
	if err != nil {
		return err
	}
	err = chstore.MigrateSchema(ctx, admin, cfg.ClickHouse.Database, cfg.Topology(), logger)
	if cerr := admin.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("close admin connection: %w", cerr)
	}
	return err
}

// newAPI builds the HTTP API over conn (moved unchanged from cmd/lokihouse).
func newAPI(cfg *config.Config, conn driver.Conn, limits *validation.Overrides, logger log.Logger, rulerReg prometheus.Registerer) (*api.API, *deletes.Store, *chstore.Querier) {
	store := &deletes.Store{Conn: conn}
	// The sample fan-out reserves one pool connection per stream, all or
	// nothing, and falls back to one query when the reservation fails; the
	// budget is half the pool (at least one), so the log path, tail, ruler
	// and stats keep headroom and the budget can never exceed the pool —
	// with max_open_conns = 1 a zero budget would default to the fan-out
	// limit and deadlock the pool.
	q := &chstore.Querier{Conn: conn, Deletes: store, FanoutConns: max(cfg.ClickHouse.MaxOpenConns/2, 1), Logger: logger}
	// When multi-tenant queries are enabled, log/metric queries fan out per
	// tenant through the multi-tenant querier; Tier A (SQL) is single-tenant
	// and is bypassed for multi-tenant requests (pushdown engine guard).
	var storeQuerier interface {
		logql.Querier
	} = q
	var meta api.MetaQuerier
	if cfg.Query.Querier.MultiTenantQueriesEnabled {
		mq := &multitenant.Querier{Next: q}
		storeQuerier = mq
		meta = mq
	}
	a := &api.API{
		Ready: conn.Ping,
		Queries: &pushdown.Engine{
			Mode:   pushdown.Mode(cfg.Query.Pushdown),
			TierB:  logql.NewEngine(logql.EngineOpts{}, storeQuerier, limits, logger),
			TierA:  &pushdown.TierA{Conn: conn, Limits: limits, Deletes: store},
			Limits: limits,
			Logger: logger,
		},
		Q:    q,
		Meta: meta,
		Push: &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: limits, Logger: logger,
			RateLimiter: ingest.NewRateLimiter(limits)},
		Limits:      limits,
		Patterns:    &patterns.Querier{Q: q, Limits: limits, Logger: logger},
		Deletes:     &deletes.Handler{Store: store, Limits: limits, MaxInterval: cfg.Compactor.DeleteMaxInterval, CancelPeriod: cfg.Compactor.DeleteRequestCancelPeriod},
		AuthEnabled: cfg.AuthEnabled,
	}
	// tail.MaxDuration is Loki's -querier.tail-max-duration default (1h).
	tq := tail.NewQuerier(&lhtail.Ingester{Q: q}, q, store, limits, time.Hour, tail.NewMetrics(nil), logger)
	// loghttp.ParseTailQuery reads the query via r.Form, which the net/http
	// server does not populate automatically; parse it first.
	a.Tail = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm() // ParseTailQuery reports a malformed query itself
		tq.TailHandler(w, r)
	})
	return a, store, q
}

// rulerService is Loki's ruler; a nil *rulerService is a disabled ruler.
type rulerService struct{ svc services.Service }

func startRuler(ctx context.Context, cfg *config.Config, a *api.API, limits *validation.Overrides, logger log.Logger, rulerReg prometheus.Registerer) (*rulerService, error) {
	r, rapi, err := lhruler.New(ctx, cfg.Ruler, cfg.RulerStorage, a.Queries, limits, log.With(logger, "component", "ruler"), rulerReg)
	if err != nil {
		return nil, fmt.Errorf("ruler: %w", err)
	}
	if err := services.StartAndAwaitRunning(ctx, r); err != nil {
		return nil, fmt.Errorf("ruler: %w", err)
	}
	// Loki serves the rules API only with ruler.enable_api (default true).
	if cfg.Ruler.EnableAPI {
		a.Rules = rapi
	}
	return &rulerService{svc: r}, nil
}

func (r *rulerService) stop(logger log.Logger) { _ = r.stopCtx(context.Background(), logger) }

func (r *rulerService) stopCtx(ctx context.Context, logger log.Logger) error {
	if r == nil {
		return nil
	}
	if err := services.StopAndAwaitTerminated(ctx, r.svc); err != nil {
		return fmt.Errorf("stop ruler: %w", err)
	}
	level.Info(logger).Log("msg", "stopped", "component", "ruler") //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	return nil
}

// sdNotify tells systemd about state changes (Type=notify); a no-op when
// NOTIFY_SOCKET is unset.
func sdNotify(logger log.Logger, state string) {
	if _, err := daemon.SdNotify(false, state); err != nil {
		level.Warn(logger).Log("msg", "sd_notify failed", "state", state, "err", err) //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
	}
}
