package ruler

import (
	"context"
	"hash/fnv"

	"github.com/go-kit/log"
	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	rulerbase "github.com/grafana/loki/v3/pkg/ruler/base"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore"
	"github.com/grafana/loki/v3/pkg/ruler/rulestore/local"
	"github.com/grafana/loki/v3/pkg/storage/bucket"
	chunk_util "github.com/grafana/loki/v3/pkg/storage/chunk/client/util"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
)

// New builds Loki's ruler over e, as pkg/loki/modules.go initRulerStorage,
// initRuleEvaluator (local mode) and initRuler do, plus Loki's ruler HTTP API.
// The ruler is a dskit service the caller starts and stops. Only one ruler may
// run per process: Loki keeps its remote-write registry in a package variable.
func New(ctx context.Context, rc lokiruler.Config, sc rulestore.Config, e Engine, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*rulerbase.Ruler, *rulerbase.API, error) {
	// Loki creates the directory of a local rule store before opening it.
	var dir string
	switch sc.Backend {
	case local.Name:
		dir = sc.Local.Directory
	case bucket.Filesystem:
		dir = sc.Filesystem.Directory
	}
	if dir != "" {
		if err := chunk_util.EnsureDirectory(dir); err != nil {
			return nil, nil, err
		}
	}
	store, err := rulerbase.NewRuleStore(ctx, sc, limits, lokiruler.GroupLoader{}, logger)
	if err != nil {
		return nil, nil, err
	}
	ev := lokiruler.NewEvaluatorWithJitter(&Evaluator{Engine: e}, rc.Evaluation.MaxJitter, fnv.New32a(), logger)
	r, err := lokiruler.NewRuler(rc, ev, reg, logger, store, limits, "lokihouse")
	if err != nil {
		return nil, nil, err
	}
	return r, rulerbase.NewAPI(r, store, logger), nil
}
