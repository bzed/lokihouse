// Package patterns serves /loki/api/v1/patterns by running Loki's Drain over the
// stored rows at query time, configured and iterated like Loki's pattern ingester.
package patterns

import (
	"context"
	"flag"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/pattern"
	"github.com/grafana/loki/v3/pkg/pattern/drain"
	"github.com/grafana/loki/v3/pkg/pattern/iter"
	querierpattern "github.com/grafana/loki/v3/pkg/querier/pattern"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/common/model"

	"github.com/bzed/lokihouse/internal/chstore"
)

// Querier answers pattern queries. MaxLines caps the rows read per query
// (default 1_000_000); rows past it are not trained (docs/divergences.md).
type Querier struct {
	Q        *chstore.Querier
	Limits   drain.Limits
	MaxLines int
	Logger   log.Logger // optional; when set, MaxLines truncation is logged
}

func (p *Querier) logger() log.Logger {
	if p.Logger == nil {
		return log.NewNopLogger()
	}
	return p.Logger
}

// Copied from grafana/loki@fbc3f077 pkg/pattern/ingester_querier.go (minClusterSize, maxPatterns).
const (
	minClusterSize = 30
	maxPatterns    = 300
)

func drainConfig() *drain.Config {
	var pc pattern.Config
	pc.RegisterFlags(flag.NewFlagSet("pattern", flag.PanicOnError))
	// Configured exactly as pkg/pattern/ingester.go does it.
	cfg := drain.DefaultConfig()
	cfg.MaxClusters = pc.MaxClusters
	cfg.MaxEvictionRatio = pc.MaxEvictionRatio
	cfg.MaxChunkAge = pc.MaxChunkAge
	cfg.SampleInterval = pc.PatternSampleInterval
	return cfg
}

type streamDrains struct {
	format string
	byLvl  map[string]*drain.Drain
}

func (p *Querier) Query(ctx context.Context, req *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	if _, err := syntax.ParseMatchers(req.Query, true); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	expr, err := syntax.ParseLogSelector(req.Query, true)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	it, err := p.Q.SelectLogs(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: req.Query, Start: req.Start, End: req.End, Direction: logproto.FORWARD,
		Plan: &plan.QueryPlan{AST: expr},
	}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	cfg := drainConfig()
	maxLines := p.MaxLines
	if maxLines == 0 {
		maxLines = 1_000_000
	}
	streams := map[string]*streamDrains{}
	var order []string
	n := 0
	for ; n < maxLines && it.Next(); n++ {
		e := it.At()
		key := it.Labels()
		sd := streams[key]
		if sd == nil {
			sd = &streamDrains{format: drain.DetectLogFormat(e.Line), byLvl: map[string]*drain.Drain{}}
			streams[key] = sd
			order = append(order, key)
		}
		lvl := constants.LogLevelUnknown
		for _, md := range e.StructuredMetadata {
			if md.Name == constants.LevelLabel {
				lvl = strings.ToLower(md.Value)
			}
		}
		if !slices.Contains(constants.LogLevels, lvl) {
			lvl = constants.LogLevelUnknown
		}
		d := sd.byLvl[lvl]
		if d == nil {
			d = drain.New(tenantID, cfg, p.Limits, sd.format, nil)
			sd.byLvl[lvl] = d
		}
		d.Train(e.Line, e.Timestamp.UnixNano())
	}
	if n == maxLines && it.Next() {
		level.Warn(p.logger()).Log("msg", "pattern query truncated at MaxLines; patterns may be incomplete", "max_lines", maxLines)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	from, through := util.RoundToMilliseconds(req.Start, req.End)
	step := model.Time(req.Step)
	if step < drain.TimeResolution {
		step = drain.TimeResolution
	}
	sampleInterval := model.Time(cfg.SampleInterval.Milliseconds())
	var iters []iter.Iterator
	for _, key := range order {
		for lvl, d := range streams[key].byLvl {
			for _, c := range d.Clusters() {
				if c.String() == "" {
					continue
				}
				iters = append(iters, c.Iterator(lvl, from, through, step, sampleInterval))
			}
		}
	}
	resp, err := iter.ReadBatch(iter.NewMerge(iters...), math.MaxInt32)
	if err != nil {
		return nil, err
	}
	return querierpattern.MergePatternResponses([]*logproto.QueryPatternsResponse{prunePatterns(resp)}), nil
}

// Copied from grafana/loki@fbc3f077 pkg/pattern/ingester_querier.go (prunePatterns);
// modifications: the metrics parameter is dropped (lokihouse has no pattern metrics).
func prunePatterns(resp *logproto.QueryPatternsResponse) *logproto.QueryPatternsResponse {
	total := make([]int64, len(resp.Series))

	for i, p := range resp.Series {
		for _, s := range p.Samples {
			total[i] += s.Value
		}
	}

	type SeriesWithTotal struct {
		Series *logproto.PatternSeries
		Total  int64
	}

	seriesWithTotals := make([]SeriesWithTotal, len(resp.Series))
	for i := range resp.Series {
		seriesWithTotals[i] = SeriesWithTotal{
			Series: resp.Series[i],
			Total:  total[i],
		}
	}

	sort.Slice(seriesWithTotals, func(i, j int) bool {
		return seriesWithTotals[i].Total > seriesWithTotals[j].Total
	})

	pos := 0
	for i := range seriesWithTotals {
		if seriesWithTotals[i].Total >= minClusterSize {
			resp.Series[pos] = seriesWithTotals[i].Series
			pos++
		}
	}
	resp.Series = resp.Series[:pos]

	if len(resp.Series) > maxPatterns {
		resp.Series = resp.Series[:maxPatterns]
	}

	return resp
}
