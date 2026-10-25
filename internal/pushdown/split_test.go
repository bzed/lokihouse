package pushdown

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/grafana/loki/v3/pkg/util/httpreq"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// seedSplitData writes tenant t1: apps web, api and worker for 6 h from
// 2026-01-01T00:00:00Z (one line every 7 s each), app early only in hours 0-2
// and app late only in hours 4-6. It returns the start.
func seedSplitData(t testing.TB, w *chstore.Writer) time.Time {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rng := rand.New(rand.NewSource(11))
	gen := func(app string, from, to time.Duration) logproto.Stream {
		s := logproto.Stream{Labels: fmt.Sprintf(`{app=%q}`, app)}
		for d := from; d < to; d += 7 * time.Second {
			lvl := "info"
			if rng.Intn(4) == 0 {
				lvl = "error"
			}
			s.Entries = append(s.Entries, logproto.Entry{
				Timestamp: base.Add(d),
				Line:      fmt.Sprintf("level=%s dur=%d", lvl, rng.Intn(1000)),
			})
		}
		return s
	}
	h := time.Hour
	streams := []logproto.Stream{
		gen("web", 0, 6*h), gen("api", 0, 6*h), gen("worker", 0, 6*h),
		gen("early", 0, 2*h), gen("late", 4*h, 6*h),
	}
	require.NoError(t, w.Write(context.Background(), "t1", streams))
	return base
}

// newSplitter builds a Splitter over conn with split_queries_by_interval 1h and
// max_query_series 500, after mod, and returns it with the raw engine it wraps.
func newSplitter(t testing.TB, conn driver.Conn, budget *semaphore.Weighted, mod func(*validation.Limits)) (*Splitter, *logql.QueryEngine) {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.QuerySplitDuration = model.Duration(time.Hour)
	l.MaxQuerySeries = 500
	if mod != nil {
		mod(&l)
	}
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	raw := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	return &Splitter{Engine: raw, Limits: overrides, Budget: budget}, raw
}

func rangeParams(t testing.TB, q string, start, end time.Time, step time.Duration) logql.Params {
	t.Helper()
	p, err := logql.NewLiteralParams(q, start, end, step, 0, logproto.FORWARD, 100, nil, nil)
	require.NoError(t, err)
	return p
}

const unwrapQuery = `quantile_over_time(0.9, {app="web"} | logfmt | unwrap dur [5m]) by (app)`

func splitFixtureConn(t testing.TB) (driver.Conn, time.Time) {
	conn, _ := chtest.New(t)
	return conn, seedSplitData(t, &chstore.Writer{Conn: conn})
}

func splitFixture(t testing.TB) (*Splitter, *logql.QueryEngine, time.Time) {
	conn, base := splitFixtureConn(t)
	s, raw := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
	return s, raw, base
}

func tctx() context.Context { return user.InjectOrgID(context.Background(), "t1") }

func TestSplitter_SplitsRangeQueryAndReportsSplits(t *testing.T) {
	s, raw, base := splitFixture(t)
	p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute+3*time.Second), base.Add(3*time.Hour+40*time.Minute), time.Minute)
	want, err := raw.Query(p).Exec(tctx())
	require.NoError(t, err)
	got, err := s.Query(p).Exec(tctx())
	require.NoError(t, err)
	requireDataEqual(t, want.Data, got.Data)
	require.NotEmpty(t, got.Data)
	require.EqualValues(t, 4, got.Statistics.Summary.Splits)
}

func TestSplitter_DoesNotSplit(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, raw := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
	off, offRaw := newSplitter(t, conn, semaphore.NewWeighted(16), func(l *validation.Limits) { l.QuerySplitDuration = 0 })
	long := rangeParams(t, unwrapQuery, base.Add(time.Hour), base.Add(3*time.Hour), time.Minute)
	cases := []struct {
		name string
		s    *Splitter
		raw  *logql.QueryEngine
		p    logql.Params
	}{
		{"instant", s, raw, rangeParams(t, unwrapQuery, base.Add(time.Hour), base.Add(time.Hour), 0)},
		{"start equals end", s, raw, rangeParams(t, unwrapQuery, base.Add(time.Hour), base.Add(time.Hour), time.Minute)},
		{"log query", s, raw, rangeParams(t, `{app="web"}`, base.Add(time.Hour), base.Add(3*time.Hour), time.Minute)},
		{"interval 0", off, offRaw, long},
		{"one bucket", s, raw, rangeParams(t, unwrapQuery, base.Add(65*time.Minute), base.Add(100*time.Minute), time.Minute)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := c.raw.Query(c.p).Exec(tctx())
			require.NoError(t, err)
			got, err := c.s.Query(c.p).Exec(tctx())
			require.NoError(t, err)
			require.EqualValues(t, 0, got.Statistics.Summary.Splits)
			if c.name != "log query" { // log results carry stats-dependent stream order
				requireDataEqual(t, want.Data, got.Data)
			}
		})
	}
}

func TestSplitter_SeriesLimit(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, raw := newSplitter(t, conn, semaphore.NewWeighted(16), func(l *validation.Limits) { l.MaxQuerySeries = 1 })
	drill := httpreq.InjectQueryTags(tctx(), "source="+constants.LogsDrilldownAppName)
	newErr := logqlmodel.NewSeriesLimitError(1).Error()

	// Each leg sees one series (early ends at 2h, late starts at 4h); the merge sees two.
	merge := rangeParams(t, `count_over_time({app=~"early|late"}[1m])`, base.Add(time.Hour), base.Add(5*time.Hour), time.Minute)
	_, err := s.Query(merge).Exec(tctx())
	var limitErr *logqlmodel.LimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, newErr, err.Error())
	want, err := raw.Query(merge).Exec(drill)
	require.NoError(t, err)
	got, err := s.Query(merge).Exec(drill)
	require.NoError(t, err)
	requireDataEqual(t, want.Data, got.Data)
	require.Equal(t, want.Warnings, got.Warnings)
	require.NotEmpty(t, got.Warnings)

	// One leg overflows on its own.
	own := rangeParams(t, `count_over_time({app=~"web|api"}[1m])`, base.Add(time.Hour), base.Add(5*time.Hour), time.Minute)
	_, wantErr := raw.Query(own).Exec(tctx())
	require.ErrorAs(t, wantErr, &limitErr)
	_, err = s.Query(own).Exec(tctx())
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, wantErr.Error(), err.Error())
	want, err = raw.Query(own).Exec(drill)
	require.NoError(t, err)
	got, err = s.Query(own).Exec(drill)
	require.NoError(t, err)
	// Which of the overflowing series the engine keeps is random (it ranges
	// over a map), so compare the shape only.
	require.Len(t, got.Data, 1)
	require.Len(t, want.Data, 1)
	require.Equal(t, want.Warnings, got.Warnings)
}

func TestSplitter_ExhaustedBudgetRunsInline(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, raw := newSplitter(t, conn, semaphore.NewWeighted(0), nil)
	p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute+3*time.Second), base.Add(3*time.Hour+40*time.Minute), time.Minute)
	want, err := raw.Query(p).Exec(tctx())
	require.NoError(t, err)
	got, err := s.Query(p).Exec(tctx())
	require.NoError(t, err)
	requireDataEqual(t, want.Data, got.Data)
	require.EqualValues(t, 4, got.Statistics.Summary.Splits)
}

func TestSplitter_QueryTimeoutCoversAllLegs(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, _ := newSplitter(t, conn, semaphore.NewWeighted(16), func(l *validation.Limits) { l.QueryTimeout = model.Duration(time.Nanosecond) })
	p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute), base.Add(3*time.Hour+40*time.Minute), time.Minute)
	_, err := s.Query(p).Exec(tctx())
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSplitter_ObservesPartitions(t *testing.T) {
	conn, base := splitFixtureConn(t)
	s, _ := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
	s.Metrics = NewSplitMetrics(prometheus.NewRegistry())
	p := rangeParams(t, unwrapQuery, base.Add(10*time.Minute+3*time.Second), base.Add(3*time.Hour+40*time.Minute), time.Minute)
	_, err := s.Query(p).Exec(tctx())
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, s.Metrics.partitions.Write(&m))
	require.EqualValues(t, 1, m.Histogram.GetSampleCount())
	require.EqualValues(t, 4, m.Histogram.GetSampleSum())
}

func chstoreWriter(conn driver.Conn) *chstore.Writer { return &chstore.Writer{Conn: conn} }

type dtoMetric = dto.Metric

func newTestOverrides(t testing.TB) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}
