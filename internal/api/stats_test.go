package api_test

import (
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/lokihouse/internal/api"
	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
	"github.com/bzed/lokihouse/internal/pushdown"
)

// statsSummary is one query response's data.stats.summary.
type statsSummary struct {
	TotalBytesProcessed int64 `json:"totalBytesProcessed"`
	TotalLinesProcessed int64 `json:"totalLinesProcessed"`
}

// TestQueryStats_ReportsClickHouseWork runs the same fixture query through the
// API in all three pushdown modes and requires the response's stats.summary to
// carry positive ClickHouse work: the plain engine and Tier A both feed their
// ClickHouse read progress into Loki's stats context.
func TestQueryStats_ReportsClickHouseWork(t *testing.T) {
	for _, mode := range []string{"off", "downstream", "on"} {
		t.Run(mode, func(t *testing.T) {
			conn, _ := chtest.New(t)
			var defaults validation.Limits
			defaults.RegisterFlags(flag.NewFlagSet("defaults", flag.PanicOnError))
			limits, err := validation.NewOverrides(defaults, nil)
			require.NoError(t, err)
			q := &chstore.Querier{Conn: conn}
			logger := log.NewNopLogger()
			srv := httptest.NewServer((&api.API{
				Queries: &pushdown.Engine{
					Mode:   pushdown.Mode(mode),
					TierB:  logql.NewEngine(logql.EngineOpts{}, q, limits, logger),
					TierA:  &pushdown.TierA{Conn: conn, Limits: limits},
					Limits: limits,
					Logger: logger,
				},
				Push:   &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: limits, Logger: logger},
				Limits: limits,
			}).Router())
			defer srv.Close()

			now := time.Now().Truncate(time.Second)
			tenant := "t1"
			require.Equal(t, http.StatusNoContent, pushLines(t, srv, tenant, now))

			v := url.Values{
				"query": {`sum(count_over_time({app="a"}[5m]))`},
				"start": {strconv.FormatInt(now.Add(-2*time.Hour).UnixNano(), 10)},
				"end":   {strconv.FormatInt(now.Add(5*time.Minute).UnixNano(), 10)},
				"step":  {"60s"},
			}
			req, err := http.NewRequest(http.MethodGet, srv.URL+"/loki/api/v1/query_range?"+v.Encode(), nil)
			require.NoError(t, err)
			req.Header.Set("X-Scope-OrgID", tenant)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Logf("query %d: %s", resp.StatusCode, body)
			}
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var res struct {
				Data struct {
					Stats struct {
						Summary statsSummary `json:"summary"`
					} `json:"stats"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(body, &res))
			require.Positive(t, res.Data.Stats.Summary.TotalBytesProcessed)
			require.Positive(t, res.Data.Stats.Summary.TotalLinesProcessed)
		})
	}
}

// pushLines pushes ten identical lines at now and requires 204.
func pushLines(t *testing.T, srv *httptest.Server, tenant string, now time.Time) int {
	t.Helper()
	var values []string
	for i := 0; i < 10; i++ {
		values = append(values, `["`+strconv.FormatInt(now.UnixNano(), 10)+`","l"]`)
	}
	body := `{"streams":[{"stream":{"app":"a"},"values":[` + strings.Join(values, ",") + `]}]}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/loki/api/v1/push", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenant)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Logf("push %d: %s", resp.StatusCode, b)
	}
	return resp.StatusCode
}
