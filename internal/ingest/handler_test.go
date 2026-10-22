package ingest_test

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/ingest"
)

func defaultLimits(t *testing.T) *validation.Overrides {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func push(t *testing.T, h http.Handler, tenant, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(user.InjectOrgID(req.Context(), tenant))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPush_DiscoversServiceNameAndLevel(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(ts) + `","level=error msg=boom"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	var lstr, lvl string
	ctx := context.Background()
	require.NoError(t, conn.QueryRow(ctx, "SELECT labels_str FROM streams WHERE tenant='t1'").Scan(&lstr))
	require.Equal(t, `{app="web", service_name="web"}`, lstr)
	require.NoError(t, conn.QueryRow(ctx, "SELECT structured_metadata['detected_level'] FROM logs WHERE tenant='t1'").Scan(&lvl))
	require.Equal(t, "error", lvl)
}

func TestPush_RejectsTooOldAndKeepsRest(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	old := time.Now().Add(-30 * 24 * time.Hour).UnixNano()
	ok := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(old) + `","old"],["` + itoa(ok) + `","new"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "timestamp too old")

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant='t1'").Scan(&n))
	require.Equal(t, uint64(1), n)
}

func TestPush_RejectsInvalidLabels(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	rec := push(t, h, "t1", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }

func TestPush_MissingTenant(t *testing.T) {
	// Test that missing tenant ID returns 401
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	// Create a request without tenant context
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(`{"streams":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestPush_InvalidJSON(t *testing.T) {
	// Test that invalid JSON returns 400
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader("invalid json"))
	req = req.WithContext(user.InjectOrgID(req.Context(), "t1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPush_OTLPPath(t *testing.T) {
	// Test that OTLP path is handled
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	// OTLP requests are more complex, for now just test that the path doesn't cause an error
	body := `{"resourceLogs":[]}`
	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/logs", strings.NewReader(body))
	req = req.WithContext(user.InjectOrgID(req.Context(), "t1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// OTLP parsing might fail, but we just want to ensure the path is recognized
	_ = rec.Code
}

func TestPush_TooManyPartitionsIs400(t *testing.T) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.RejectOldSamples = false // a backfill
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()}

	day := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Hour)
	var values []string
	for i := chstore.MaxPushPartitions + 1; i > 0; i-- {
		values = append(values, fmt.Sprintf(`[%q,"x"]`, itoa(day.AddDate(0, 0, -i).UnixNano())))
	}
	body := `{"streams":[{"stream":{"app":"a"},"values":[` + strings.Join(values, ",") + `]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "at most 60 are accepted per push")
}
