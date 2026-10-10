package ingest_test

import (
	"context"
	"flag"
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

func TestPush_TenantRetention(t *testing.T) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	tenants := map[string]*validation.Limits{"short": {}}
	*tenants["short"] = l
	require.NoError(t, tenants["short"].RetentionPeriod.Set("36h"))
	o, err := validation.NewOverrides(l, fakeTenantLimits(tenants))
	require.NoError(t, err)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()}
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(time.Now().UnixNano()) + `","x"]]}]}`
	for _, tn := range []string{"short", "default"} {
		rec := push(t, h, tn, body)
		require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	}
	var short, def uint16
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT anyIf(retention_days, tenant='short'), anyIf(retention_days, tenant='default') FROM logs").Scan(&short, &def))
	require.Equal(t, uint16(2), short, "36h rounds up to 2 days")
	require.Equal(t, uint16(0), def, "Loki default retention_period 0 keeps data forever")
}

type fakeTenantLimits map[string]*validation.Limits

func (f fakeTenantLimits) TenantLimits(id string) *validation.Limits  { return f[id] }
func (f fakeTenantLimits) AllByUserID() map[string]*validation.Limits { return f }

func itoa(i int64) string { return strconv.FormatInt(i, 10) }
