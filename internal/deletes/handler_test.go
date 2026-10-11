package deletes_test

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/deletes"
)

func newHandler(t *testing.T, mutate func(*validation.Limits)) *deletes.Handler {
	s := newStore(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	if mutate != nil {
		mutate(&l)
	}
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return &deletes.Handler{Store: s, Limits: o, MaxInterval: time.Hour, CancelPeriod: time.Hour}
}

// doTenant issues req with the tenant injected into the context, like the API's tenant wrapper.
func doTenant(t *testing.T, h http.Handler, method, target, tenant string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), tenant))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestHandler_AddListCancel(t *testing.T) {
	h := newHandler(t, nil)
	start := t0.Truncate(time.Second)
	end := t0.Add(time.Minute).Truncate(time.Second)
	qs := url.Values{"query": {`{app="x"} |= "secret"`},
		"start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}}

	// Add.
	w := doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?"+qs.Encode(), "t1")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	id := w.Header().Get("X-Delete-Request-ID")
	require.NotEmpty(t, id)

	// List.
	w = doTenant(t, h.Gate(http.HandlerFunc(h.List)), "GET", "/loki/api/v1/delete", "t1")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var reqs []struct {
		RequestID string `json:"request_id"`
		StartTime model.Time `json:"start_time"`
		EndTime   model.Time `json:"end_time"`
		Query     string     `json:"query"`
		Status    string     `json:"status"`
		CreatedAt model.Time `json:"created_at"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reqs))
	require.Len(t, reqs, 1)
	require.Equal(t, id, reqs[0].RequestID)
	require.Equal(t, `{app="x"} |= "secret"`, reqs[0].Query)
	require.Equal(t, model.Time(start.UnixMilli()), reqs[0].StartTime)
	require.Equal(t, model.Time(end.UnixMilli()), reqs[0].EndTime)
	require.Equal(t, "received", reqs[0].Status)

	// Cancel (force=true because the fixed store clock puts the request past
	// the cancel period; without it Loki's check 400s).
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Cancel)), "DELETE", "/loki/api/v1/delete?request_id="+id+"&force=true", "t1")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())

	// Cancel again: not found.
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Cancel)), "DELETE", "/loki/api/v1/delete?request_id="+id, "t1")
	require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
	require.Equal(t, "could not find delete request with given id\n", w.Body.String())
}

func TestHandler_Errors(t *testing.T) {
	h := newHandler(t, nil)
	start := t0.Truncate(time.Second)
	end := t0.Add(time.Minute).Truncate(time.Second)

	// Missing query.
	w := doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?start="+strconv.FormatInt(start.Unix(), 10), "t1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "query not set\n", w.Body.String())

	// End in the future.
	future := t0.Add(24 * time.Hour)
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?query="+url.QueryEscape(`{app="x"}`)+"&start="+strconv.FormatInt(start.Unix(), 10)+"&end="+strconv.FormatInt(future.Unix(), 10), "t1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "deletes in the future are not allowed\n", w.Body.String())

	// Start >= end.
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?query="+url.QueryEscape(`{app="x"}`)+"&start="+strconv.FormatInt(end.Unix(), 10)+"&end="+strconv.FormatInt(end.Unix(), 10), "t1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "start time can't be greater than or equal to end time\n", w.Body.String())

	// max_interval larger than the configured maximum (Loki checks that before
	// the interval-to-delete check).
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?query="+url.QueryEscape(`{app="x"} |= "s"`)+"&start="+strconv.FormatInt(start.Unix(), 10)+"&end="+strconv.FormatInt(end.Unix(), 10)+"&max_interval=2h", "t1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "max_interval can't be greater than 1h0m0s\n", w.Body.String())

	// max_interval larger than the interval to delete (with the configured max
	// large enough not to trip the check above).
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?query="+url.QueryEscape(`{app="x"} |= "s"`)+"&start="+strconv.FormatInt(start.Unix(), 10)+"&end="+strconv.FormatInt(end.Unix(), 10)+"&max_interval=30m", "t1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "max_interval can't be greater than the interval to be deleted (1m0s)\n", w.Body.String())
}

func TestHandler_CancelPeriod(t *testing.T) {
	// CancelPeriod 1ns: the cancel deadline has already passed.
	s := newStore(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	h := &deletes.Handler{Store: s, Limits: o, MaxInterval: time.Hour, CancelPeriod: time.Nanosecond}
	start := t0.Truncate(time.Second)
	end := t0.Add(time.Minute).Truncate(time.Second)
	qs := url.Values{"query": {`{app="x"}`}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}}
	w := doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?"+qs.Encode(), "t1")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	id := w.Header().Get("X-Delete-Request-ID")

	// Without force: past the cancel period → 400.
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Cancel)), "DELETE", "/loki/api/v1/delete?request_id="+id, "t1")
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	// With force: 204.
	w = doTenant(t, h.Gate(http.HandlerFunc(h.Cancel)), "DELETE", "/loki/api/v1/delete?request_id="+id+"&force=true", "t1")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
}

func TestHandler_DeletionDisabled(t *testing.T) {
	h := newHandler(t, func(l *validation.Limits) { l.DeletionMode = "disabled" })
	w := doTenant(t, h.Gate(http.HandlerFunc(h.Add)), "POST", "/loki/api/v1/delete?query="+url.QueryEscape(`{app="x"}`), "t1")
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.Equal(t, "deletion is not available for this tenant\n", w.Body.String())
}
