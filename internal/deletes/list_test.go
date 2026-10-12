package deletes_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/deletes"
)

func TestHandler_List(t *testing.T) {
	conn, _ := chtest.New(t)
	store := &deletes.Store{Conn: conn}
	h := &deletes.Handler{Store: store, Limits: nil, MaxInterval: time.Hour, CancelPeriod: time.Hour}

	// Test that the handler doesn't panic
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/loki/api/v1/delete", nil)
	req.Header.Set("X-Scope-OrgID", "t1")
	rr := httptest.NewRecorder()
	h.List(rr, req)
	// Just check that it returns some status code without panicking
	require.NotEqual(t, 0, rr.Code)
}

func TestHandler_ListWithQuery(t *testing.T) {
	conn, _ := chtest.New(t)
	store := &deletes.Store{Conn: conn}
	h := &deletes.Handler{Store: store, Limits: nil, MaxInterval: time.Hour, CancelPeriod: time.Hour}

	// Test with a query parameter
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/loki/api/v1/delete?start=1h&end=now", nil)
	req.Header.Set("X-Scope-OrgID", "t1")
	rr := httptest.NewRecorder()
	h.List(rr, req)
	require.NotEqual(t, 0, rr.Code)
}
