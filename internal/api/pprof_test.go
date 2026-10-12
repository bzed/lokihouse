package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/api"
)

func TestPprofRoutes(t *testing.T) {
	srv := httptest.NewServer((&api.API{Pprof: true, AuthEnabled: true}).Router())
	t.Cleanup(srv.Close)
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/heap?gc=1", "/debug/pprof/goroutine?debug=1"} {
		code, body := do(t, http.MethodGet, srv.URL+p, "", "")
		require.Equal(t, http.StatusOK, code, p)
		require.NotEmpty(t, body, p)
	}
	off := httptest.NewServer((&api.API{AuthEnabled: true}).Router())
	t.Cleanup(off.Close)
	code, _ := do(t, http.MethodGet, off.URL+"/debug/pprof/heap", "", "")
	require.Equal(t, http.StatusNotFound, code)
}
