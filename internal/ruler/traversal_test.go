package ruler_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

func TestRules_PathTraversalRejected(t *testing.T) {
	// This test proves that path traversal attacks are rejected
	// as required by the govulncheck allowlist (GO-2026-5115)
	conn, _ := chtest.New(t)
	h := startRuler(t, t.TempDir(), engine(t, conn), nil)

	group := `{"name":"test-group","interval_seconds":1,"rules":[{"record":"up"}]}`

	for _, p := range []string{
		"/loki/api/v1/rules/..%2F..%2Fetc",
		"/loki/api/v1/rules/ns/..%2F..%2Fpasswd",
	} {
		code, body := h.do(t, http.MethodPost, p, "t1", group)
		require.NotEqual(t, http.StatusAccepted, code, "%s: %s", p, body)
	}
}
