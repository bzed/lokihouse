package build

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfo(t *testing.T) {
	// Set some test values
	Version = "1.0.0"
	Revision = "abc123"
	Branch = "main"
	BuildUser = "testuser"
	BuildDate = "2024-01-01T00:00:00Z"
	// GoVersion is set by init() to runtime.Version()

	info := Info()

	require.Equal(t, "1.0.0", info["version"])
	require.Equal(t, "abc123", info["revision"])
	require.Equal(t, "main", info["branch"])
	require.Equal(t, "testuser", info["buildUser"])
	require.Equal(t, "2024-01-01T00:00:00Z", info["buildDate"])
	require.NotEmpty(t, info["goVersion"])
	require.Len(t, info, 6)
}
