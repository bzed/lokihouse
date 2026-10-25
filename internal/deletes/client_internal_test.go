package deletes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Loki's cache never calls these; they only satisfy its CompactorClient.
func TestCompactorClient_Stubs(t *testing.T) {
	c := compactorClient{}
	gen, err := c.GetCacheGenerationNumber(t.Context(), "t1")
	require.NoError(t, err)
	require.Empty(t, gen)
	require.Equal(t, "lokihouse", c.Name())
	c.Stop()
}
