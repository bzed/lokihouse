package lokidep

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

// Guards that the pinned Loki LogQL parser is importable and behaves as expected.
func TestLokiParserImport(t *testing.T) {
	e, err := syntax.ParseExpr(`sum by (app) (rate({app="a"} |= "x" | json [5m]))`)
	require.NoError(t, err)
	require.Equal(t, `sum by (app)(rate({app="a"} |= "x" | json[5m]))`, e.String())
}
