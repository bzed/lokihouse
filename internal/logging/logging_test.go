package logging_test

import (
	"bytes"
	"testing"

	"github.com/go-kit/log/level"
	dskitlog "github.com/grafana/dskit/log"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/logging"
)

func TestNew_LevelAndFormat(t *testing.T) {
	var lvl dskitlog.Level
	require.NoError(t, lvl.Set("warn"))
	var buf bytes.Buffer
	l, err := logging.New(&buf, lvl, "json")
	require.NoError(t, err)
	level.Info(l).Log("msg", "hidden")
	level.Warn(l).Log("msg", "shown")
	require.NotContains(t, buf.String(), "hidden")
	require.Contains(t, buf.String(), `"msg":"shown"`)

	_, err = logging.New(&buf, lvl, "xml")
	require.Error(t, err)
}
