package distcopy_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/ingest/distcopy"
)

func overrides(t *testing.T, mut func(*validation.Limits)) *validation.Overrides {
	t.Helper()
	var l validation.Limits
	// Set reasonable defaults
	l.MaxLabelNamesPerSeries = 100
	l.MaxLabelNameLength = 1000
	l.MaxLabelValueLength = 10000
	l.AllowStructuredMetadata = true
	l.MaxLineSize = 1000000 // 1MB
	l.MaxLineSizeTruncate = true
	l.MaxLineSizeTruncateIdentifier = "..."
	mut(&l)
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func req(labels string, entries ...logproto.Entry) *logproto.InternalPushRequest {
	r := &logproto.PushRequest{Streams: []logproto.Stream{{Labels: labels, Entries: entries}}}
	return logproto.FromPushRequest(r)
}

func TestPrepare_Basic(t *testing.T) {
	o := overrides(t, func(*validation.Limits) {})
	now := time.Now()
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "test"}), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries, 1)
	require.Equal(t, "test", out[0].Entries[0].Line)
}
