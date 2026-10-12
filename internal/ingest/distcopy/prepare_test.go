package distcopy_test

import (
	"context"
	"flag"
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
	l.RegisterFlags(flag.NewFlagSet("l", flag.PanicOnError))
	// Set reasonable defaults to avoid validation errors
	l.MaxLabelNamesPerSeries = 100
	l.MaxLabelNameLength = 1000
	l.MaxLabelValueLength = 10000
	l.AllowStructuredMetadata = true
	l.MaxLineSize = 1000000 // 1MB
	l.MaxLineSizeTruncate = true
	l.MaxLineSizeTruncateIdentifier = "..."
	l.IncrementDuplicateTimestamp = false
	if mut != nil {
		mut(&l)
	}
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

func TestPrepare_TruncatesLongLines(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.MaxLineSize = 5; l.MaxLineSizeTruncate = true })
	now := time.Now()
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "0123456789"}), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	// Loki appends its truncation marker
	// Just check that the line is truncated
	require.LessOrEqual(t, len(out[0].Entries[0].Line), 8) // Should be truncated from 10 chars
}

func TestPrepare_MissingEnforcedLabels(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.EnforcedLabels = []string{"team"} })
	now := time.Now()
	_, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "x"}), now)
	// With enforced labels missing, this should return an error
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required labels")
}

func TestPrepare_IncrementsDuplicateTimestamps(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) { l.IncrementDuplicateTimestamp = true })
	now := time.Now().Truncate(time.Second)
	out, err := distcopy.Prepare(context.Background(), o, "t",
		req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "1"}, logproto.Entry{Timestamp: now, Line: "2"}), now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Nanosecond), out[0].Entries[1].Timestamp)
}

func TestPrepare_NormalisesStructuredMetadataNames(t *testing.T) {
	o := overrides(t, func(*validation.Limits) {})
	now := time.Now()
	e := logproto.Entry{Timestamp: now, Line: "x", StructuredMetadata: []logproto.LabelAdapter{{Name: "trace-id", Value: "1"}}}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Equal(t, "trace_id", out[0].Entries[0].StructuredMetadata[0].Name)
}

func TestPrepare_MultipleStreams(t *testing.T) {
	o := overrides(t, func(*validation.Limits) {})
	now := time.Now()
	req := &logproto.PushRequest{
		Streams: []logproto.Stream{
			{Labels: `{a="b"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "test1"}}},
			{Labels: `{c="d"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "test2"}}},
		},
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", logproto.FromPushRequest(req), now)
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestPrepare_DuplicateTimestampsIncremented(t *testing.T) {
	o := overrides(t, func(l *validation.Limits) {
		l.IncrementDuplicateTimestamp = true
	})
	now := time.Now().Truncate(time.Second)
	entries := []logproto.Entry{
		{Timestamp: now, Line: "1"},
		{Timestamp: now, Line: "2"},
		{Timestamp: now, Line: "3"},
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, entries...), now)
	require.NoError(t, err)
	require.Len(t, out[0].Entries, 3)
	// Each duplicate should be incremented by 1ns
	require.Equal(t, now, out[0].Entries[0].Timestamp)
	require.Equal(t, now.Add(time.Nanosecond), out[0].Entries[1].Timestamp)
	require.Equal(t, now.Add(2*time.Nanosecond), out[0].Entries[2].Timestamp)
}

func TestPrepare_EmptyEntry(t *testing.T) {
	o := overrides(t, func(*validation.Limits) {})
	now := time.Now()
	// Empty line should still be processed
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: ""}), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
}
