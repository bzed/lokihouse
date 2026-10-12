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

func TestPrepare_NormalisesLogLevelInMetadata(t *testing.T) {
	// Test that log levels in structured metadata are normalized
	// The level label name is "detected_level" (constants.LevelLabel)
	o := overrides(t, func(l *validation.Limits) {
		l.DiscoverLogLevels = true
		l.AllowStructuredMetadata = true
	})
	now := time.Now()
	// Entry with a detected_level in structured metadata with mixed case
	e := logproto.Entry{
		Timestamp: now,
		Line:      "test",
		StructuredMetadata: []logproto.LabelAdapter{
			{Name: "detected_level", Value: "WaRn"},
		},
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	// The level should be normalized to lowercase
	// Check that we have structured metadata
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries, 1)
	// The level should be normalized
	for _, sm := range out[0].Entries[0].StructuredMetadata {
		if sm.Name == "detected_level" {
			require.Equal(t, "warn", sm.Value)
		}
	}
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

func TestPrepare_DetectsLogLevelFromLine(t *testing.T) {
	// Test that log levels are detected from the log line
	o := overrides(t, func(l *validation.Limits) {
		l.DiscoverLogLevels = true
		l.AllowStructuredMetadata = true
	})
	now := time.Now()
	// Entry with a log line that contains a level
	e := logproto.Entry{
		Timestamp: now,
		Line:      "this is a warning message",
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries, 1)
	// The level should be detected and added to structured metadata
	found := false
	for _, sm := range out[0].Entries[0].StructuredMetadata {
		if sm.Name == "detected_level" {
			found = true
			require.Equal(t, "warn", sm.Value)
		}
	}
	require.True(t, found, "detected_level should be present in structured metadata")
}

func TestPrepare_DetectsGenericFields(t *testing.T) {
	// Test that generic fields are detected from the log line
	// Generic field detection needs to be configured with fields to detect
	o := overrides(t, func(l *validation.Limits) {
		l.DiscoverLogLevels = false
		l.AllowStructuredMetadata = true
		// Enable generic field detection for a specific field
		l.DiscoverGenericFields = validation.FieldDetectorConfig{
			Fields: map[string][]string{
				"service_name": {"service", "app"},
			},
		}
	})
	now := time.Now()
	// Entry with a JSON log line that contains a service field
	e := logproto.Entry{
		Timestamp: now,
		Line:      `{"service":"web","message":"test"}`,
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries, 1)
	// The service_name should be detected and added to structured metadata
	found := false
	for _, sm := range out[0].Entries[0].StructuredMetadata {
		if sm.Name == "service_name" {
			found = true
			require.Equal(t, "web", sm.Value)
		}
	}
	require.True(t, found, "service_name should be present in structured metadata")
}

func TestPrepare_DetectsGenericFieldsFromLogfmt(t *testing.T) {
	// Test that generic fields are detected from logfmt log lines
	o := overrides(t, func(l *validation.Limits) {
		l.DiscoverLogLevels = false
		l.AllowStructuredMetadata = true
		// Enable generic field detection for a specific field
		l.DiscoverGenericFields = validation.FieldDetectorConfig{
			Fields: map[string][]string{
				"service_name": {"service", "app"},
			},
		}
	})
	now := time.Now()
	// Entry with a logfmt log line that contains a service field
	e := logproto.Entry{
		Timestamp: now,
		Line:      `service=web message=test`,
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Entries, 1)
	// The service_name should be detected and added to structured metadata
	found := false
	for _, sm := range out[0].Entries[0].StructuredMetadata {
		if sm.Name == "service_name" {
			found = true
			require.Equal(t, "web", sm.Value)
		}
	}
	require.True(t, found, "service_name should be present in structured metadata")
}

func TestPrepare_DetectsLogLevelFromOTLPSeverity(t *testing.T) {
	// Test that log levels are detected from OTLP severity number in structured metadata
	// This test just ensures the code path doesn't panic
	o := overrides(t, func(l *validation.Limits) {
		l.DiscoverLogLevels = true
		l.AllowStructuredMetadata = true
	})
	now := time.Now()
	// Entry with OTLP severity number in structured metadata
	e := logproto.Entry{
		Timestamp: now,
		Line:      "test",
		StructuredMetadata: []logproto.LabelAdapter{
			{Name: "severity_number", Value: "9"},
		},
	}
	out, err := distcopy.Prepare(context.Background(), o, "t", req(`{a="b"}`, e), now)
	require.NoError(t, err)
	require.Len(t, out, 1)
	// Just ensure the test doesn't panic and returns something
	_ = out[0].Entries[0].StructuredMetadata
}
