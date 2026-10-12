// Copied from grafana/loki@fbc3f077 pkg/distributor/field_detection_test.go
// (the tests of the functions in field_detection.go); modifications: package
// distcopy; Distributor-based tests dropped; validationContext built with
// lokihouse's copy.

package distcopy

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_detectLogLevelFromLogEntry(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level", "LEVEL", "Level", "severity", "SEVERITY", "Severity", "lvl", "LVL", "Lvl"},
		})

	for _, tc := range []struct {
		name             string
		entry            string
		expectedLogLevel string
	}{
		{
			name:             "non otlp with log level keywords in log line",
			entry:            "this is a warning log",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with debug keyword in log line",
			entry:            "this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with DEBUG keyword in log line",
			entry:            "this is a DEBUG message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with debug: prefix in log line",
			entry:            "debug: something happened",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with DEBUG: prefix in log line",
			entry:            "DEBUG: something happened",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with critical keyword in log line",
			entry:            "this is a critical message",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with CRITICAL keyword in log line",
			entry:            "this is a CRITICAL message",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with critical: prefix in log line",
			entry:            "critical: something happened",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with CRITICAL: prefix in log line",
			entry:            "CRITICAL: something happened",
			expectedLogLevel: constants.LogLevelCritical,
		},
		{
			name:             "non otlp with info keyword in log line",
			entry:            "this is a info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with error keyword in log line",
			entry:            "this is a error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with fatal keyword in log line",
			entry:            "this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp without any of the log level keywords in log line",
			entry:            "foo",
			expectedLogLevel: constants.LogLevelUnknown,
		},
		{
			name:             "non otlp with [debug] bracket pattern in log line",
			entry:            "[debug] this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with [DEBUG] bracket pattern in log line",
			entry:            "[DEBUG] this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with [warn] bracket pattern in log line",
			entry:            "[warn] this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with [WARN] bracket pattern in log line",
			entry:            "[WARN] this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with [error] bracket pattern in log line",
			entry:            "[error] this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with [ERROR] bracket pattern in log line",
			entry:            "[ERROR] this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with [info] bracket pattern in log line",
			entry:            "[info] this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with [INFO] bracket pattern in log line",
			entry:            "[INFO] this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with [fatal] bracket pattern in log line",
			entry:            "[fatal] this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with [FATAL] bracket pattern in log line",
			entry:            "[FATAL] this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with <debug> angle bracket pattern in log line",
			entry:            "<debug> this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with <DEBUG> angle bracket pattern in log line",
			entry:            "<DEBUG> this is a debug message",
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "non otlp with <warn> angle bracket pattern in log line",
			entry:            "<warn> this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with <WARN> angle bracket pattern in log line",
			entry:            "<WARN> this is a warn message",
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "non otlp with <error> angle bracket pattern in log line",
			entry:            "<error> this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with <ERROR> angle bracket pattern in log line",
			entry:            "<ERROR> this is an error message",
			expectedLogLevel: constants.LogLevelError,
		},
		{
			name:             "non otlp with <info> angle bracket pattern in log line",
			entry:            "<info> this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with <INFO> angle bracket pattern in log line",
			entry:            "<INFO> this is an info message",
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "non otlp with <fatal> angle bracket pattern in log line",
			entry:            "<fatal> this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
		{
			name:             "non otlp with <FATAL> angle bracket pattern in log line",
			entry:            "<FATAL> this is a fatal message",
			expectedLogLevel: constants.LogLevelFatal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.entry,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLogLevelFromLogEntryWithCustomLabels(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"custom_level"},
		})

	for _, tc := range []struct {
		name             string
		entry            string
		expectedLogLevel string
	}{
		{
			name:             "custom level field in log line",
			entry:            `{"custom_level":"debug","message":"test"}`,
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "custom level field in log line uppercase",
			entry:            `{"custom_level":"DEBUG","message":"test"}`,
			expectedLogLevel: constants.LogLevelDebug,
		},
		{
			name:             "custom level field not present",
			entry:            `{"message":"test"}`,
			expectedLogLevel: constants.LogLevelUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.entry,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_DetectGenericFields_Enabled(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       false,
			allowStructuredMetadata: true,
			logLevelFields:          []string{},
		})

	entry := logproto.Entry{
		Timestamp: time.Now(),
		Line:      "this is a test message",
	}
	level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
	require.Equal(t, constants.LogLevelUnknown, level)
}

func Test_DetectGenericFields(t *testing.T) {
	// Test that generic field detection is working
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level"},
		})

	entry := logproto.Entry{
		Timestamp: time.Now(),
		Line:      `{"level":"info","message":"test"}`,
	}
	level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
	require.Equal(t, constants.LogLevelInfo, level)
}

func TestGetLevelUsingJsonParser(t *testing.T) {
	ld := newFieldDetector(
		validationContext{
			discoverLogLevels:       true,
			allowStructuredMetadata: true,
			logLevelFields:          []string{"level", "severity"},
		})

	for _, tc := range []struct {
		name             string
		line             string
		expectedLogLevel string
	}{
		{
			name:             "json with level field",
			line:             `{"level":"info","message":"test"}`,
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "json with severity field",
			line:             `{"severity":"warn","message":"test"}`,
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "json with level field uppercase",
			line:             `{"level":"INFO","message":"test"}`,
			expectedLogLevel: constants.LogLevelInfo,
		},
		{
			name:             "json with severity field uppercase",
			line:             `{"severity":"WARN","message":"test"}`,
			expectedLogLevel: constants.LogLevelWarn,
		},
		{
			name:             "json without level or severity",
			line:             `{"message":"test"}`,
			expectedLogLevel: constants.LogLevelUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := logproto.Entry{
				Timestamp: time.Now(),
				Line:      tc.line,
			}
			level := ld.detectLogLevelFromLogEntry(entry, labels.Labels{}, labels.Labels{})
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLevelFromLogLine(t *testing.T) {
	for _, tc := range []struct {
		line             string
		expectedLogLevel string
	}{
		{"this is a warning log", constants.LogLevelWarn},
		{"this is a debug message", constants.LogLevelDebug},
		{"this is a critical message", constants.LogLevelCritical},
		{"this is a error message", constants.LogLevelError},
		{"this is a fatal message", constants.LogLevelFatal},
		{"this is a info message", constants.LogLevelInfo},
		{"foo", constants.LogLevelUnknown},
	} {
		t.Run(tc.line, func(t *testing.T) {
			level := detectLevelFromLogLine(tc.line)
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}

func Test_detectLevelFromLogLine_earliestPosition(t *testing.T) {
	for _, tc := range []struct {
		line             string
		expectedLogLevel string
	}{
		{"debug: something happened", constants.LogLevelDebug},
		{"DEBUG: something happened", constants.LogLevelDebug},
		{"warn: something happened", constants.LogLevelWarn},
		{"WARN: something happened", constants.LogLevelWarn},
		{"error: something happened", constants.LogLevelError},
		{"ERROR: something happened", constants.LogLevelError},
		{"critical: something happened", constants.LogLevelCritical},
		{"CRITICAL: something happened", constants.LogLevelCritical},
		{"fatal: something happened", constants.LogLevelFatal},
		{"FATAL: something happened", constants.LogLevelFatal},
		{"info: something happened", constants.LogLevelInfo},
		{"INFO: something happened", constants.LogLevelInfo},
	} {
		t.Run(tc.line, func(t *testing.T) {
			level := detectLevelFromLogLine(tc.line)
			require.Equal(t, tc.expectedLogLevel, level)
		})
	}
}
