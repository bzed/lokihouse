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
