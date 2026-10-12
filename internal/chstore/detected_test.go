package chstore

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func TestDetermineType(t *testing.T) {
	// Test integer detection
	pf := newParsedLabels()
	pf.DetermineType("42")
	require.Equal(t, logproto.DetectedFieldInt, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("-123")
	require.Equal(t, logproto.DetectedFieldInt, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("0")
	require.Equal(t, logproto.DetectedFieldInt, pf.fieldType)

	// Test float detection
	pf = newParsedLabels()
	pf.DetermineType("3.14")
	require.Equal(t, logproto.DetectedFieldFloat, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("-0.5")
	require.Equal(t, logproto.DetectedFieldFloat, pf.fieldType)

	// Test boolean detection
	pf = newParsedLabels()
	pf.DetermineType("true")
	require.Equal(t, logproto.DetectedFieldBoolean, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("false")
	require.Equal(t, logproto.DetectedFieldBoolean, pf.fieldType)

	// Test duration detection
	pf = newParsedLabels()
	pf.DetermineType("1h")
	require.Equal(t, logproto.DetectedFieldDuration, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("30m")
	require.Equal(t, logproto.DetectedFieldDuration, pf.fieldType)

	// Test bytes detection
	pf = newParsedLabels()
	pf.DetermineType("1KB")
	require.Equal(t, logproto.DetectedFieldBytes, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("1MB")
	require.Equal(t, logproto.DetectedFieldBytes, pf.fieldType)

	// Test string detection (fallback)
	pf = newParsedLabels()
	pf.DetermineType("hello")
	require.Equal(t, logproto.DetectedFieldString, pf.fieldType)

	pf = newParsedLabels()
	pf.DetermineType("hello world")
	require.Equal(t, logproto.DetectedFieldString, pf.fieldType)
}

func TestDetermineTypeFunction(t *testing.T) {
	// Test all type detections via the standalone function
	tests := []struct {
		input    string
		expected logproto.DetectedFieldType
	}{
		{"42", logproto.DetectedFieldInt},
		{"-123", logproto.DetectedFieldInt},
		{"3.14", logproto.DetectedFieldFloat},
		{"-0.5", logproto.DetectedFieldFloat},
		{"true", logproto.DetectedFieldBoolean},
		{"false", logproto.DetectedFieldBoolean},
		{"1h", logproto.DetectedFieldDuration},
		{"30m", logproto.DetectedFieldDuration},
		{"1KB", logproto.DetectedFieldBytes},
		{"1MB", logproto.DetectedFieldBytes},
		{"hello", logproto.DetectedFieldString},
		{"", logproto.DetectedFieldString},
		{"not a number", logproto.DetectedFieldString},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := determineType(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldRejectLabel(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected bool
	}{
		{
			name:     "empty values",
			values:   []string{},
			expected: true,
		},
		{
			name:     "single value",
			values:   []string{"value"},
			expected: false,
		},
		{
			name:     "two values",
			values:   []string{"a", "b"},
			expected: false,
		},
		{
			name:     "three numeric values",
			values:   []string{"1", "2", "3"},
			expected: true,
		},
		{
			name:     "bool values 0 and 1",
			values:   []string{"0", "1"},
			expected: false,
		},
		{
			name:     "bool and non-bool",
			values:   []string{"0", "x"},
			expected: false,
		},
		{
			name:     "UUID values",
			values:   []string{"550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001"},
			expected: true,
		},
		{
			name:     "mixed UUID and numeric",
			values:   []string{"1", "550e8400-e29b-41d4-a716-446655440000"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldRejectLabel(tt.values)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsAllIDTypes(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected bool
	}{
		{
			name:     "empty",
			values:   []string{},
			expected: true,
		},
		{
			name:     "single numeric",
			values:   []string{"123"},
			expected: true,
		},
		{
			name:     "single UUID",
			values:   []string{"550e8400-e29b-41d4-a716-446655440000"},
			expected: true,
		},
		{
			name:     "mixed numeric and UUID",
			values:   []string{"123", "550e8400-e29b-41d4-a716-446655440000"},
			expected: true,
		},
		{
			name:     "numeric and non-numeric",
			values:   []string{"123", "abc"},
			expected: false,
		},
		{
			name:     "UUID and non-UUID",
			values:   []string{"550e8400-e29b-41d4-a716-446655440000", "abc"},
			expected: false,
		},
		{
			name:     "all numeric",
			values:   []string{"1", "2", "3"},
			expected: true,
		},
		{
			name:     "all UUID",
			values:   []string{"550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsAllIDTypes(tt.values)
			require.Equal(t, tt.expected, result)
		})
	}
}
