package patterns

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestQuerier_Logger(t *testing.T) {
	// Test that logger() returns a nop logger when Logger is nil
	q := &Querier{Q: nil, Limits: nil, MaxLines: 0, Logger: nil}
	logger := q.logger()
	require.NotNil(t, logger)
	// A nop logger should not be nil

	// Test that logger() returns the configured logger when set
	customLogger := log.NewNopLogger()
	q.Logger = customLogger
	logger = q.logger()
	require.NotNil(t, logger)
	require.Equal(t, customLogger, logger)
}
