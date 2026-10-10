package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalise(t *testing.T) {
	q, ok := normalise(`sum(rate({foo="bar"}[5m]))`)
	require.True(t, ok)
	require.Equal(t, `range:sum(rate({app=~".+"}[5m]))`, q)

	_, ok = normalise(`{foo="bar"} |= `) // unparsable
	require.False(t, ok)

	// maxRangeFor measures the largest selector lookback (filter threshold).
	require.Equal(t, 5*time.Minute, maxRangeFor(`count_over_time({app="api"}[5m])`))
	require.Equal(t, time.Hour, maxRangeFor(`count_over_time({app="api"}[1h])`))
	require.Equal(t, 5*time.Hour, maxRangeFor(`count_over_time({app="api"}[5h])`))
}
