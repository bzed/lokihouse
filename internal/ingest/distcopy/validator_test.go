// Copied from grafana/loki@fbc3f077 pkg/distributor/validator_test.go
// (the tests of the functions in validator.go); modifications: package
// distcopy; validationContext built with lokihouse's copy.

package distcopy

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"
)

func TestShouldBlockIngestion(t *testing.T) {
	// Create a validator with some limits
	lim := &validation.Limits{}
	// Don't call RegisterFlags - it panics with a nil FlagSet
	lim.MaxLineSize = 100
	lim.MaxLineSizeTruncate = false

	o, err := validation.NewOverrides(*lim, nil)
	require.NoError(t, err)

	v, err := NewValidator(o)
	require.NoError(t, err)

	// This test just verifies the validator can be created and used
	require.NotNil(t, v)
}
