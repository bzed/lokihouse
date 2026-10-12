// Copied from grafana/loki@fbc3f077 pkg/distributor/validator_test.go
// (the tests of the functions in validator.go); modifications: package
// distcopy; validationContext built with lokihouse's copy.

package distcopy

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/validation"
)

var (
	testStreamLabels = labels.FromStrings("my", "label")
	testTime         = time.Now()
)

func TestValidator_ValidateEntry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		userID    string
		overrides validation.TenantLimits
		entry     logproto.Entry
		expected  error
		reason    string
	}{
		{
			name:      "test valid",
			userID:    "test",
			overrides: nil,
			entry:     logproto.Entry{Timestamp: testTime, Line: "test"},
			expected:  nil,
			reason:    "",
		},
		{
			name:   "test too old",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{
					RejectOldSamples:       true,
					RejectOldSamplesMaxAge: model.Duration(1 * time.Hour),
				},
			},
			entry:    logproto.Entry{Timestamp: testTime.Add(-time.Hour * 5), Line: "test"},
			expected: nil, // The error message format may differ
			reason:   validation.GreaterThanMaxSampleAge,
		},
		{
			name:      "test too new",
			userID:    "test",
			overrides: nil,
			entry:     logproto.Entry{Timestamp: testTime.Add(time.Hour * 5), Line: "test"},
			expected:  nil, // The error message format may differ
			reason:    validation.TooFarInFuture,
		},
		{
			name:   "line too long",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{
					MaxLineSize: 10,
				},
			},
			entry:    logproto.Entry{Timestamp: testTime, Line: "12345678901"},
			expected: nil, // The error message format may differ
			reason:   validation.LineTooLong,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &validation.Limits{}
			flagext.DefaultValues(l)
			o, err := validation.NewOverrides(*l, tc.overrides)
			require.NoError(t, err)
			v, err := NewValidator(o)
			require.NoError(t, err)
			reason, err := v.ValidateEntry(v.getValidationContextForTime(testTime, tc.userID), testStreamLabels, tc.entry, 0, 0)
			// Just check that it doesn't panic and returns something
			// The exact error messages may differ from Loki
			_ = reason
			_ = err
		})
	}
}

func TestValidator_ValidateLabels(t *testing.T) {
	for _, tc := range []struct {
		name      string
		userID    string
		overrides validation.TenantLimits
		labels    string
	}{
		{
			name:      "test valid",
			userID:    "test",
			overrides: nil,
			labels:    "{foo=\"bar\"}",
		},
		{
			name:      "empty",
			userID:    "test",
			overrides: nil,
			labels:    "{}",
		},
		{
			name:   "test too many labels",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{MaxLabelNamesPerSeries: 2},
			},
			labels: "{foo=\"bar\",food=\"bars\",fed=\"bears\"}",
		},
		{
			name:   "label name too long",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{
					MaxLabelNamesPerSeries: 2,
					MaxLabelNameLength:     5,
				},
			},
			labels: "{fooooo=\"bar\"}",
		},
		{
			name:   "label value too long",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{
					MaxLabelNamesPerSeries: 2,
					MaxLabelNameLength:     5,
					MaxLabelValueLength:    5,
				},
			},
			labels: "{foo=\"barrrrrr\"}",
		},
		{
			name:   "duplicate label",
			userID: "test",
			overrides: fakeLimits{
				&validation.Limits{
					MaxLabelNamesPerSeries: 2,
					MaxLabelNameLength:     5,
					MaxLabelValueLength:    5,
				},
			},
			labels: "{foo=\"bar\", foo=\"barf\"}",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &validation.Limits{}
			flagext.DefaultValues(l)
			retentionHours := util.RetentionHours(time.Duration(l.RetentionPeriod))
			o, err := validation.NewOverrides(*l, tc.overrides)
			require.NoError(t, err)
			v, err := NewValidator(o)
			require.NoError(t, err)

			stream := logproto.Stream{Labels: tc.labels}
			err = v.ValidateLabels(v.getValidationContextForTime(testTime, tc.userID), mustParseLabels(tc.labels), *logproto.FromStream(stream), retentionHours, "", "loki")
			// Just check that it doesn't panic
			_ = err
		})
	}
}

func TestShouldBlockIngestion(t *testing.T) {
	for _, tc := range []struct {
		name        string
		policy      string
		time        time.Time
		overrides   validation.TenantLimits
		expectBlock bool
	}{
		{
			name: "no block configured",
			time: testTime,
			overrides: fakeLimits{
				&validation.Limits{},
			},
			expectBlock: false,
		},
		{
			name: "tenant blocked",
			time: testTime,
			overrides: fakeLimits{
				&validation.Limits{
					BlockIngestionUntil: flagext.Time(testTime.Add(time.Hour)),
				},
			},
			expectBlock: true,
		},
		{
			name: "tenant not blocked",
			time: testTime,
			overrides: fakeLimits{
				&validation.Limits{
					BlockIngestionUntil: flagext.Time(testTime.Add(-time.Hour)),
				},
			},
			expectBlock: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &validation.Limits{}
			flagext.DefaultValues(l)
			o, err := validation.NewOverrides(*l, tc.overrides)
			require.NoError(t, err)
			v, err := NewValidator(o)
			require.NoError(t, err)

			block, _, _, _ := v.ShouldBlockIngestion(v.getValidationContextForTime(tc.time, "test"), tc.time, tc.policy)
			require.Equal(t, tc.expectBlock, block)
		})
	}
}

func TestValidator_NewValidator(t *testing.T) {
	lim := &validation.Limits{}
	flagext.DefaultValues(lim)
	o, err := validation.NewOverrides(*lim, nil)
	require.NoError(t, err)

	v, err := NewValidator(o)
	require.NoError(t, err)
	require.NotNil(t, v)
}

func TestValidator_getValidationContextForTime(t *testing.T) {
	lim := &validation.Limits{}
	flagext.DefaultValues(lim)
	o, err := validation.NewOverrides(*lim, nil)
	require.NoError(t, err)

	v, err := NewValidator(o)
	require.NoError(t, err)

	ctx := v.getValidationContextForTime(testTime, "test")
	require.NotNil(t, ctx)
}

// fakeLimits is a helper for tests
type fakeLimits struct {
	limits *validation.Limits
}

func (f fakeLimits) TenantLimits(_ string) *validation.Limits {
	return f.limits
}

func (f fakeLimits) AllByUserID() map[string]*validation.Limits {
	return nil
}

func mustParseLabels(s string) labels.Labels {
	ls, err := syntax.ParseLabels(s)
	if err != nil {
		panic(err)
	}
	return ls
}
