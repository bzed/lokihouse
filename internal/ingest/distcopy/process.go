// Copied from grafana/loki@fbc3f077 pkg/distributor/distributor.go
// (processStreamEntries, normalizeMetadataGroup, normalizeStructuredMetadata,
// parseStreamLabels, truncateLines, missingEnforcedLabels, metadataGroup,
// newMetadataGroup, duplicateTimestampTracker and the push request loop);
// modifications: methods turned into functions on a small preparer; rate
// limiting, writeFailuresManager, metrics, usage tracker, ingester/ring and
// Kafka paths removed; labelCache inlined.
package distcopy

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/compactor/retention"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/otlptranslator"
	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/validation"
)

type preparer struct {
	validator *Validator
}

type streamStats struct {
	entriesKept int
	// Shared attributes counted once per group for ingestion rate limiting.
	unexpandedSize int
	// Shared attributes counted once per entry for rate sharding.
	expandedSize int
}

// metadataGroup keeps validation counts separate from normalized ingestion sizes.
type metadataGroup struct {
	attrs          *[]logproto.LabelAdapter
	originalSize   int
	originalCount  int
	normalizedSize int
	labels         labels.Labels
	normalized     bool
}

func newMetadataGroup(attrs *[]logproto.LabelAdapter) metadataGroup {
	size := util.StructuredMetadataSize(*attrs)
	return metadataGroup{
		attrs:          attrs,
		originalSize:   size,
		originalCount:  len(*attrs),
		normalizedSize: size,
	}
}

// duplicateTimestampTracker tracks consecutive kept entries across a stream's groups.
type duplicateTimestampTracker struct {
	previousLine      string
	previousTimestamp time.Time
	originalTimestamp time.Time
}

func (t *duplicateTimestampTracker) increment(entry *logproto.Entry) {
	// Preserve identical lines' timestamps for downstream deduplication.
	if !t.previousTimestamp.IsZero() && t.previousLine != entry.Line &&
		(entry.Timestamp.Equal(t.originalTimestamp) || entry.Timestamp.Equal(t.previousTimestamp)) {
		entry.Timestamp = t.previousTimestamp.Add(time.Nanosecond)
	} else {
		t.originalTimestamp = entry.Timestamp
	}
	t.previousLine = entry.Line
	t.previousTimestamp = entry.Timestamp
}

func nestedStreamSize(s logproto.InternalStreamAdapter) int {
	lines, structuredMetadata := calculateStreamSizes(s)
	return int(lines + structuredMetadata)
}

// calculateStreamSizes counts line and metadata bytes, with shared attributes counted once
// per nonempty resource or scope, without deduplicating names.
func calculateStreamSizes(stream logproto.InternalStreamAdapter) (uint64, uint64) {
	var entriesSize, structuredMetadataSize uint64
	for _, resource := range stream.ResourceLogs {
		hasEntries := false
		for _, scope := range resource.ScopeLogs {
			if len(scope.Entries) == 0 {
				continue
			}
			hasEntries = true
			structuredMetadataSize += uint64(util.StructuredMetadataSize(scope.Attrs))
			for _, entry := range scope.Entries {
				entriesSize += uint64(len(entry.Line))
				structuredMetadataSize += uint64(util.StructuredMetadataSize(entry.StructuredMetadata))
			}
		}
		if hasEntries {
			structuredMetadataSize += uint64(util.StructuredMetadataSize(resource.Attrs))
		}
	}
	return entriesSize, structuredMetadataSize
}

// Prepare validates and normalises the push request, the way the Loki distributor does,
// and returns the streams that survive validation with discovered fields applied.
// validationErr is non-nil when any entry or stream was rejected; its message matches
// Loki's. Kept entries of rejected streams are still returned, as in Loki.
func Prepare(ctx context.Context, limits *validation.Overrides, tenantID string, req *logproto.InternalPushRequest, now time.Time) ([]logproto.Stream, error) {
	if len(req.Streams) == 0 {
		return nil, fmt.Errorf(validation.MissingStreamsErrorMsg)
	}
	p := &preparer{validator: &Validator{Overrides: limits}}
	vctx := p.validator.getValidationContextForTime(now, tenantID)
	fieldDetector := newFieldDetector(vctx)
	tenantsRetention := retention.NewTenantsRetention(limits)

	var validationErrors util.GroupedErrors

	for i := range req.Streams {
		stream := &req.Streams[i]

		// Return early if stream does not contain any entries
		if stream.EntryCount() == 0 {
			continue
		}

		// Truncate first so subsequent steps have consistent line lengths
		p.truncateLines(vctx, stream)

		lbs, err := p.parseStreamLabels(vctx, stream.Labels, stream, tenantsRetention)
		if err != nil {
			validationErrors.Add(err)
			p.validator.reportDiscardedDataWithTracker(ctx, validation.InvalidLabels, vctx, labels.EmptyLabels(), "", "", nestedStreamSize(*stream), stream.EntryCount(), "")
			continue
		}

		if !p.validator.IsInternalStream(lbs) {
			if missing, lbsMissing := missingEnforcedLabels(lbs, vctx.enforcedLabels); missing {
				err := fmt.Errorf(validation.MissingEnforcedLabelsErrorMsg, strings.Join(lbsMissing, ","), tenantID, stream.Labels, "")
				validationErrors.Add(err)
				p.validator.reportDiscardedDataWithTracker(ctx, validation.MissingEnforcedLabels, vctx, lbs, "", "", nestedStreamSize(*stream), stream.EntryCount(), "")
				continue
			}
		}

		if block, statusCode, reason, err := p.validator.ShouldBlockIngestion(vctx, now, ""); block {
			if statusCode == 200 {
				p.validator.reportDiscardedDataWithTracker(ctx, reason, vctx, lbs, "", "", nestedStreamSize(*stream), stream.EntryCount(), "")
				continue
			}
			p.validator.reportDiscardedDataWithTracker(ctx, reason, vctx, lbs, "", "", nestedStreamSize(*stream), stream.EntryCount(), "")
			return nil, err
		}

		if _, err := p.processStreamEntries(vctx, stream, lbs, fieldDetector, &validationErrors); err != nil {
			return nil, err
		}
		if stream.EntryCount() == 0 {
			// Empty stream after validating all the entries
			req.Streams[i] = logproto.InternalStreamAdapter{}
		}
	}

	// Collect the surviving streams in Loki's flat view.
	var out []logproto.Stream
	for _, s := range req.Streams {
		if s.EntryCount() == 0 {
			continue
		}
		out = append(out, s.FlatView())
	}

	var validationErr error
	if validationErrors.Err() != nil {
		validationErr = fmt.Errorf("%s", validationErrors.Error())
	}
	return out, validationErr
}

func parseLabelsErr(key string) error {
	_, err := syntax.ParseLabels(key)
	return err
}

// parseStreamLabels parses and validates the stream label string, as the Loki distributor does.
func (p *preparer) parseStreamLabels(vContext validationContext, key string, stream *logproto.InternalStreamAdapter, tenantsRetention *retention.TenantsRetention) (labels.Labels, error) {
	ls, err := syntax.ParseLabels(key)
	if err != nil {
		return labels.EmptyLabels(), fmt.Errorf(validation.InvalidLabelsErrorMsg, key, err)
	}
	if err := p.validator.ValidateLabels(vContext, ls, *stream, tenantsRetention.RetentionHoursFor(vContext.userID, ls), "", ""); err != nil {
		return labels.EmptyLabels(), err
	}
	stream.Labels = ls.String()
	stream.Hash = labels.StableHash(ls)
	return ls, nil
}

// missingEnforcedLabels returns true if the stream is missing any of the required labels.
//
// It also returns the first label that is missing if any (for the case of multiple labels missing).
func missingEnforcedLabels(lbs labels.Labels, tenantEnforcedLabels []string) (bool, []string) {
	requiredLbs := tenantEnforcedLabels
	if len(requiredLbs) == 0 {
		// no enforced labels configured.
		return false, []string{}
	}

	// Use a map to deduplicate the required labels. Duplicates may happen if the same label is configured
	// in both the per-tenant and per-policy enforced labels.
	seen := make(map[string]struct{})
	missingLbs := []string{}

	for _, lb := range requiredLbs {
		if _, ok := seen[lb]; ok {
			continue
		}

		seen[lb] = struct{}{}
		if !lbs.Has(lb) {
			missingLbs = append(missingLbs, lb)
		}
	}

	return len(missingLbs) > 0, missingLbs
}

func (p *preparer) processStreamEntries(
	vContext validationContext,
	stream *logproto.InternalStreamAdapter,
	lbs labels.Labels,
	fieldDetector *FieldDetector,
	validationErrors *util.GroupedErrors,
) (streamStats, error) {
	var entriesKept, unexpandedSize, expandedSize int
	shouldDiscoverLevels := fieldDetector.shouldDiscoverLogLevels()
	var duplicateTimestamps duplicateTimestampTracker

	resourcesKept := 0
	for resourceIdx := range stream.ResourceLogs {
		resource := &stream.ResourceLogs[resourceIdx]
		resourceMetadata := newMetadataGroup(&resource.Attrs)
		scopesKept := 0
		var resourceDiscardReason string
		for scopeIdx := range resource.ScopeLogs {
			scope := &resource.ScopeLogs[scopeIdx]
			scopeMetadata := newMetadataGroup(&scope.Attrs)
			sharedMetadataSize := resourceMetadata.originalSize + scopeMetadata.originalSize
			sharedMetadataCount := resourceMetadata.originalCount + scopeMetadata.originalCount
			var normalizedSharedMetadataSize int
			sharedMetadata := make([]labels.Labels, 0, 2)
			scopeEntriesKept := 0
			var scopeDiscardReason string
			for _, entry := range scope.Entries {
				if reason, err := p.validator.ValidateEntry(vContext, lbs, entry, sharedMetadataSize, sharedMetadataCount); err != nil {
					p.validator.reportDiscardedDataWithTracker(context.Background(), reason, vContext, lbs, "", "", util.EntryTotalSize(&entry), 1, "")
					scopeDiscardReason = reason
					resourceDiscardReason = reason
					validationErrors.Add(err)
					continue
				}

				// Normalize shared attributes once, after an entry passes validation.
				if scopeEntriesKept == 0 {
					if err := p.normalizeMetadataGroup(&resourceMetadata, "", shouldDiscoverLevels); err != nil {
						return streamStats{}, err
					}
					if err := p.normalizeMetadataGroup(&scopeMetadata, "", shouldDiscoverLevels); err != nil {
						return streamStats{}, err
					}
					// Scope attributes take precedence over resource attributes.
					if !scopeMetadata.labels.IsEmpty() {
						sharedMetadata = append(sharedMetadata, scopeMetadata.labels)
					}
					if !resourceMetadata.labels.IsEmpty() {
						sharedMetadata = append(sharedMetadata, resourceMetadata.labels)
					}
					normalizedSharedMetadataSize = resourceMetadata.normalizedSize + scopeMetadata.normalizedSize
				}

				structuredMetadata, _, err := p.normalizeStructuredMetadata(entry.StructuredMetadata, false)
				if err != nil {
					return streamStats{}, err
				}
				entry.StructuredMetadata = logproto.CopyToLabelAdapters(entry.StructuredMetadata, structuredMetadata)

				fieldDetector.discoverFields(context.Background(), lbs, &entry, structuredMetadata, sharedMetadata...)
				if vContext.incrementDuplicateTimestamps {
					duplicateTimestamps.increment(&entry)
				}
				scope.Entries[scopeEntriesKept] = entry
				scopeEntriesKept++
				entrySize := util.EntryTotalSize(&entry)
				unexpandedSize += entrySize
				expandedSize += entrySize + normalizedSharedMetadataSize
			}
			scope.Entries = scope.Entries[:scopeEntriesKept]
			if scopeEntriesKept == 0 {
				// Charge shared bytes only when the group is emptied, using its last rejection reason.
				if scopeDiscardReason != "" && scopeMetadata.originalSize > 0 {
					p.validator.reportDiscardedDataWithTracker(context.Background(), scopeDiscardReason, vContext, lbs, "", "", scopeMetadata.originalSize, 0, "")
				}
				continue
			}
			unexpandedSize += scopeMetadata.normalizedSize
			entriesKept += scopeEntriesKept
			resource.ScopeLogs[scopesKept] = *scope
			scopesKept++
		}
		clear(resource.ScopeLogs[scopesKept:])
		resource.ScopeLogs = resource.ScopeLogs[:scopesKept]
		if scopesKept == 0 {
			if resourceDiscardReason != "" && resourceMetadata.originalSize > 0 {
				p.validator.reportDiscardedDataWithTracker(context.Background(), resourceDiscardReason, vContext, lbs, "", "", resourceMetadata.originalSize, 0, "")
			}
			continue
		}
		unexpandedSize += resourceMetadata.normalizedSize
		stream.ResourceLogs[resourcesKept] = *resource
		resourcesKept++
	}
	clear(stream.ResourceLogs[resourcesKept:])
	stream.ResourceLogs = stream.ResourceLogs[:resourcesKept]
	return streamStats{
		entriesKept:    entriesKept,
		unexpandedSize: unexpandedSize,
		expandedSize:   expandedSize,
	}, nil
}

func (p *preparer) normalizeMetadataGroup(group *metadataGroup, _ string, normalizeLevel bool) error {
	if group.normalized {
		return nil
	}

	metadata, changed, err := p.normalizeStructuredMetadata(*group.attrs, normalizeLevel)
	if err != nil {
		return err
	}
	if changed {
		*group.attrs = logproto.CopyToLabelAdapters(nil, metadata)
		group.normalizedSize = util.StructuredMetadataSize(*group.attrs)
	}
	group.labels = metadata
	group.normalized = true
	return nil
}

// normalizeStructuredMetadata normalizes names and values without changing the input.
func (p *preparer) normalizeStructuredMetadata(metadata []logproto.LabelAdapter, normalizeLevel bool) (labels.Labels, bool, error) {
	if len(metadata) == 0 {
		return labels.EmptyLabels(), false, nil
	}
	labelNamer := otlptranslator.LabelNamer{}
	builder := labels.NewBuilder(logproto.FromLabelAdaptersToLabels(metadata))
	changed := false
	for _, lbl := range metadata {
		name, err := labelNamer.Build(lbl.Name)
		if err != nil {
			return labels.EmptyLabels(), false, err
		}
		if name != lbl.Name {
			builder.Del(lbl.Name)
			builder.Set(name, lbl.Value)
			changed = true
		}
		value := lbl.Value
		if value == "" {
			// The labels builder removes empty values.
			changed = true
		}
		if strings.ContainsRune(value, utf8.RuneError) {
			value = strings.Map(removeInvalidUtf, value)
			builder.Set(name, value)
			changed = true
		}
	}
	if normalizeLevel {
		value := builder.Get(constants.LevelLabel)
		if normalized := normalizeLogLevel(value); normalized != value {
			builder.Set(constants.LevelLabel, normalized)
			changed = true
		}
	}
	return builder.Labels(), changed, nil
}

// removeInvalidUtf maps the Unicode replacement rune to a space, as in Loki.
var removeInvalidUtf = func(r rune) rune {
	if r == utf8.RuneError {
		return 32 // rune value for space
	}
	return r
}

// truncateLines truncates lines over maxLineSize when maxLineSizeTruncate is enabled.
func (p *preparer) truncateLines(vContext validationContext, stream *logproto.InternalStreamAdapter) {
	if !vContext.maxLineSizeTruncate {
		return
	}

	suffix := vContext.maxLineSizeTruncateIdentifier

	stream.EachGroup(func(_, _ []logproto.LabelAdapter, entries []logproto.Entry) {
		for i, e := range entries {
			if maxSize := vContext.maxLineSize; maxSize != 0 && len(e.Line) > maxSize {
				truncateTo := maxSize - len(suffix)
				if truncateTo <= 0 {
					continue
				}

				entries[i].Line = e.Line[:truncateTo] + suffix
			}
		}
	})
}
