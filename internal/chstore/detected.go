package chstore

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// Copied from grafana/loki@fbc3f077 pkg/querier/querier.go (countLabelsAndCardinality,
// shouldRejectLabel, containsAllIDTypes, parsedFields and its methods); modifications:
// no ingester branch (lokihouse is store-only), and the sketch is marshalled via the
// same method the original uses.
type parsedFields struct {
	sketch    *hyperloglog.Sketch
	fieldType logproto.DetectedFieldType
}

func newParsedLabels() *parsedFields {
	return &parsedFields{
		sketch:    hyperloglog.New(),
		fieldType: logproto.DetectedFieldString,
	}
}

func (p *parsedFields) Insert(value string) {
	p.sketch.Insert([]byte(value))
}

func (p *parsedFields) Estimate() uint64 {
	return p.sketch.Estimate()
}

func (p *parsedFields) Marshal() ([]byte, error) {
	return p.sketch.MarshalBinary()
}

func (p *parsedFields) DetermineType(value string) {
	p.fieldType = determineType(value)
}

func determineType(value string) logproto.DetectedFieldType {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return logproto.DetectedFieldInt
	}

	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return logproto.DetectedFieldFloat
	}

	if _, err := strconv.ParseBool(value); err == nil {
		return logproto.DetectedFieldBoolean
	}

	if _, err := time.ParseDuration(value); err == nil {
		return logproto.DetectedFieldDuration
	}

	if _, err := humanize.ParseBytes(value); err == nil {
		return logproto.DetectedFieldBytes
	}

	return logproto.DetectedFieldString
}

// shouldRejectLabel checks if the label should be rejected based on the values.
func shouldRejectLabel(values []string) bool {
	if len(values) == 0 {
		return true
	}

	if len(values) > 2 {
		return containsAllIDTypes(values)
	}

	boolValues := map[string]bool{
		"0": true,
		"1": true,
	}

	for _, v := range values {
		if !boolValues[v] {
			return containsAllIDTypes(values)
		}
	}

	return false
}

// containsAllIDTypes checks if all values in the array are IDs (UUIDs or numeric values).
func containsAllIDTypes(values []string) bool {
	for _, v := range values {
		_, err := strconv.ParseFloat(v, 64)
		if err != nil {
			_, err = uuid.Parse(v)
			if err != nil {
				return false
			}
		}
	}

	return true
}

func countLabelsAndCardinality(storeLabelsMap map[string][]string, staticLabels map[string]struct{}) []*logproto.DetectedLabel {
	dlMap := make(map[string]*parsedFields)

	for label, values := range storeLabelsMap {
		if _, isStatic := staticLabels[label]; (isStatic && values != nil) || !shouldRejectLabel(values) {
			_, ok := dlMap[label]
			if !ok {
				dlMap[label] = newParsedLabels()
			}

			parsedFields := dlMap[label]
			for _, v := range values {
				parsedFields.Insert(v)
			}
		}
	}

	var detectedLabels []*logproto.DetectedLabel
	for k, v := range dlMap {
		sketch, err := v.Marshal()
		if err != nil {
			continue
		}
		detectedLabels = append(detectedLabels, &logproto.DetectedLabel{
			Label:       k,
			Cardinality: v.Estimate(),
			Sketch:      sketch,
		})
	}
	return detectedLabels
}

// DetectedLabels serves /loki/api/v1/detected_labels from the streams table:
// the store path of Loki's SingleTenantQuerier.DetectedLabels.
func (q *Querier) DetectedLabels(ctx context.Context, req *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	var ms []*labels.Matcher
	if req.Query != "" {
		if ms, err = syntax.ParseMatchers(req.Query, true); err != nil {
			return nil, err
		}
	}
	streams, err := ResolveStreams(ctx, q.Conn, tenantID, ms, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	values := map[string]map[string]struct{}{}
	for _, s := range streams {
		s.Labels.Range(func(l labels.Label) {
			if values[l.Name] == nil {
				values[l.Name] = map[string]struct{}{}
			}
			values[l.Name][l.Value] = struct{}{}
		})
	}
	if len(values) == 0 {
		return &logproto.DetectedLabelsResponse{DetectedLabels: []*logproto.DetectedLabel{}}, nil
	}
	store := make(map[string][]string, len(values))
	for name, vs := range values {
		for v := range vs {
			store[name] = append(store[name], v)
		}
		sort.Strings(store[name])
	}
	static := map[string]struct{}{"cluster": {}, "namespace": {}, "instance": {}, "pod": {}}
	return &logproto.DetectedLabelsResponse{DetectedLabels: countLabelsAndCardinality(store, static)}, nil
}
