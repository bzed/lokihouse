package chstore

import (
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// StreamIdentity canonicalises a stream label string and returns its Loki-compatible hash.
func StreamIdentity(lbls string) (labels.Labels, uint64, error) {
	ls, err := syntax.ParseLabels(lbls)
	if err != nil {
		return labels.EmptyLabels(), 0, err
	}
	return ls, labels.StableHash(ls), nil
}
