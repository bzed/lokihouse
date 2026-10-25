package deletes

import (
	"context"

	"github.com/grafana/loki/v3/pkg/compactor/deletion"
)

// NewQueryClient returns the delete-requests client queries filter with: the
// store behind Loki's per-tenant cache (refreshed every 5 minutes) and its
// deletion_mode check. The stack mirrors deleteRequestsClient in
// grafana/loki@bc1962860a16 pkg/loki/modules.go. The caller must call Stop.
func NewQueryClient(s *Store, l deletion.Limits, m *deletion.DeleteRequestClientMetrics, opts ...deletion.DeleteRequestsStoreOption) deletion.DeleteRequestsClient {
	// The constructor returns an error for interface symmetry only; it has no
	// failing path at the pinned commit.
	cached, _ := deletion.NewDeleteRequestsClient(compactorClient{s}, m, "querier", opts...)
	return deletion.NewPerTenantDeleteRequestsClient(cached, l)
}

// compactorClient adapts the store to the compactor API Loki's cache wraps.
type compactorClient struct{ *Store }

// GetCacheGenerationNumber returns no generation: lokihouse has no results
// cache to invalidate.
func (compactorClient) GetCacheGenerationNumber(context.Context, string) (string, error) {
	return "", nil
}

// Name identifies the client in Loki's logs.
func (compactorClient) Name() string { return "lokihouse" }

// Stop does nothing: the store owns no goroutine.
func (compactorClient) Stop() {}
