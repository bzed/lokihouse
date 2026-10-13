package multitenant_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/multitenant"
)

// fakeIndex answers per tenant from fixed data and records the tenants it saw.
type fakeIndex struct {
	seen []string
	fail string // tenant whose calls fail
}

func (f *fakeIndex) tenant(ctx context.Context) (string, error) {
	id, err := tenant.TenantID(ctx)
	if err != nil {
		return "", err
	}
	f.seen = append(f.seen, id)
	if id == f.fail {
		return "", errors.New("boom " + id)
	}
	return id, nil
}

func (f *fakeIndex) IndexStats(ctx context.Context, _ *logproto.IndexStatsRequest) (*logproto.IndexStatsResponse, error) {
	if _, err := f.tenant(ctx); err != nil {
		return nil, err
	}
	return &logproto.IndexStatsResponse{Streams: 1, Chunks: 2, Entries: 3, Bytes: 4}, nil
}

func (f *fakeIndex) Volume(ctx context.Context, _ *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	if _, err := f.tenant(ctx); err != nil {
		return nil, err
	}
	return &logproto.VolumeResponse{Volumes: []logproto.Volume{{Name: `{app="a"}`, Volume: 10}}, Limit: 100}, nil
}

func (f *fakeIndex) DetectedLabels(ctx context.Context, _ *logproto.DetectedLabelsRequest) (*logproto.DetectedLabelsResponse, error) {
	id, err := f.tenant(ctx)
	if err != nil {
		return nil, err
	}
	sk := hyperloglog.New()
	sk.Insert([]byte(id)) // a different value per tenant: merged cardinality 2
	b, err := sk.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &logproto.DetectedLabelsResponse{DetectedLabels: []*logproto.DetectedLabel{{Label: "app", Cardinality: 1, Sketch: b}}}, nil
}

func (f *fakeIndex) Patterns(ctx context.Context, _ *logproto.QueryPatternsRequest) (*logproto.QueryPatternsResponse, error) {
	id, err := f.tenant(ctx)
	if err != nil {
		return nil, err
	}
	return &logproto.QueryPatternsResponse{Series: []*logproto.PatternSeries{{Pattern: "user <_> " + id, Samples: []*logproto.PatternSample{{Timestamp: 1000, Value: 1}}}}}, nil
}

func mtCtx(ids string) context.Context { return user.InjectOrgID(context.Background(), ids) }

func TestIndex_SingleTenantPassesThrough(t *testing.T) {
	f := &fakeIndex{}
	q := &multitenant.Index{Next: f}
	res, err := q.IndexStats(mtCtx("t1"), &logproto.IndexStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, &logproto.IndexStatsResponse{Streams: 1, Chunks: 2, Entries: 3, Bytes: 4}, res)
	require.Equal(t, []string{"t1"}, f.seen)
}

func TestIndex_IndexStatsMerged(t *testing.T) {
	f := &fakeIndex{}
	res, err := (&multitenant.Index{Next: f}).IndexStats(mtCtx("t1|t2"), &logproto.IndexStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, &logproto.IndexStatsResponse{Streams: 2, Chunks: 4, Entries: 6, Bytes: 8}, res)
	require.Equal(t, []string{"t1", "t2"}, f.seen)
}

func TestIndex_VolumeMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).Volume(mtCtx("t1|t2"), &logproto.VolumeRequest{Limit: 100})
	require.NoError(t, err)
	require.Equal(t, []logproto.Volume{{Name: `{app="a"}`, Volume: 20}}, res.Volumes)
}

func TestIndex_DetectedLabelsMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).DetectedLabels(mtCtx("t1|t2"), &logproto.DetectedLabelsRequest{})
	require.NoError(t, err)
	require.Len(t, res.DetectedLabels, 1)
	require.Equal(t, "app", res.DetectedLabels[0].Label)
	require.Equal(t, uint64(2), res.DetectedLabels[0].Cardinality)
}

func TestIndex_PatternsMerged(t *testing.T) {
	res, err := (&multitenant.Index{Next: &fakeIndex{}}).Patterns(mtCtx("t1|t2"), &logproto.QueryPatternsRequest{})
	require.NoError(t, err)
	var got []string
	for _, s := range res.Series {
		got = append(got, s.Pattern)
	}
	require.ElementsMatch(t, []string{"user <_> t1", "user <_> t2"}, got)
}

func TestIndex_TenantErrorFailsRequest(t *testing.T) {
	q := &multitenant.Index{Next: &fakeIndex{fail: "t2"}}
	ctx := mtCtx("t1|t2")
	_, err := q.IndexStats(ctx, &logproto.IndexStatsRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.Volume(ctx, &logproto.VolumeRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.DetectedLabels(ctx, &logproto.DetectedLabelsRequest{})
	require.ErrorContains(t, err, "boom t2")
	_, err = q.Patterns(ctx, &logproto.QueryPatternsRequest{})
	require.ErrorContains(t, err, "boom t2")
}
