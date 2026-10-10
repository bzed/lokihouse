package chstore

import (
	"github.com/cespare/xxhash/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
)

// sampleIter turns the ts-ordered row cursor into samples one row at a time.
// Loki's range-vector iterators only need samples in timestamp order; they group by Labels().
type sampleIter struct {
	rr        *rowReader
	byID      map[uint64]Stream
	extractor log.SampleExtractor
	ex        map[uint64]log.StreamSampleExtractor
	cur       logproto.Sample
	lbls      string
	hash      uint64
}

func (it *sampleIter) Next() bool {
	for it.rr.next() {
		r := it.rr.cur
		se, ok := it.ex[r.streamID]
		if !ok {
			se = it.extractor.ForStream(it.byID[r.streamID].Labels)
			it.ex[r.streamID] = se
		}
		s, ok := se.ProcessString(r.ts.UnixNano(), r.line, r.md)
		if !ok {
			continue
		}
		it.lbls = s.Labels.String()
		it.hash = r.streamID
		it.cur = logproto.Sample{Timestamp: r.ts.UnixNano(), Value: s.Value, Hash: xxhash.Sum64String(it.lbls + r.line)}
		return true
	}
	return false
}

func (it *sampleIter) At() logproto.Sample { return it.cur }
func (it *sampleIter) Labels() string      { return it.lbls }
func (it *sampleIter) StreamHash() uint64  { return it.hash }
func (it *sampleIter) Err() error          { return it.rr.err }
func (it *sampleIter) Close() error        { return it.rr.close() }
