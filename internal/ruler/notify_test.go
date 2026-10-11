package ruler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	lokiruler "github.com/grafana/loki/v3/pkg/ruler"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v4"

	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

// rwSink is a Prometheus remote-write receiver. The first failFirst requests
// get 503 (Prometheus retries 5xx).
type rwSink struct {
	*httptest.Server
	mu        sync.Mutex
	failFirst int
	requests  int
	samples   map[string]float64 // series labels -> latest value
	tenants   map[string]bool    // X-Scope-OrgID headers seen
}

func newRWSink(t *testing.T, failFirst int) *rwSink {
	s := &rwSink{failFirst: failFirst, samples: map[string]float64{}, tenants: map[string]bool{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.requests++
		if s.requests <= s.failFirst {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.tenants[r.Header.Get("X-Scope-OrgID")] = true
		for _, ts := range req.Timeseries {
			if n := len(ts.Samples); n > 0 {
				s.samples[seriesKey(ts.Labels)] = ts.Samples[n-1].Value
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(s.Close)
	return s
}

func seriesKey(ls []prompb.Label) string {
	m := map[string]string{}
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return fmt.Sprint(m)
}

func (s *rwSink) sample(key string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.samples[key]
	return v, ok
}

// amGroup is a group whose alert keeps firing for the whole test: the 2m
// evaluation window covers every line written by writeLines (1m + n seconds
// old), so the alert never resolves while the test runs.
const amGroup = `name: g1
interval: 1s
rules:
  - alert: Lines
    expr: sum by (app) (count_over_time({app="a"}[2m])) > 0
    labels: {severity: page}
    annotations: {summary: '{{ $labels.app }} has {{ $value }} lines'}
`

// withRemoteWrite enables Loki's ruler remote write to url, decoding the
// client from YAML exactly as config.Load does (so Prometheus defaults apply).
func withRemoteWrite(t *testing.T, url string) func(*lokiruler.Config) {
	return func(rc *lokiruler.Config) {
		src := fmt.Sprintf("enabled: true\nclients:\n  sink: {url: %q, queue_config: {batch_send_deadline: 100ms, min_backoff: 10ms, max_backoff: 100ms}}\n", url+"/api/v1/write")
		require.NoError(t, yaml.Unmarshal([]byte(src), &rc.RemoteWrite))
	}
}

func TestRemoteWrite_RecordingRuleSamples(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	sink := newRWSink(t, 0)
	h := startRuler(t, t.TempDir(), engine(t, conn), withRemoteWrite(t, sink.URL))
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	key := fmt.Sprint(map[string]string{"__name__": "app:lines:count", "app": "a"})
	require.Eventually(t, func() bool { _, ok := sink.sample(key); return ok }, 60*time.Second, 100*time.Millisecond)
	v, _ := sink.sample(key)
	require.Equal(t, 3.0, v)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Equal(t, map[string]bool{"t1": true}, sink.tenants, "Loki's add_org_id_header default sends the rule's tenant")
}

func TestRemoteWrite_RetriesAfterReceiverError(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	sink := newRWSink(t, 3)
	h := startRuler(t, t.TempDir(), engine(t, conn), withRemoteWrite(t, sink.URL))
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", group)
	require.Equal(t, http.StatusAccepted, code)

	key := fmt.Sprint(map[string]string{"__name__": "app:lines:count", "app": "a"})
	require.Eventually(t, func() bool { _, ok := sink.sample(key); return ok }, 60*time.Second, 100*time.Millisecond)
}

type amAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	GeneratorURL string           `json:"generatorURL"`
}

func TestAlertmanager_ReceivesFiringAlert(t *testing.T) {
	conn, _ := chtest.New(t)
	writeLines(t, conn, "t1", 3)
	var mu sync.Mutex
	var got []amAlert
	am := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var as []amAlert
		if err := json.NewDecoder(r.Body).Decode(&as); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		got = append(got, as...)
		mu.Unlock()
	}))
	t.Cleanup(am.Close)

	h := startRuler(t, t.TempDir(), engine(t, conn), func(rc *lokiruler.Config) { rc.AlertmanagerURL = am.URL })
	code, _ := h.do(t, http.MethodPost, "/loki/api/v1/rules/ns", "t1", amGroup)
	require.Equal(t, http.StatusAccepted, code)

	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) > 0 }, 60*time.Second, 100*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]string{"alertname": "Lines", "app": "a", "severity": "page"}, got[0].Labels)
	require.Equal(t, "a has 3 lines", got[0].Annotations["summary"])
}
