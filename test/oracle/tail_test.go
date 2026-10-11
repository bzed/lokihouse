//go:build oracle

package oracle

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/grafana/loki/v3/pkg/loghttp"
)

// tailEntry is one (labels, ts, line) triple decoded from a tail response.
type tailEntry struct {
	labels string
	ts     int64
	line   string
}

// tailOracle exercises /loki/api/v1/tail over WebSocket end-to-end. It pushes
// 20 lines to a fresh tenant, opens a tail on both stores, reads each socket
// until 20 entries arrive (or 20 s pass), and compares the sorted
// (labels, ts, line) sets. It runs last because it pushes new data after the
// fixture.
func tailOracle(t *testing.T, loki, house string) {
	t.Helper()
	const tenant = "tenant-tail"
	const n = 20

	// 1. Push 20 lines (ts = now + i ms) to both stores *before* opening the
	// tails. Both stores serve a tail as history (a backward select over
	// [start, now] at connect time) merged with the live stream, so pushing
	// first puts the entries in the history path on both sides.
	now := time.Now().UTC()
	var vals []string
	for i := 0; i < n; i++ {
		ts := now.Add(time.Duration(i) * time.Millisecond).UnixNano()
		vals = append(vals, `["`+strconv.FormatInt(ts, 10)+`","tail line `+strconv.Itoa(i)+`"]`)
	}
	body := `{"streams":[{"stream":{"app":"tail"},"values":[` + strings.Join(vals, ",") + `]}]}`
	PushBoth(t, []string{loki, house}, PushBatch{Tenant: tenant, Body: []byte(body)})

	// Wait until both stores can count all n lines (the tail reads via the
	// store, which must see them).
	for _, u := range []string{loki, house} {
		waitTailCount(t, u, tenant, n, now)
	}

	// 2. Open both WebSocket tails. start is well before the entries so the
	// history select [start, dial-time] covers all of them (the entries sit at
	// now..now+19ms and the dial happens after the count wait).
	startS := strconv.FormatInt(now.Add(-30 * time.Second).UnixNano(), 10)
	clients := map[string]*websocket.Conn{}
	for name, u := range map[string]string{"loki": loki, "lokihouse": house} {
		base := strings.TrimSuffix(u, "/")
		q := url.Values{"query": {`{app="tail"}`}, "start": {startS}, "limit": {"100"}}
		wsURL := "ws" + strings.TrimPrefix(base, "http") + "/loki/api/v1/tail?" + q.Encode()
		hdr := http.Header{}
		hdr.Set("X-Scope-OrgID", tenant)
		conn, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
		if err != nil {
			bres := ""
			if resp != nil {
				b, _ := io.ReadAll(resp.Body)
				bres = strings.TrimSpace(string(b))
			}
			t.Fatalf("%s tail dial: %v (status %d) body=%q", u, err, resp.StatusCode, bres)
		}
		clients[name] = conn
		t.Cleanup(func() { _ = conn.Close() })
	}

	// 3. Read from each socket until n entries arrive or 20 s pass.
	got := map[string][]tailEntry{}
	for name, conn := range clients {
		got[name] = readTail(t, conn, n, 20*time.Second)
	}

	// 4. Compare the sorted (labels, ts, line) lists; require n from each.
	lk := normTail(got["loki"])
	lh := normTail(got["lokihouse"])
	requireEq := func(a, b []string, what string) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("%s: loki %d entries vs lokihouse %d", what, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("%s: entry %d differs:\nloki=%q\nhouse=%q", what, i, a[i], b[i])
			}
		}
	}
	if len(lk) != n || len(lh) != n {
		t.Fatalf("tail: loki returned %d entries, lokihouse returned %d, want %d each", len(lk), len(lh), n)
	}
	requireEq(lk, lh, "tail")
	t.Logf("tailOracle: %d entries matched between loki and lokihouse", n)
}

// waitTailCount polls the instant count_over_time until the store reports want
// tail lines (or times out), so the tail's history select can see them.
func waitTailCount(t *testing.T, u, tenant string, want int, base time.Time) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		v := url.Values{
			"query": {`sum(count_over_time({app="tail"}[2h]))`},
			"time":  {strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10)},
		}
		b := Get(t, u+"/loki/api/v1/query?"+v.Encode(), tenant)
		var r struct {
			Data struct {
				Result []struct{ Value []any } `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(b, &r) == nil && len(r.Data.Result) == 1 {
			if s, ok := r.Data.Result[0].Value[1].(string); ok && s == strconv.Itoa(want) {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("tail count for %s never reached %d", u, want)
}

// readTail reads WebSocket tail responses from conn until want entries or timeout.
func readTail(t *testing.T, conn *websocket.Conn, want int, timeout time.Duration) []tailEntry {
	t.Helper()
	var out []tailEntry
	deadline := time.Now().Add(timeout)
	for len(out) < want && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(time.Until(deadline)))
		var tr loghttp.TailResponse
		msg, merr := func() ([]byte, error) {
			mt, m, e := conn.ReadMessage()
			_ = mt
			return m, e
		}()
		if merr != nil {
			t.Logf("readTail: ReadMessage err: %v (after %d entries)", merr, len(out))
			break
		} else if err := json.Unmarshal(msg, &tr); err != nil {
			t.Logf("readTail: unmarshal err: %v (raw %q)", err, msg)
			break
		}
		for _, s := range tr.Streams {
			for _, e := range s.Entries {
				out = append(out, tailEntry{
					labels: mapKey(s.Labels),
					ts:     e.Timestamp.UnixNano(),
					line:   e.Line,
				})
			}
		}
	}
	return out
}

// normTail returns a sorted list of "labels|ts|line" strings.
func normTail(in []tailEntry) []string {
	out := make([]string, len(in))
	for i, e := range in {
		out[i] = e.labels + "|" + strconv.FormatInt(e.ts, 10) + "|" + e.line
	}
	sort.Strings(out)
	return out
}

// mapKey renders a loghttp.LabelSet as a stable canonical string.
func mapKey(m loghttp.LabelSet) string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
