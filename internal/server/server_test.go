package server_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/config"
	"github.com/bzed/lokihouse/internal/server"
)

const defaultServer = "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, graceful_shutdown_timeout: 10s}\n"

// testConfig writes a config for a fresh ClickHouse database (dropped when
// the test ends) with the given server block and extra top-level YAML.
func testConfig(t *testing.T, serverYAML, extra string) *config.Config {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_srv_" + hex.EncodeToString(b)
	topo := chtest.Topology()
	o := chtest.Options("default")
	y := "auth_enabled: true\n" + serverYAML + fmt.Sprintf(`clickhouse:
  addr: [%q]
  database: %s
  username: %q
  password: %q
  cluster: %q
  replicated: %t
`, o.Addr[0], db, o.Auth.Username, o.Auth.Password, topo.Cluster, topo.Replicated) + extra
	p := filepath.Join(t.TempDir(), "lokihouse.yaml")
	require.NoError(t, os.WriteFile(p, []byte(y), 0o600))
	cfg, err := config.Load(p, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		admin, err := clickhouse.Open(chtest.Options("default"))
		if err == nil {
			_ = chstore.DropDatabase(context.Background(), admin, db, topo)
			_ = admin.Close()
		}
	})
	return cfg
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type running struct {
	addr   string // host:port
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// start runs server.Run in the background and waits until it listens.
func start(t *testing.T, cfg *config.Config, logger log.Logger) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrc := make(chan net.Addr, 1)
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() {
		r.done <- server.Run(ctx, cfg, server.Options{Logger: logger, Listening: func(a net.Addr) { addrc <- a }})
	}()
	select {
	case a := <-addrc:
		r.addr = a.String()
	case err := <-r.done:
		t.Fatalf("Run returned before listening: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("server did not start")
	}
	t.Cleanup(func() { _ = r.stop() })
	return r
}

// stop cancels Run's context and returns Run's result (once).
func (r *running) stop() error {
	r.once.Do(func() {
		r.cancel()
		select {
		case r.err = <-r.done:
		case <-time.After(60 * time.Second):
			r.err = fmt.Errorf("Run did not return within 60s")
		}
	})
	return r.err
}

func pushBody(ts time.Time, line string) string {
	return fmt.Sprintf(`{"streams":[{"stream":{"app":"a"},"values":[[%q,%q]]}]}`, fmt.Sprint(ts.UnixNano()), line)
}

func push(t *testing.T, addr, tenant, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/loki/api/v1/push", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenant)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func countRows(t *testing.T, cfg *config.Config, tenant string) uint64 {
	t.Helper()
	conn, err := clickhouse.Open(chtest.Options(cfg.ClickHouse.Database))
	require.NoError(t, err)
	defer conn.Close()
	var n uint64
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count() FROM logs WHERE tenant = ?", tenant).Scan(&n))
	return n
}

func TestRun_PushQueryShutdown(t *testing.T) {
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(time.Now(), "hello")))
	require.Equal(t, uint64(1), countRows(t, cfg, "t1"))

	require.NoError(t, r.stop())
	_, err := net.DialTimeout("tcp", r.addr, time.Second)
	require.Error(t, err, "listener must be closed after Run returns")
}

func TestRun_InFlightPushCompletes(t *testing.T) {
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())

	body := pushBody(time.Now(), "in flight")
	pr, pw := io.Pipe()
	// context.Background(): the request must stay in flight across r.stop(),
	// which cancels the test's context (and would otherwise tear down the
	// client connection before the server drains it).
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+r.addr+"/loki/api/v1/push", pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", "t1")
	req.ContentLength = int64(len(body))
	type result struct {
		code int
		err  error
	}
	resc := make(chan result, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			resc <- result{err: err}
			return
		}
		defer resp.Body.Close()
		resc <- result{code: resp.StatusCode}
	}()
	_, err = pw.Write([]byte(body[:10])) // the request is now in flight
	require.NoError(t, err)

	stopped := make(chan error, 1)
	go func() { stopped <- r.stop() }()
	// Shutdown has begun once new connections are refused.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", r.addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
		}
		return err != nil
	}, 10*time.Second, 20*time.Millisecond)

	_, err = pw.Write([]byte(body[10:]))
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	res := <-resc
	require.NoError(t, res.err)
	require.Equal(t, http.StatusNoContent, res.code)
	require.NoError(t, <-stopped)
	require.Equal(t, uint64(1), countRows(t, cfg, "t1"))
}

func TestRun_StopsRulerAndLoops(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, defaultServer, fmt.Sprintf(`ruler:
  rule_path: %s
ruler_storage:
  backend: filesystem
  filesystem: {dir: %s}
`, filepath.Join(dir, "rules-temp"), filepath.Join(dir, "rules")))
	var logs syncBuffer
	r := start(t, cfg, log.NewLogfmtLogger(&logs))
	require.NoError(t, r.stop())
	out := logs.String()
	for _, c := range []string{"http", "ruler", "deletes", "clickhouse"} {
		require.Contains(t, out, `msg=stopped component=`+c)
	}
	// Order: HTTP before ruler before background loops before ClickHouse.
	require.Less(t, strings.Index(out, "msg=stopped component=http"), strings.Index(out, "msg=stopped component=ruler"))
	require.Less(t, strings.Index(out, "msg=stopped component=ruler"), strings.Index(out, "msg=stopped component=deletes"))
	require.Less(t, strings.Index(out, "msg=stopped component=deletes"), strings.Index(out, "msg=stopped component=clickhouse"))
}

func TestRun_Metrics(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, defaultServer, fmt.Sprintf("ruler_storage: {backend: filesystem, filesystem: {dir: %s}}\nruler: {rule_path: %s}\n",
		filepath.Join(dir, "rules"), filepath.Join(dir, "rules-temp")))
	r := start(t, cfg, log.NewNopLogger())
	// metrics-t1: distributor counters are process-global, so a unique tenant
	// keeps this test's exact values.
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "metrics-t1", pushBody(time.Now(), "hello")))
	require.Equal(t, http.StatusBadRequest, push(t, r.addr, "metrics-t1", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`))

	resp, err := http.Get("http://" + r.addr + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, _ := io.ReadAll(resp.Body)
	m := string(b)
	for _, want := range []string{
		`loki_request_duration_seconds_count{method="POST",route="loki_api_v1_push",status_code="204",ws="false"}`,
		`loki_distributor_lines_received_total{format="loki",is_internal_stream="false",policy="",tenant="metrics-t1"} 2`,
		`loki_discarded_samples_total{`,
		`loki_ruler_sync_rules_total`,
		`lokihouse_build_info{`,
	} {
		require.Contains(t, m, want)
	}
}

func TestRun_ListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	cfg := testConfig(t, fmt.Sprintf("server: {http_listen_address: 127.0.0.1, http_listen_port: %d}\n", port), "")
	err = server.Run(t.Context(), cfg, server.Options{Logger: log.NewNopLogger()})
	require.ErrorContains(t, err, "address already in use")
}

func TestRun_ReadTimeoutClosesSlowClients(t *testing.T) {
	cfg := testConfig(t, "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, http_server_read_timeout: 300ms}\n", "")
	r := start(t, cfg, log.NewNopLogger())
	c, err := net.Dial("tcp", r.addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Write([]byte("GET /ready HTTP/1.1\r\nHost: x\r\n")) // headers never finish
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadAll(c) // the server closes the connection after the read timeout
	require.NoError(t, err, "connection must be closed by the server, not by our 5s deadline")
}

func TestRun_TailOutlivesWriteTimeout(t *testing.T) {
	cfg := testConfig(t, "server: {http_listen_address: 127.0.0.1, http_listen_port: 0, http_server_write_timeout: 500ms}\n", "")
	r := start(t, cfg, log.NewNopLogger())
	u := url.URL{Scheme: "ws", Host: r.addr, Path: "/loki/api/v1/tail", RawQuery: url.Values{"query": {`{app="a"}`}}.Encode()}
	ws, resp, err := websocket.DefaultDialer.DialContext(t.Context(), u.String(), http.Header{"X-Scope-OrgID": {"t1"}})
	require.NoError(t, err)
	defer resp.Body.Close()
	defer ws.Close()

	time.Sleep(1500 * time.Millisecond) // 3× the write timeout
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(time.Now(), "late line")))
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(msg), "late line")
}

func TestRun_NotifiesSystemd(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "notify.sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sock, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()
	t.Setenv("NOTIFY_SOCKET", sock)

	read := func() string {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(30*time.Second)))
		b := make([]byte, 256)
		n, err := conn.Read(b)
		require.NoError(t, err)
		return string(b[:n])
	}
	cfg := testConfig(t, defaultServer, "")
	r := start(t, cfg, log.NewNopLogger())
	require.Equal(t, "READY=1", read())
	require.NoError(t, r.stop())
	require.Equal(t, "STOPPING=1", read())
}
