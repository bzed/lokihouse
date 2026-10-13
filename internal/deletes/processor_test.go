package deletes_test

import (
	"bytes"
	"context"
	"flag"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
	"github.com/bzed/lokihouse/internal/deletes"
)

// newProcessor builds a Processor over a fresh database with a fixed clock.
// now controls the "current time" for the cancel-period check; created is the
// time requests are created at (so ProcessDue only acts on requests old enough).
func newProcessor(t *testing.T, cancelPeriod, interval time.Duration, now, created time.Time) (*deletes.Processor, *deletes.Store, driver.Conn, string) {
	conn, db := chtest.New(t)
	p, store, _ := buildProcessor(conn, db, cancelPeriod, interval, now, created)
	return p, store, conn, db
}

// buildProcessor wires a Processor over an existing conn+db (so a test can
// swap conn for a fault-injecting wrapper).
func buildProcessor(conn driver.Conn, db string, cancelPeriod, interval time.Duration, now, created time.Time) (*deletes.Processor, *deletes.Store, driver.Conn) {
	store := &deletes.Store{Conn: conn, Now: func() time.Time { return created }}
	p := &deletes.Processor{
		Store: store, Conn: conn, DB: db, Topo: chtest.Topology(),
		CancelPeriod: cancelPeriod, Interval: interval, Logger: log.NewNopLogger(),
		Now: func() time.Time { return now },
	}
	return p, store, conn
}

// failConn wraps a driver.Conn and makes Query fail exactly once, to simulate
// a transient ClickHouse failure mid-scan.
type failConn struct {
	driver.Conn
	once bool
}

func (f *failConn) Query(ctx context.Context, q string, args ...any) (driver.Rows, error) {
	if !f.once {
		f.once = true
		return nil, context.DeadlineExceeded
	}
	return f.Conn.Query(ctx, q, args...)
}

// contains reports whether v is in s.
func contains(s []string, v string) (int, bool) {
	for i, x := range s {
		if x == v {
			return i, true
		}
	}
	return -1, false
}

// lines returns the sorted distinct lines of a tenant's logs table.
func lines(t *testing.T, conn driver.Conn, tenant string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT DISTINCT line FROM logs WHERE tenant = ? ORDER BY line", tenant)
	require.NoError(t, err)
	var out []string
	for rows.Next() {
		var l string
		_ = rows.Scan(&l)
		out = append(out, l)
	}
	require.NoError(t, rows.Err())
	return out
}

// queryTimeLines is what a querier with Deletes: store sees (the authoritative
// "surviving" set that physical deletion must match).
func queryTimeLines(t *testing.T, conn driver.Conn, store *deletes.Store, q string, start, end time.Time) []string {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	ng := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn, Deletes: store}, o, log.NewNopLogger())
	p, err := logql.NewLiteralParams(q, start, end, 0, 0, logproto.FORWARD, 1000, nil, nil)
	require.NoError(t, err)
	res, err := ng.Query(p).Exec(user.InjectOrgID(context.Background(), "t1"))
	require.NoError(t, err)
	streams := res.Data.(logqlmodel.Streams)
	var out []string
	for _, s := range streams {
		for _, e := range s.Entries {
			out = append(out, e.Line)
		}
	}
	sort.Strings(out)
	return out
}

func TestProcessor_MatchesQueryTimeFilter(t *testing.T) {
	p, store, conn, _ := newProcessor(t, 0, time.Hour, t0, t0)

	w := &chstore.Writer{Conn: conn}
	base := t0
	// {app="a"}: keep, secret a, secret b, secret c(at end), secret d(just past end)
	aEntries := []logproto.Entry{
		{Timestamp: base, Line: "keep"},
		{Timestamp: base.Add(time.Millisecond), Line: "secret a"},
		{Timestamp: base.Add(2 * time.Millisecond), Line: "secret b"},
		{Timestamp: base.Add(3 * time.Millisecond), Line: "secret c"},
		{Timestamp: base.Add(3*time.Millisecond + time.Millisecond/2), Line: "secret d"},
	}
	// {app="b"}: other stream, must survive
	bEntries := []logproto.Entry{{Timestamp: base.Add(2 * time.Millisecond), Line: "secret x"}}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: aEntries},
		{Labels: `{app="b"}`, Entries: bEntries},
	}, nil))

	// Delete request {app="a"} |~ "secret [a-c]" over [base, base+3ms].
	end := base.Add(3 * time.Millisecond)
	_, err := store.Add(context.Background(), "t1", `{app="a"} |~ "secret [a-c]"`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)

	// The query-time view (what the API already hides) plus the untouched {app="b"} row.
	want := queryTimeLines(t, conn, store, `{app="a"}`, base, base.Add(time.Minute))
	want = append(want, "secret x")
	sort.Strings(want)

	require.NoError(t, p.ProcessDue(context.Background(), t0))

	// The surviving physical rows must equal the query-time view + secret x.
	got := lines(t, conn, "t1")
	sort.Strings(got)
	require.Equal(t, want, got, "physical deletion must match the query-time filter")

	// The request is now processed.
	reqs, err := store.Unprocessed(context.Background())
	require.NoError(t, err)
	require.Empty(t, reqs, "request should be marked processed")

	// A second ProcessDue is a no-op (rows unchanged).
	require.NoError(t, p.ProcessDue(context.Background(), t0))
	require.Equal(t, got, lines(t, conn, "t1"))
}

func TestProcessor_SelectorOnly(t *testing.T) {
	p, store, conn, _ := newProcessor(t, 0, time.Hour, t0, t0)

	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "keep"}}},
		{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "secret x"}, {Timestamp: base.Add(time.Second), Line: "secret y"}}},
	}, nil))

	end := base.Add(time.Hour)
	_, err := store.Add(context.Background(), "t1", `{app="b"}`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)

	require.NoError(t, p.ProcessDue(context.Background(), t0))

	got := lines(t, conn, "t1")
	sort.Strings(got)
	// The bare selector {app="b"} deletes all its rows; only {app="a"}/keep remains.
	require.Equal(t, []string{"keep"}, got)
}

func TestProcessor_RespectsCancelPeriod(t *testing.T) {
	// CancelPeriod 1h; now = created + 59m -> nothing is due.
	p, store, conn, _ := newProcessor(t, time.Hour, time.Hour, t0.Add(59*time.Minute), t0)

	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "secret x"}}},
	}, nil))
	end := base.Add(time.Hour)
	_, err := store.Add(context.Background(), "t1", `{app="a"}`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)

	require.NoError(t, p.ProcessDue(context.Background(), t0.Add(59*time.Minute)))

	// The row is still there and the request is still unprocessed.
	got := lines(t, conn, "t1")
	require.Equal(t, []string{"secret x"}, got, "nothing deleted before the cancel period")
	reqs, err := store.Unprocessed(context.Background())
	require.NoError(t, err)
	require.Len(t, reqs, 1, "request still pending")
}

func TestProcessor_FaultIsolation(t *testing.T) {
	// Two due requests: req1's scan hits a transient failure, req2 must still
	// be processed in the same pass (per-request fault isolation), and req1 is
	// retried to completion on the next pass.
	conn, db := chtest.New(t)
	p, store, _ := buildProcessor(conn, db, 0, time.Hour, t0, t0)

	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: "keep a"},
			{Timestamp: base.Add(time.Millisecond), Line: "secret a"},
		}},
		{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: base, Line: "secret b"}}},
	}, nil))
	end := base.Add(time.Hour)
	_, err := store.Add(context.Background(), "t1", `{app="a"}`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)
	_, err = store.Add(context.Background(), "t1", `{app="b"}`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)

	// First pass with a conn that fails once: whichever request hits the
	// failure is skipped, the other must still be processed in the same pass.
	bad := &failConn{Conn: conn}
	p.Conn = bad
	err = p.ProcessDue(context.Background(), t0)
	require.Error(t, err, "the failed request must surface an error")
	remaining := lines(t, conn, "t1")
	_, aStill := contains(remaining, "secret a")
	_, bStill := contains(remaining, "secret b")
	require.NotEqual(t, aStill, bStill, "exactly one of the two requests was processed")
	reqs, err := store.Unprocessed(context.Background())
	require.NoError(t, err)
	require.Len(t, reqs, 1, "only the failed request is still pending")

	// Second pass (good conn): the remaining request completes.
	p.Conn = conn
	require.NoError(t, p.ProcessDue(context.Background(), t0))
	reqs, err = store.Unprocessed(context.Background())
	require.NoError(t, err)
	require.Empty(t, reqs, "the failed request was retried and marked processed")
}

func TestProcessor_RunQuietOnShutdown(t *testing.T) {
	p, _, _, _ := newProcessor(t, time.Hour, time.Hour, time.Now(), time.Now())
	var buf bytes.Buffer
	p.Logger = log.NewLogfmtLogger(&buf)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutdown already requested: ProcessDue fails with context.Canceled
	p.Run(ctx)
	require.Empty(t, buf.String())
}

func TestProcessor_ParserStageScans(t *testing.T) {
	// {app="a"} | logfmt | level="debug" must take the scan path (not DeleteRange).
	p, store, conn, _ := newProcessor(t, 0, time.Hour, t0, t0)

	w := &chstore.Writer{Conn: conn}
	base := t0
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{
			{Timestamp: base, Line: `level=debug msg="go away"`},
			{Timestamp: base.Add(time.Millisecond), Line: `level=info msg="keep me"`},
		}},
	}, nil))
	end := base.Add(time.Hour)
	_, err := store.Add(context.Background(), "t1", `{app="a"} | logfmt | level="debug"`,
		model.TimeFromUnixNano(base.UnixNano()), model.TimeFromUnixNano(end.UnixNano()))
	require.NoError(t, err)

	require.NoError(t, p.ProcessDue(context.Background(), t0))

	got := lines(t, conn, "t1")
	sort.Strings(got)
	require.Equal(t, []string{`level=info msg="keep me"`}, got, "only the debug line deleted")
}

// storedRows counts a tenant's rows including those only masked by a
// lightweight DELETE (apply_deleted_mask=0 shows them).
func storedRows(t *testing.T, conn driver.Conn, tenant string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count() FROM logs WHERE tenant = ? SETTINGS apply_deleted_mask = 0", tenant).Scan(&n))
	return n
}

func seedSecret(t *testing.T, conn driver.Conn, store *deletes.Store) {
	t.Helper()
	w := &chstore.Writer{Conn: conn}
	require.NoError(t, w.Write(context.Background(), "t1", []logproto.Stream{
		{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "keep"}}},
		{Labels: `{app="b"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "secret x"}, {Timestamp: t0.Add(time.Second), Line: "secret y"}}},
	}, nil))
	_, err := store.Add(context.Background(), "t1", `{app="b"}`,
		model.TimeFromUnixNano(t0.UnixNano()), model.TimeFromUnixNano(t0.Add(time.Hour).UnixNano()))
	require.NoError(t, err)
}

func TestProcessor_PhysicallyRemovesRows(t *testing.T) {
	p, store, conn, _ := newProcessor(t, 0, time.Hour, t0, t0)
	seedSecret(t, conn, store)
	require.NoError(t, p.ProcessDue(context.Background(), t0))
	require.Equal(t, []string{"keep"}, lines(t, conn, "t1"))
	require.Equal(t, uint64(1), storedRows(t, conn, "t1"), "masked rows must be gone from disk")
}

// maskFailConn fails the first APPLY DELETED MASK, to check the next pass retries it.
type maskFailConn struct {
	driver.Conn
	failed bool
}

func (f *maskFailConn) Exec(ctx context.Context, q string, args ...any) error {
	if !f.failed && strings.Contains(q, "APPLY DELETED MASK") {
		f.failed = true
		return context.DeadlineExceeded
	}
	return f.Conn.Exec(ctx, q, args...)
}

func TestProcessor_RetriesDeletedMask(t *testing.T) {
	conn, db := chtest.New(t)
	fc := &maskFailConn{Conn: conn}
	p, store, _ := buildProcessor(fc, db, 0, time.Hour, t0, t0)
	seedSecret(t, conn, store)

	require.Error(t, p.ProcessDue(context.Background(), t0))
	require.Equal(t, []string{"keep"}, lines(t, conn, "t1"), "rows are deleted even when the mask step fails")
	require.Equal(t, uint64(3), storedRows(t, conn, "t1"))

	require.NoError(t, p.ProcessDue(context.Background(), t0), "no request is due, the mask is retried")
	require.Equal(t, uint64(1), storedRows(t, conn, "t1"))
}
