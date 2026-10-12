package ruler_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak. The ruler runs
// evaluation and poller loops that must stop when the ruler is stopped.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}

func leakIgnores() []goleak.Option {
	return []goleak.Option{
		// The bce-sdk-go logger goroutine is started in the package's init and
		// lives for the process; it is not lokihouse's to stop.
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
		// Loki keeps its ruler WAL registry and per-tenant MemStore in
		// package-level variables (pkg/ruler/compat.go). Only the last ruler
		// created in a process has its resources stopped on shutdown; earlier
		// rulers' MemStore cleanup loop and WAL record-size loop outlive their
		// Stop. These are Loki's, not lokihouse's, to reclaim.
		goleak.IgnoreTopFunction("github.com/grafana/loki/v3/pkg/ruler.(*MemStore).run"),
		goleak.IgnoreTopFunction("github.com/grafana/loki/v3/pkg/ruler/storage/wal.(*Storage).recordSize"),
	}
}
