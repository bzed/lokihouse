package server_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak. server.Run starts
// background loops (delete processing, patterns, the ruler) that must stop on
// shutdown.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}

func leakIgnores() []goleak.Option {
	return []goleak.Option{
		// The bce-sdk-go logger goroutine is started in the package's init and
		// lives for the process; it is not lokihouse's to stop.
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
		// Loki's tail.Tailer.loop (pkg/querier/tail) is started by the server's
		// TailHandler for each tail request and is stopped only when the Tailer
		// is closed (max duration or all ingester clients dropped), not when the
		// HTTP request context is cancelled. A tail request in a test can leave
		// the loop briefly alive (sleeping in its throttle) when the test ends;
		// it is Loki's to reclaim. IgnoreAnyFunction matches the goroutine even
		// while its top-of-stack is the runtime time.Sleep inside the loop.
		goleak.IgnoreAnyFunction("github.com/grafana/loki/v3/pkg/querier/tail.(*Tailer).loop"),
	}
}
