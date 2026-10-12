package tail_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak. Tail connections spawn
// per-connection goroutines that must stop when the WebSocket closes or the
// context ends.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}

func leakIgnores() []goleak.Option {
	return []goleak.Option{
		// The bce-sdk-go logger goroutine is started in the package's init and
		// lives for the process; it is not lokihouse's to stop.
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
	}
}
