package chstore_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak: the sample pipeline
// starts a producer and worker goroutines per query, and each must stop when
// its iterator is closed or its context ends.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}

func leakIgnores() []goleak.Option {
	// The bce-sdk-go logger goroutine is started in the package's init and
	// lives for the process; it is not lokihouse's to stop.
	return []goleak.Option{
		goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"),
	}
}
