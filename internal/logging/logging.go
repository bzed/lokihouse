// Package logging builds lokihouse's go-kit logger from Loki's server.log_level
// and server.log_format keys.
package logging

import (
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dskitlog "github.com/grafana/dskit/log"
)

// New returns a goroutine-safe logger writing to w in format ("logfmt" or
// "json"), dropping messages below lvl, with a UTC timestamp and caller.
func New(w io.Writer, lvl dskitlog.Level, format string) (log.Logger, error) {
	sw := log.NewSyncWriter(w)
	var l log.Logger
	switch format {
	case "logfmt":
		l = log.NewLogfmtLogger(sw)
	case "json":
		l = log.NewJSONLogger(sw)
	default:
		return nil, fmt.Errorf("unknown log format %q (want logfmt or json)", format)
	}
	l = level.NewFilter(l, lvl.Option)
	return log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.Caller(4)), nil
}
