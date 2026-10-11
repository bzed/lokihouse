// Package build holds lokihouse's build-time version information, following
// Loki's pkg/util/build: the variables are set from -ldflags (Makefile) and
// exposed so the binary reports them.
package build

import "runtime"

// These variables are set from the Makefile LDFLAGS.
var (
	Version   string
	Revision  string
	Branch    string
	BuildUser string
	BuildDate string
	GoVersion string
)

func init() { GoVersion = runtime.Version() }

// Info returns the build information for the /loki/api/v1/status/buildinfo
// endpoint and -version output.
func Info() map[string]string {
	return map[string]string{
		"version": Version, "revision": Revision, "branch": Branch,
		"buildUser": BuildUser, "buildDate": BuildDate, "goVersion": GoVersion,
	}
}
