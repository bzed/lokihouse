package config

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const examplePath = "../../lokihouse.example.yaml"

// TestExampleShowsEveryKey checks that every lokihouse-owned config key is in
// lokihouse.example.yaml, active or commented out.
func TestExampleShowsEveryKey(t *testing.T) {
	b, err := os.ReadFile(examplePath)
	require.NoError(t, err)
	example := string(b)
	var walk func(typ reflect.Type)
	walk = func(typ reflect.Type) {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			key, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if key == "" || key == "-" {
				continue
			}
			require.Regexp(t, `(?m)^\s*#?\s*`+regexp.QuoteMeta(key)+`:`, example, "config key %s missing from lokihouse.example.yaml", key)
			if f.Type.Kind() == reflect.Struct && !lokiOwned[key] && f.Type.PkgPath() == "" {
				walk(f.Type)
			}
		}
	}
	walk(reflect.TypeOf(Config{}))
}

// keyLine matches a commented-out option or list item; prose comments do not match.
var keyLine = regexp.MustCompile(`^(\s*)# ?(\s*)((?:[a-z_]+:(?: |$)|- ).*)$`)

// notOptions are commented lines that look like options but are prose or
// alternatives to an active option.
var notOptions = regexp.MustCompile(`^\s*#\s+(on|off|downstream|true|false):|^\s*# (local|password): |enable_sharding|systemd-creds|credstore`)

// TestExampleWithEveryOptionUncommented loads the example with every
// commented-out option enabled: all of them must be accepted by the strict
// parser and pass validation.
func TestExampleWithEveryOptionUncommented(t *testing.T) {
	b, err := os.ReadFile(examplePath)
	require.NoError(t, err)
	var out strings.Builder
	for _, l := range strings.Split(string(b), "\n") {
		if notOptions.MatchString(l) {
			continue
		}
		if m := keyLine.FindStringSubmatch(l); m != nil {
			l = m[1] + m[2] + m[3]
		}
		out.WriteString(l + "\n")
	}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lokihouse.clickhouse-password"), []byte("x\n"), 0o600))
	t.Setenv("CREDENTIALS_DIRECTORY", dir)
	p := filepath.Join(dir, "all.yaml")
	require.NoError(t, os.WriteFile(p, []byte(out.String()), 0o600))
	c, err := Load(p, true)
	require.NoError(t, err)
	require.True(t, c.RulerEnabled(), "the ruler block of the example is enabled with everything uncommented")
	require.Equal(t, "x", c.ClickHouse.Password)
}
