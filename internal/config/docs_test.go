package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// lokiOwned are blocks whose keys are Loki's own; the docs link to Loki's
// reference instead of listing every key.
var lokiOwned = map[string]bool{"ruler": true, "ruler_storage": true, "limits_config": true}

// TestConfigurationDocsCoverEveryKey walks the Config struct and checks that
// every lokihouse-owned config key appears in docs/reference/configuration.md
// with its full dotted name in backticks.
func TestConfigurationDocsCoverEveryKey(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "reference", "configuration.md"))
	require.NoError(t, err)
	doc := string(b)
	var walk func(prefix string, typ reflect.Type)
	walk = func(prefix string, typ reflect.Type) {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			key, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if key == "" || key == "-" {
				continue
			}
			full := prefix + key
			require.Contains(t, doc, "`"+full+"`", "config key %s missing from docs/reference/configuration.md", full)
			if f.Type.Kind() == reflect.Struct && !lokiOwned[key] && f.Type.PkgPath() == "" {
				walk(full+".", f.Type)
			}
		}
	}
	walk("", reflect.TypeOf(Config{}))
}
