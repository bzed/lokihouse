package ruler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate_WALDir(t *testing.T) {
	base := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		wal, rules, rulePath string
		ok                   bool
	}{
		"sibling dirs":           {wal: filepath.Join(base, "ruler-wal"), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "rules-temp"), ok: true},
		"default relative":       {wal: "ruler-wal", rules: filepath.Join(base, "rules"), rulePath: "rules-temp", ok: true},
		"empty":                  {wal: "", rules: filepath.Join(base, "rules"), rulePath: "rules-temp"},
		"working directory":      {wal: ".", rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"filesystem root":        {wal: "/", rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"parent of cwd":          {wal: filepath.Dir(cwd), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "t")},
		"parent of rule storage": {wal: base, rules: filepath.Join(base, "rules"), rulePath: filepath.Join(t.TempDir(), "t")},
		"same as rule_path":      {wal: filepath.Join(base, "x"), rules: filepath.Join(base, "rules"), rulePath: filepath.Join(base, "x")},
	} {
		t.Run(name, func(t *testing.T) {
			rc, sc := Defaults()
			rc.RemoteWrite.Enabled = false
			rc.WAL.Dir, rc.RulePath = tc.wal, tc.rulePath
			sc.Backend, sc.Filesystem.Directory = "filesystem", tc.rules
			require.NoError(t, checkWALDir(rc, sc), "remote write disabled: Loki never wipes the WAL dir")

			rc.RemoteWrite.Enabled = true
			err := checkWALDir(rc, sc)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "ruler.wal.dir")
			}
		})
	}
}
