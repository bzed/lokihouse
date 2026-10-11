#!/bin/sh
# govulncheck with a reviewed allowlist (scripts/govulncheck-ignore.txt).
# Fails on any called vulnerability that is not allowlisted.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ignore=$(grep -v -e '^#' -e '^$' "$here/govulncheck-ignore.txt" || true)
json=$(mktemp /tmp/govulncheck.XXXXXX)
trap 'rm -f "$json"' EXIT
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 -format json ./... > "$json"
# A finding whose first trace frame has a function is a called (reachable) one.
found=$(python3 - "$json" <<'EOF'
import json, sys
ids = set()
dec = json.JSONDecoder()
buf = open(sys.argv[1]).read()
i = 0
while i < len(buf):
    while i < len(buf) and buf[i].isspace():
        i += 1
    if i >= len(buf):
        break
    obj, i = dec.raw_decode(buf, i)
    f = obj.get("finding")
    if f and f.get("trace") and f["trace"][0].get("function"):
        ids.add(f["osv"])
print("\n".join(sorted(ids)))
EOF
)
bad=0
for id in $found; do
	if printf '%s\n' "$ignore" | grep -qx "$id"; then
		echo "allowlisted: $id"
	else
		echo "VULNERABLE: $id (see https://pkg.go.dev/vuln/$id)" >&2
		bad=1
	fi
done
exit $bad
