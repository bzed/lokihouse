#!/bin/sh
# Total statement coverage gate (AGENTS.md: >= 85 %).
# Env: COVERAGE_MIN (default 85), COVERAGE_OUT (default coverage.out).
set -eu
min=${COVERAGE_MIN:-85}
out=${COVERAGE_OUT:-coverage.out}
pkgs=$(go list ./... | grep -v -e '/internal/chstore/chtest$' -e '/test/')
coverpkg=$(printf '%s\n' $pkgs | paste -sd, -)
# shellcheck disable=SC2086 # word splitting of the package list is intended
go test -race -count=1 -covermode=atomic -coverpkg="$coverpkg" -coverprofile="$out" $pkgs
total=$(go tool cover -func="$out" | awk '/^total:/ { sub("%", "", $3); print $3 }')
echo "total coverage: ${total}% (minimum ${min}%)"
if ! awk -v t="$total" -v m="$min" 'BEGIN { exit !(t + 0 >= m + 0) }'; then
	echo "coverage ${total}% is below the minimum ${min}%" >&2
	exit 1
fi
