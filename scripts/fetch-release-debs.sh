#!/bin/sh
# Download the .deb files of the latest GitHub release (needs gh and GH_TOKEN).
# No release yet is not an error.
#
#   scripts/fetch-release-debs.sh <out-dir>
set -eu
[ $# -eq 1 ] || { echo "usage: $0 <out-dir>" >&2; exit 2; }
mkdir -p "$1"
gh release download --pattern '*.deb' --dir "$1" 2>/dev/null || echo "no release .deb files yet" >&2
