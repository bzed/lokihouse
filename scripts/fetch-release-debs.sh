#!/bin/sh
# Download the runakefli .deb files of the latest GitHub release (needs gh and
# GH_TOKEN). Releases up to 0.14.0 shipped the package under its old name
# (lokihouse); those are not carried over. No release with a runakefli package
# yet is not an error.
#
#   scripts/fetch-release-debs.sh <out-dir>
set -eu
[ $# -eq 1 ] || { echo "usage: $0 <out-dir>" >&2; exit 2; }
mkdir -p "$1"
gh release download --pattern 'runakefli_*.deb' --dir "$1" 2>/dev/null || echo "no release runakefli .deb files yet" >&2
