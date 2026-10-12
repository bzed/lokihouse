#!/bin/sh
# Download the .deb files a suite of the already published apt repository holds,
# so a deploy that rebuilds the other suite does not drop it. A missing
# repository or suite is not an error (nothing is downloaded).
#
#   scripts/fetch-published-debs.sh <repo-url> <suite> <out-dir>
set -eu

[ $# -eq 3 ] || { echo "usage: $0 <repo-url> <suite> <out-dir>" >&2; exit 2; }
url=${1%/}
suite=$2
out=$3
mkdir -p "$out"

for arch in amd64 arm64; do
	index=$(curl -fsS --proto '=https' "$url/dists/$suite/main/binary-$arch/Packages" 2>/dev/null) || continue
	printf '%s\n' "$index" | sed -n 's/^Filename: //p' | while read -r file; do
		curl -fsS --proto '=https' -o "$out/$(basename "$file")" "$url/$file"
	done
done
