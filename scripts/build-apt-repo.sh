#!/bin/sh
# Build the signed apt repository (reprepro) from directories of .deb files.
#
#   scripts/build-apt-repo.sh <out-dir> <suite>=<deb-dir> [<suite>=<deb-dir> ...]
#
# Suites are the codenames in apt/conf/distributions; a suite whose directory is
# missing or has no .deb files is left empty. <out-dir> is replaced. The
# repository is signed with the only secret key in GNUPGHOME (CI imports it from
# the APT_GPG_PRIVATE_KEY secret into a temporary home; locally point GNUPGHOME
# at a throwaway directory, never at your own keyring). Needs reprepro and gpg.
set -eu

[ $# -ge 2 ] || { echo "usage: $0 <out-dir> <suite>=<deb-dir>..." >&2; exit 2; }
out=$1
shift
here=$(cd "$(dirname "$0")/.." && pwd)

[ -n "${GNUPGHOME:-}" ] || { echo "GNUPGHOME must point at a keyring with the signing key" >&2; exit 2; }
[ "$(gpg --list-secret-keys --with-colons | grep -c '^sec')" -eq 1 ] ||
	{ echo "$GNUPGHOME must hold exactly one secret key" >&2; exit 2; }

rm -rf "$out"
mkdir -p "$out"
base=$(mktemp -d)
trap 'rm -rf "$base"' EXIT
cp -r "$here/apt/conf" "$base/conf"

# Every suite gets its (possibly empty) indexes, so apt never sees a 404.
reprepro --silent --basedir "$base" --outdir "$out" export

for spec in "$@"; do
	suite=${spec%%=*}
	dir=${spec#*=}
	grep -q "^Codename: $suite\$" "$base/conf/distributions" ||
		{ echo "unknown suite $suite" >&2; exit 2; }
	for deb in "$dir"/*.deb; do
		[ -e "$deb" ] || continue
		reprepro --silent --basedir "$base" --outdir "$out" includedeb "$suite" "$deb"
	done
done
# only the published tree: no database, no config
rm -rf "$out/db" "$out/conf"

# the public key, armored (for humans) and binary (for signed-by=)
gpg --armor --export >"$out/lokihouse-archive-keyring.asc"
gpg --export >"$out/lokihouse-archive-keyring.gpg"
