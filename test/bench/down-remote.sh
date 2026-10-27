#!/bin/sh
# Stop and remove up-remote.sh's servers, then delete what they left in the
# remote bucket (needs the same S3_* and AWS_* variables as up-remote.sh).
set -u
here=$(cd "$(dirname "$0")" && pwd)
prefixes=$here/../../.cache/bench/remote-prefixes
podman rm -f lhb-rch-s3cache >/dev/null 2>&1
if [ -s "$prefixes" ]; then
  # shellcheck disable=SC2046 # prefixes are plain words
  sh "$here/clean-remote.sh" $(cat "$prefixes") && : >"$prefixes"
fi
exit 0
