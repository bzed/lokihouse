#!/bin/sh
# Delete every object under the given key prefixes in the remote S3 bucket
# (S3_* and AWS_* from the environment, as for up-remote.sh): the objects
# ClickHouse's S3 disks leave behind when their containers are removed.
set -eu
: "${S3_ENDPOINT:?}" "${S3_REGION:?}" "${S3_BUCKET:?}" "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
host=${S3_ENDPOINT#https://}
# The credentials go in a private config file, not on the command line.
cfg=$(mktemp /tmp/s3cmd.XXXXXX)
trap 'rm -f "$cfg"' EXIT
cat >"$cfg" <<CFG
[default]
access_key = $AWS_ACCESS_KEY_ID
secret_key = $AWS_SECRET_ACCESS_KEY
host_base = $host
host_bucket = %(bucket)s.$host
bucket_location = $S3_REGION
CFG
for prefix in "$@"; do
  s3cmd -c "$cfg" --quiet --recursive --force del "s3://$S3_BUCKET/$prefix"
done
