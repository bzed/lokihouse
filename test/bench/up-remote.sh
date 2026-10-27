#!/bin/sh
# Start the benchmark's ClickHouse servers (podman) on a remote S3 store
# instead of RustFS: one with MergeTree on S3 behind ClickHouse's filesystem
# cache. (No uncached server: its reads would measure the network latency to
# the store, which nothing else in the comparison pays.) The store comes from the environment (S3_ENDPOINT,
# S3_REGION, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY; see
# .secrets/s3.env); each up uses a fresh prefix of its own in the bucket.
# The port matches up.sh (19102), so run only one of the two.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ch=docker.io/clickhouse/clickhouse-server:26.9
: "${S3_ENDPOINT:?}" "${S3_REGION:?}" "${S3_BUCKET:?}" "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
host=${S3_ENDPOINT#https://}
# Every prefix used, for down-remote.sh to empty.
prefixes=$here/../../.cache/bench/remote-prefixes
mkdir -p "$(dirname "$prefixes")"
start_ch() { # name port config
  podman run -d --name "$1" -p "$2:9000" -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e S3_REGION \
    -e "BENCH_S3_URL=https://$host/$S3_BUCKET/$run/$1/" \
    -v "$here/$3:/etc/clickhouse-server/config.d/storage.xml:Z" "$ch" >/dev/null
}
# ClickHouse writes a probe object to its S3 disks at startup, and Hetzner
# Object Storage now and then answers a PUT with 403; a server that stopped on
# it is started again with a fresh prefix.
for attempt in 1 2 3 4 5; do
  run=lokihouse-bench-ch-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
  podman rm -f lhb-rch-s3cache >/dev/null 2>&1
  echo "$run" >>"$prefixes"
  start_ch lhb-rch-s3cache 19102 s3cache-remote.xml
  ok=1
  for c in lhb-rch-s3cache; do
    n=0
    until podman exec "$c" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; do
      n=$((n + 1))
      if [ "$n" -ge 30 ] || [ "$(podman inspect -f '{{.State.Running}}' "$c")" != true ]; then ok=0; break; fi
      sleep 1
    done
    [ "$ok" = 1 ] || break
  done
  if [ "$ok" = 1 ]; then
    echo "bench servers up: clickhouse localhost:19102 (remote s3 + cache), prefix $run/"
    exit 0
  fi
  echo "attempt $attempt: a server did not start, retrying" >&2
done
echo "bench servers not ready" >&2
podman logs --tail 20 lhb-rch-s3cache >&2
exit 1
