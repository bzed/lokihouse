#!/bin/sh
# Start the benchmark's servers (podman): RustFS as the S3 store, and three
# single-node ClickHouse servers with MergeTree on local disk, on RustFS, and
# on RustFS behind ClickHouse's filesystem cache. RustFS keeps no volume, so
# every up starts with empty buckets.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ch=docker.io/clickhouse/clickhouse-server:26.9
s3=docker.io/rustfs/rustfs:1.0.3
podman network exists lhbench || podman network create lhbench >/dev/null
podman container exists lhb-rustfs || podman run -d --name lhb-rustfs --network lhbench -p 19900:9000 "$s3" >/dev/null
n=0
# ClickHouse checks its S3 disks at startup, so the bucket must exist first.
# RustFS answers /health before it accepts writes; retry until the bucket is
# there (200 when created, 409 when it already exists).
until code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT --aws-sigv4 aws:amz:us-east-1:s3 \
    --user rustfsadmin:rustfsadmin http://127.0.0.1:19900/clickhouse 2>/dev/null) &&
  { [ "$code" = 200 ] || [ "$code" = 409 ]; }; do
  n=$((n + 1)); [ "$n" -lt 60 ] || { echo "rustfs not ready" >&2; exit 1; }; sleep 1
done
start_ch() { # name port [config]
  podman container exists "$1" && return
  if [ $# -eq 3 ]; then
    podman run -d --name "$1" --network lhbench -p "$2:9000" -e CLICKHOUSE_SKIP_USER_SETUP=1 \
      -v "$here/$3:/etc/clickhouse-server/config.d/storage.xml:Z" "$ch" >/dev/null
  else
    podman run -d --name "$1" --network lhbench -p "$2:9000" -e CLICKHOUSE_SKIP_USER_SETUP=1 "$ch" >/dev/null
  fi
}
start_ch lhb-ch 19100
start_ch lhb-ch-s3 19101 s3.xml
start_ch lhb-ch-s3cache 19102 s3cache.xml
for c in lhb-ch lhb-ch-s3 lhb-ch-s3cache; do
  n=0
  until podman exec "$c" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; do
    n=$((n + 1)); [ "$n" -lt 60 ] || { echo "$c not ready" >&2; podman logs --tail 20 "$c" >&2; exit 1; }; sleep 1
  done
done
echo "bench servers up: rustfs localhost:19900, clickhouse localhost:19100 (disk), 19101 (s3), 19102 (s3 + cache)"
