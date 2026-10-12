#!/bin/sh
# Start a 2-shard x 2-replica ClickHouse cluster "lokihouse" with one Keeper (podman).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
img=docker.io/clickhouse/clickhouse-server:26.9
work=${LOKIHOUSE_CLUSTER_DIR:-$here/../../.cache/cluster}
mkdir -p "$work"
podman network exists lhnet || podman network create lhnet >/dev/null
podman container exists keeper || podman run -d --name keeper --network lhnet \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$here/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:Z" "$img" >/dev/null
for i in 1 2 3 4; do
  printf '<clickhouse><macros><shard>%s</shard><replica>ch%s</replica></macros></clickhouse>\n' \
    $(( (i + 1) / 2 )) "$i" > "$work/macros$i.xml"
  podman container exists "ch$i" || podman run -d --name "ch$i" --network lhnet -p "1900$i:9000" \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$here/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:Z" \
    -v "$work/macros$i.xml:/etc/clickhouse-server/config.d/macros.xml:Z" "$img" >/dev/null
done
for i in 1 2 3 4; do
  n=0
  until podman exec "ch$i" clickhouse-client -q "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; do
    n=$((n + 1)); [ "$n" -lt 60 ] || { echo "ch$i not ready" >&2; exit 1; }; sleep 1
  done
done
echo "cluster lokihouse up: localhost:19001-19004"
