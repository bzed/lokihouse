#!/bin/sh
# Test a built lokihouse image against a throwaway ClickHouse: version, /ready,
# push and query. Works with podman or docker.
#
#   scripts/docker-test.sh [image]        (default: localhost/lokihouse:test)
set -eu

image=${1:-localhost/lokihouse:test}
engine=${ENGINE:-$(command -v podman || command -v docker)}
ch_image=${CH_IMAGE:-docker.io/clickhouse/clickhouse-server:26.9}
id=$$
net=lokihouse-test-$id
ch=lokihouse-test-ch-$id
lh=lokihouse-test-lh-$id

cleanup() {
	$engine rm -f "$lh" "$ch" >/dev/null 2>&1 || true
	$engine network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

$engine run --rm "$image" -version | grep -q '^lokihouse, version '

$engine network create "$net" >/dev/null
$engine run -d --name "$ch" --network "$net" -e CLICKHOUSE_SKIP_USER_SETUP=1 \
	--ulimit nofile=262144:262144 "$ch_image" >/dev/null
i=0
until $engine exec "$ch" clickhouse-client -q 'SELECT 1' >/dev/null 2>&1; do
	i=$((i + 1))
	[ $i -lt 60 ] || { echo "clickhouse did not start" >&2; exit 1; }
	sleep 2
done

port=$(( 20000 + id % 20000 ))
$engine run -d --name "$lh" --network "$net" -p "127.0.0.1:$port:3100" \
	-e LOKIHOUSE_CLICKHOUSE_ADDR="$ch:9000" "$image" >/dev/null
i=0
until curl -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1; do
	i=$((i + 1))
	if [ $i -ge 30 ]; then
		echo "lokihouse did not become ready" >&2
		$engine logs "$lh" >&2
		exit 1
	fi
	sleep 2
done

ts=$(date +%s)000000000
curl -fsS -X POST "http://127.0.0.1:$port/loki/api/v1/push" -H 'Content-Type: application/json' \
	-d "{\"streams\":[{\"stream\":{\"app\":\"docker-test\"},\"values\":[[\"$ts\",\"hello from the image\"]]}]}"
i=0
until curl -fsS -G "http://127.0.0.1:$port/loki/api/v1/query_range" \
	--data-urlencode 'query={app="docker-test"}' --data-urlencode "start=$((ts - 60000000000))" \
	--data-urlencode "end=$((ts + 60000000000))" | grep -q 'hello from the image'; do
	i=$((i + 1))
	[ $i -lt 15 ] || { echo "pushed line not found" >&2; $engine logs "$lh" >&2; exit 1; }
	sleep 1
done
echo "image $image ok"
