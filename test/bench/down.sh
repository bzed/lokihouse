#!/bin/sh
# Stop and remove the benchmark's servers.
set -u
podman rm -f lhb-ch lhb-ch-s3 lhb-ch-s3cache lhb-rustfs >/dev/null 2>&1
podman network rm lhbench >/dev/null 2>&1
exit 0
