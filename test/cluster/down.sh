#!/bin/sh
podman rm -f ch1 ch2 ch3 ch4 keeper >/dev/null 2>&1 || true
podman network rm lhnet >/dev/null 2>&1 || true
