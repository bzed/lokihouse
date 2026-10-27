LOKI_COMMIT := bc1962860a1607eccb7e0c8ee206970b292b9e80
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')
REVISION   ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null)
BRANCH     ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILD_DATE ?= $(shell date -u -d "@$${SOURCE_DATE_EPOCH:-$$(date +%s)}" +%Y-%m-%dT%H:%M:%SZ)
PV := github.com/bzed/lokihouse/internal/build
LDFLAGS := -X $(PV).Version=$(VERSION) -X $(PV).Revision=$(REVISION) -X $(PV).Branch=$(BRANCH) -X $(PV).BuildDate=$(BUILD_DATE) -X $(PV).BuildUser=lokihouse
.PHONY: build test oracle oracle-downstream oracle-all bench soak
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/lokihouse ./cmd/lokihouse
test:
	go test ./...
oracle:
	go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
oracle-downstream:
	LOKIHOUSE_ORACLE_PUSHDOWN=downstream go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
oracle-all:
	@echo "=== Mode: off ==="
	LOKIHOUSE_ORACLE_PUSHDOWN=off go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
	@echo "=== Mode: downstream ==="
	LOKIHOUSE_ORACLE_PUSHDOWN=downstream go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
	@echo "=== Mode: on ==="
	go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
# The benchmark's own servers (test/bench/up.sh): ClickHouse on local disk,
# ClickHouse on S3 with and without its filesystem cache, and RustFS as the S3
# store for those and for Loki.
BENCH_ENV := LOKIHOUSE_TEST_CH_ADDR=localhost:19100 LOKIHOUSE_TEST_CH_CLUSTER= \
	LOKIHOUSE_BENCH_S3=localhost:19900 LOKIHOUSE_BENCH_CH_S3=localhost:19101 \
	LOKIHOUSE_BENCH_CH_S3_CACHE=localhost:19102
.PHONY: bench-up bench-down
bench-up:
	sh test/bench/up.sh
bench-down:
	sh test/bench/down.sh
bench: bench-up
	$(BENCH_ENV) go test -tags oracle,bench -run TestBench -count=1 -timeout 60m -v ./test/oracle/
# make bench-s3 runs the object-storage scenario against the remote S3 store
# of .secrets/s3.env (gitignored; S3_*/AWS_* variables, see
# test/oracle/bench_s3_test.go). BENCH_STORES picks loki, lokihouse-cache or
# both (default); LOKIHOUSE_BENCH_LINES, _ROUNDS and _SPAN in
# the environment size the run.
BENCH_S3_ENV ?= .secrets/s3.env
BENCH_STORES ?=
.PHONY: bench-s3 bench-s3-down
bench-s3:
	set -a; . ./$(BENCH_S3_ENV); set +a; \
	trap 'sh test/bench/down-remote.sh' EXIT; \
	sh test/bench/down-remote.sh; sh test/bench/up-remote.sh && \
	LOKIHOUSE_TEST_CH_ADDR=localhost:19102 LOKIHOUSE_TEST_CH_CLUSTER= LOKIHOUSE_BENCH_S3_REMOTE=1 \
	LOKIHOUSE_BENCH_CH_S3_CACHE=localhost:19102 \
	LOKIHOUSE_BENCH_STORES=$(BENCH_STORES) \
	go test -tags oracle,bench -run TestBench -count=1 -timeout 60m -v ./test/oracle/
bench-s3-down:
	set -a; . ./$(BENCH_S3_ENV); set +a; sh test/bench/down-remote.sh
soak:
	go test -tags oracle,soak -run TestSoak -count=1 -timeout 90m -v ./test/oracle/

CLUSTER_ENV := LOKIHOUSE_TEST_CH_ADDR=localhost:19001 LOKIHOUSE_TEST_CH_CLUSTER=lokihouse
.PHONY: cluster-up cluster-down test-cluster oracle-cluster
cluster-up:
	sh test/cluster/up.sh
cluster-down:
	sh test/cluster/down.sh
test-cluster: cluster-up
	$(CLUSTER_ENV) go test -count=1 ./...
oracle-cluster: cluster-up
	$(CLUSTER_ENV) go test -tags oracle -count=1 -timeout 30m ./test/oracle/...

GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
# Raised to 85 at the end of Task 5; never lowered afterwards.
COVERAGE_MIN ?= 85

.PHONY: fmt fmt-check vet lint vuln cover check
fmt:
	$(GOLANGCI_LINT) fmt
fmt-check:
	@out=$$($(GOLANGCI_LINT) fmt --diff); if [ -n "$$out" ]; then echo "$$out"; echo "run: make fmt" >&2; exit 1; fi
vet:
	go vet ./...
	go vet -tags oracle,bench ./test/...
lint:
	$(GOLANGCI_LINT) run ./...
vuln:
	sh scripts/govulncheck.sh
cover:
	COVERAGE_MIN=$(COVERAGE_MIN) sh scripts/coverage.sh
check: fmt-check vet lint vuln cover

.PHONY: docs man
docs:
	python3 -m venv /tmp/lokihouse-docs-venv
	/tmp/lokihouse-docs-venv/bin/pip install -q -r docs/requirements.txt
	/tmp/lokihouse-docs-venv/bin/mkdocs build --strict --site-dir site
man:
	mkdir -p bin
	go-md2man -in docs/man/lokihouse.1.md -out bin/lokihouse.1

.PHONY: docker docker-test
# Container image for the host architecture (podman, or ENGINE=docker).
ENGINE ?= $(shell command -v podman || command -v docker)
docker:
	$(ENGINE) build -t localhost/lokihouse:test \
		--build-arg VERSION=$(VERSION) --build-arg REVISION=$(REVISION) \
		--build-arg BRANCH=$(BRANCH) --build-arg BUILD_DATE=$(BUILD_DATE) .
docker-test: docker
	ENGINE=$(ENGINE) sh scripts/docker-test.sh localhost/lokihouse:test

.PHONY: deb
# Builds the .deb for the host architecture in a debian:trixie container (podman).
deb:
	out=$$(mktemp -d /tmp/lokihouse-deb.XXXXXX); \
	podman run --rm -v "$(CURDIR):/src:ro,Z" -v "$$out:/out:Z" docker.io/library/debian:trixie sh -euc '\
		apt-get update -qq && apt-get install -y -qq --no-install-recommends devscripts equivs build-essential lintian git rsync >/dev/null; \
		rsync -a --exclude .cache --exclude bin --exclude site /src/ /build/; cd /build; \
		git config --global --add safe.directory /build; \
		mk-build-deps -i -r -t "apt-get -y -qq --no-install-recommends" debian/control >/dev/null; \
		dpkg-buildpackage -us -uc -b; \
		lintian --fail-on error,warning --info ../lokihouse_*.deb; \
		apt-get install -y -qq systemd ../lokihouse_*.deb >/dev/null && sh debian/tests/smoke; \
		cp ../lokihouse_*.deb ../lokihouse_*.buildinfo ../lokihouse_*.changes /out/'; \
	echo "packages in $$out"; ls -l "$$out"
