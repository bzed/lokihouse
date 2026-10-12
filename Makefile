LOKI_COMMIT := fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1
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
bench:
	go test -tags oracle,bench -run TestBench -count=1 -timeout 60m -v ./test/oracle/
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
