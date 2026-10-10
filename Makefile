LOKI_COMMIT := fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1
.PHONY: build test oracle
build:
	go build -o bin/lokihouse ./cmd/lokihouse
test:
	go test ./...
oracle:
	go test -tags oracle -count=1 -timeout 30m ./test/oracle/...

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
