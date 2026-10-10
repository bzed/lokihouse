LOKI_COMMIT := fbc3f0770b8c56ebec56fbd0fb16dc3b606822d1
.PHONY: build test oracle
build:
	go build -o bin/lokihouse ./cmd/lokihouse
test:
	go test ./...
oracle:
	go test -tags oracle -count=1 -timeout 30m ./test/oracle/...
