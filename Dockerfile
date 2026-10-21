# Build:  podman build -t lokihouse .   (or docker build)
# Run:    podman run -p 3100:3100 -e LOKIHOUSE_CLICKHOUSE_ADDR=host:9000 lokihouse
#         Password: -e LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE=/run/secrets/x (mounted file)
#         or -e LOKIHOUSE_CLICKHOUSE_PASSWORD=...
# Multi-arch images are built by .github/workflows/docker.yml; the Go build runs
# on the build platform and cross-compiles, so no emulation is needed.

# Keep GO_VERSION equal to the toolchain line of go.mod.
ARG GO_VERSION=1.27.3

FROM --platform=$BUILDPLATFORM docker.io/library/golang:${GO_VERSION}-trixie AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BRANCH=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH}
RUN PV=github.com/bzed/lokihouse/internal/build && \
    go build -trimpath -ldflags "-s -w \
      -X $PV.Version=${VERSION} -X $PV.Revision=${REVISION} -X $PV.Branch=${BRANCH} \
      -X $PV.BuildDate=${BUILD_DATE} -X $PV.BuildUser=docker" \
    -o /out/lokihouse ./cmd/lokihouse

FROM gcr.io/distroless/static-debian13:nonroot
LABEL org.opencontainers.image.title="lokihouse" \
      org.opencontainers.image.description="Grafana Loki's HTTP API, with logs stored in ClickHouse" \
      org.opencontainers.image.source="https://github.com/bzed/lokihouse" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build /out/lokihouse /usr/bin/lokihouse
COPY docker/lokihouse.yaml /etc/lokihouse/lokihouse.yaml
EXPOSE 3100
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/lokihouse"]
CMD ["-config.file=/etc/lokihouse/lokihouse.yaml", "-config.expand-env=true"]
