# Build:  podman build -t runakefli .   (or docker build)
# Run:    podman run -p 3100:3100 -e RUNAKEFLI_CLICKHOUSE_ADDR=host:9000 runakefli
#         Password: -e RUNAKEFLI_CLICKHOUSE_PASSWORD_FILE=/run/secrets/x (mounted file)
#         or -e RUNAKEFLI_CLICKHOUSE_PASSWORD=...
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
RUN PV=github.com/bzed/runakefli/internal/build && \
    go build -trimpath -ldflags "-s -w \
      -X $PV.Version=${VERSION} -X $PV.Revision=${REVISION} -X $PV.Branch=${BRANCH} \
      -X $PV.BuildDate=${BUILD_DATE} -X $PV.BuildUser=docker" \
    -o /out/runakefli ./cmd/runakefli

FROM gcr.io/distroless/static-debian13:nonroot
LABEL org.opencontainers.image.title="runakefli" \
      org.opencontainers.image.description="The Loki® HTTP API, with logs stored in ClickHouse®" \
      org.opencontainers.image.source="https://github.com/bzed/runakefli" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build /out/runakefli /usr/bin/runakefli
COPY docker/runakefli.yaml /etc/runakefli/runakefli.yaml
EXPOSE 3100
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/runakefli"]
CMD ["-config.file=/etc/runakefli/runakefli.yaml", "-config.expand-env=true"]
