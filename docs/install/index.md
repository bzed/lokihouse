# Installing lokihouse

> **Release candidate.** lokihouse is feature complete but not yet proven in production. CI builds the packages and
> images below for every release; the apt suite `trixie-snapshot` follows
> `main`.

There are three ways to run lokihouse. All of them need a running
[ClickHouse](https://clickhouse.com/) server (26.9 or later).

1. [Debian package](#debian-package-apt-repository) (Debian 13 "trixie",
   amd64 and arm64): the easiest, with a hardened systemd unit.
2. [Container image](#container-image) (linux/amd64 and linux/arm64).
3. [Build from source](#build-from-source-manual-install): a static Go binary
   that you run under any supervisor.

## Debian package (apt repository)

The repository is signed and published with this documentation. It has two
suites: `trixie` (releases) and `trixie-snapshot` (the latest build of `main`).
Packages are built for amd64 and arm64.

```sh
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl -fsSLo /etc/apt/keyrings/lokihouse-archive-keyring.asc \
    https://bzed.github.io/lokihouse/apt/lokihouse-archive-keyring.asc
# compare the fingerprint with the one below
gpg --show-keys --with-fingerprint /etc/apt/keyrings/lokihouse-archive-keyring.asc
sudo tee /etc/apt/sources.list.d/lokihouse.sources <<'EOF'
Types: deb
URIs: https://bzed.github.io/lokihouse/apt
Suites: trixie
Components: main
Signed-By: /etc/apt/keyrings/lokihouse-archive-keyring.asc
EOF
sudo apt update && sudo apt install lokihouse
```

The key's fingerprint is `74E9 21AC B040 DCC7 2C02  A144 1193 7B6E A8AF C649`.
Use `Suites: trixie-snapshot` to follow `main` instead of the releases.

The package installs `/usr/bin/lokihouse`, the man page, a hardened systemd
unit (dynamic user, state in `/var/lib/lokihouse`) and
`/etc/lokihouse/lokihouse.yaml`. It does **not** start the service: edit the
config first (at least `clickhouse.addr`; see
[the configuration reference](../reference/configuration.md)), then:

```sh
sudo systemctl enable --now lokihouse
curl -fsS http://localhost:3100/ready
```

The default listen address is `127.0.0.1`; change `server.http_listen_address`
to accept remote clients. Put the ClickHouse password in a systemd credential
as described in `/usr/share/doc/lokihouse/README.Debian`. `apt upgrade` keeps
lokihouse current; each release also has its `.deb` files and a `SHA256SUMS`
file on the [GitHub releases page](https://github.com/bzed/lokihouse/releases).
Then continue with [Step 5: first push and query](#step-5-first-push-and-query).

## Container image

```sh
podman run -d --name lokihouse -p 3100:3100 \
    -e LOKIHOUSE_CLICKHOUSE_ADDR=clickhouse.example.org:9000 \
    ghcr.io/bzed/lokihouse:latest
```

(`docker run` works the same.) The image is based on distroless `static`, runs
as a non-root user and listens on `0.0.0.0:3100`. To reach it on another host
port, just map it (`-p 8080:3100`). To change the port inside the container,
set `LOKIHOUSE_HTTP_LISTEN_PORT` and map that port (`-e
LOKIHOUSE_HTTP_LISTEN_PORT=8080 -p 8080:8080`); it must be above 1023, as the
container runs as non-root. The built-in config reads these variables:
`LOKIHOUSE_HTTP_LISTEN_PORT` (default `3100`), `LOKIHOUSE_CLICKHOUSE_ADDR` (default `clickhouse:9000`),
`LOKIHOUSE_CLICKHOUSE_DATABASE` (`lokihouse`), `LOKIHOUSE_CLICKHOUSE_USER`
(`default`), `LOKIHOUSE_CLICKHOUSE_PASSWORD` and `LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE`
(both unset: no password).

### ClickHouse password

The quick way is `-e LOKIHOUSE_CLICKHOUSE_PASSWORD=secret`. Environment
variables show up in `docker inspect` and `/proc`, and the value must not
contain a single quote, so prefer a file: mount it read-only and point
`LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE` at it. Set only one of the two; setting
both is a startup error.

```sh
install -m 0600 /dev/null clickhouse-password
printf '%s' 'secret' > clickhouse-password   # a trailing newline is stripped

podman run -d --name lokihouse -p 3100:3100 \
    -e LOKIHOUSE_CLICKHOUSE_ADDR=clickhouse.example.org:9000 \
    -e LOKIHOUSE_CLICKHOUSE_USER=lokihouse \
    -e LOKIHOUSE_CLICKHOUSE_PASSWORD_FILE=/run/secrets/clickhouse-password \
    -v ./clickhouse-password:/run/secrets/clickhouse-password:ro \
    ghcr.io/bzed/lokihouse:latest
```

The container runs as `nonroot` (uid 65532), so the file must be readable by
that user (with rootless podman, add `:U` to the volume or `chown` the file).
With podman or Docker secrets, use `--secret clickhouse-password` (podman) or
a Compose/Swarm `secrets:` entry, which appear under `/run/secrets/`.

For anything else, mount your own config file (the image runs with
`-config.expand-env=true`) and set `password_file:` in it:

```sh
podman run -d -p 3100:3100 \
    -v ./lokihouse.yaml:/etc/lokihouse/lokihouse.yaml:ro \
    -v ./clickhouse-password:/run/secrets/clickhouse-password:ro \
    ghcr.io/bzed/lokihouse:latest
```

Tags: `X.Y.Z` and `X.Y` for each release, and `latest`. Build it yourself with
`make docker` (podman or docker) and test it with `make docker-test`, which
starts a throwaway ClickHouse, pushes a line and queries it back.

## Build from source (manual install)

The steps below build the static binary and run it under systemd.

### Prerequisites

- **A running ClickHouse server** (26.9 or later), single-node or cluster.
  The [configuration reference](../reference/configuration.md#clickhouse) covers
  the keys you will set. For a single server on `localhost:9000`, the
  default [ClickHouse container image](https://hub.docker.com/r/clickhouse/clickhouse-server)
  works out of the box.
- **Go** (only needed for the build step; the binary is static —
  `CGO_ENABLED=0` — and needs no Go toolchain or shared libraries at
  runtime).
- **systemd** (or an equivalent process supervisor; the unit file below is
  systemd-specific, but the same `ExecStart` line works under any supervisor).

### Step 1 — Build the binary

From the repository root:

```sh
make build
```

This produces `bin/lokihouse`, a static binary. The `Makefile` adds
version/revision/date `ldflags` so `lokihouse -version` reports the build.
An equivalent one-liner:

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/lokihouse ./cmd/lokihouse
```

Copy `bin/lokihouse` to its final location, e.g. `/usr/local/bin/lokihouse`.

### Step 2 — Write the config

Create `/etc/lokihouse/lokihouse.yaml`. A minimal working config for a
single ClickHouse server on `localhost:9000` with the default `default`
user (no password):

```yaml
server:
  http_listen_port: 3100
clickhouse:
  addr: ["localhost:9000"]
  database: lokihouse
  username: default
```

If your ClickHouse user has a password, either set `password:` in the
config (not recommended for production) or, better, write the password to a
`0o600` file and reference it with `password_file:`:

```yaml
clickhouse:
  addr: ["localhost:9000"]
  database: lokihouse
  username: default
  password_file: /run/credentials/lokihouse.service/lokihouse.clickhouse-password
```

The full key-by-key reference is in
[Reference → Configuration](../reference/configuration.md).

### Step 3 — Create the systemd unit

Create `/etc/systemd/system/lokihouse.service`:

```ini
[Unit]
Description=lokihouse — Loki HTTP API on ClickHouse
After=network.target clickhouse.service

[Service]
Type=exec
User=lokihouse
Group=lokihouse
ExecStart=/usr/local/bin/lokihouse -config.file=/etc/lokihouse/lokihouse.yaml
Restart=on-failure
RestartSec=5

# Hardening. Adjust the ReadWritePaths lines to your actual paths:
#   - the ruler storage dir, if you enable the ruler
#   - the delete WAL dir, if you enable the ruler with remote write
ProtectHome=true
ProtectSystem=full
NoNewPrivileges=true
# If you use password_file with a systemd-credentials path, uncomment:
# LoadCredentialEncrypted=lokihouse.clickhouse-password:/run/credentials/lokihouse.service/lokihouse.clickhouse-password
```

Create the service user (no login):

```sh
sudo useradd --system --user-group --home-dir /var/lib/lokihouse \
    --no-create-home --shell /usr/sbin/nologin lokihouse
sudo mkdir -p /etc/lokihouse /var/lib/lokihouse
sudo chown lokihouse:lokihouse /etc/lokihouse /var/lib/lokihouse
```

If you use `password_file` with a plain file (the default in the config
above), create it with the right owner and mode:

```sh
sudo install -m 600 -o lokihouse -g lokihouse /dev/null \
    /run/credentials/lokihouse.service/lokihouse.clickhouse-password
# then write the password into that file
```

(If you prefer, the same path can be supplied via systemd credentials with
`LoadCredentialEncrypted=` / `SetCredentialEncrypted=`; the config key is
the same either way.)

### Step 4 — Start and health-check

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now lokihouse
systemctl status lokihouse
```

Health checks (both are served by lokihouse, both are Loki-identical paths
where applicable):

```sh
# readiness — returns the literal text "ready\n" with HTTP 200
curl -fsS http://localhost:3100/ready

# build info — returns JSON with version, revision, branch, build date, Go version
curl -fsS http://localhost:3100/loki/api/v1/status/buildinfo
```

### Step 5 — First push and query

Push one entry (Loki's push JSON shape):

```sh
curl -fsS -X POST http://localhost:3100/loki/api/v1/push \
  -H 'Content-Type: application/json' \
  -d '{
    "streams": [
      {
        "stream": {"app": "demo", "env": "dev"},
        "values": [["1700000000000000000", "hello from lokihouse"]]
      }
    ]
  }'
```

Query it back (instant LogQL query; the timestamp is in nanoseconds since
the epoch, as Loki expects):

```sh
curl -fsS 'http://localhost:3100/loki/api/v1/query?query=%7Bapp%3D%22demo%22%7D&limit=10'
```

(`%7B`/`%22`/`%7D` are URL-encoded `{`/`"`. The query is `{app="demo"`.)

Expected response shape (Loki's `loghttp` JSON, served unchanged):

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": {"app": "demo", "env": "dev"},
        "values": [["1700000000000000000", "hello from lokihouse"]]
      }
    ]
  }
}
```

### Upgrading

Rebuild the binary, stop the unit, replace the binary, restart:

```sh
sudo systemctl stop lokihouse
sudo install -m 755 -o root -g root bin/lokihouse /usr/local/bin/lokihouse
sudo systemctl start lokihouse
```

> **Note:** lokihouse is a release candidate. The ClickHouse schema may change
> between builds without a migration (the Phase 1 post-completion review
> changed the schema and required a database recreate). Until a stable
> release, back up the ClickHouse database before upgrading if your data
> matters.

### Uninstall

```sh
sudo systemctl disable --now lokihouse
sudo rm /etc/systemd/system/lokihouse.service
sudo systemctl daemon-reload
sudo rm /usr/local/bin/lokihouse
# drop the ClickHouse database (irreversible):
#   clickhouse client --query "DROP DATABASE lokihouse"
```

## ClickHouse cluster (optional)

For a sharded/replicated ClickHouse cluster (e.g. 2 shards × 2 replicas),
set these in the `clickhouse:` block:

```yaml
clickhouse:
  addr: ["ch1:9000", "ch2:9000", "ch3:9000", "ch4:9000"]
  database: lokihouse
  username: default
  cluster: lokihouse      # the remote_servers name in ClickHouse
  replicated: true        # Replicated*MergeTree; false = shard without replication
```

lokihouse creates `logs_local` / `streams_local` `Replicated*MergeTree`
tables `ON CLUSTER` and `Distributed` `logs` / `streams` tables over them,
sharded by `(tenant, stream_id)`. Inserts use `distributed_foreground_insert`
and (when replicated) `insert_quorum`, so a push is acknowledged only when
the rows are on a majority of replicas. Full cluster setup (Keeper,
`remote_servers`, replica paths) is a ClickHouse operations task; see the
[ClickHouse documentation](https://clickhouse.com/docs/en/enables/high-availability/overview)
and the [configuration reference](../reference/configuration.md#clickhouse)
for the lokihouse-side keys.
