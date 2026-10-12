% lokihouse 1 "October 2026" "lokihouse" "User Commands"

# NAME

lokihouse - Loki-compatible log store on ClickHouse

# SYNOPSIS

**lokihouse** [**-config.file** *path*] [**-config.expand-env**] [**-version**]

# DESCRIPTION

**lokihouse** serves Grafana Loki's HTTP API (push, LogQL queries, labels,
series, tail, index statistics, deletes and the ruler) and stores the logs in
ClickHouse. Clients such as Grafana, Promtail, Alloy and logcli talk to it as
they would to Loki.

On start it creates the database and tables it needs, then listens on
*server.http_listen_address*:*server.http_listen_port* (default port 3100).

# OPTIONS

**-config.file** *path*
:   YAML configuration file (default *lokihouse.yaml*). Unknown keys are an error.

**-config.expand-env**
:   Expand *${VAR}* and *${VAR:-default}* in the configuration file before
    parsing it, as Loki does.

**-version**
:   Print version, revision, branch, build date and Go version, then exit.

# SIGNALS

**SIGTERM**, **SIGINT**
:   Stop accepting connections, let in-flight requests finish (at most
    *server.graceful_shutdown_timeout*, default 30s), stop the ruler and the
    background loops, close the ClickHouse connection and exit 0. A second
    signal terminates immediately.

# EXIT STATUS

**0** clean shutdown; **1** configuration or runtime error; **2** invalid
command line.

# FILES

*/etc/lokihouse/lokihouse.yaml*
:   Configuration used by the Debian package's *lokihouse.service*.

*/var/lib/lokihouse*
:   Working directory of the service: temporary rule files and the ruler WAL.

# ENVIRONMENT

**NOTIFY_SOCKET**
:   Set by systemd for *Type=notify* services; lokihouse reports readiness and
    shutdown on it.

**CREDENTIALS_DIRECTORY**
:   Set by systemd when credentials are imported; reference it from the
    configuration with **-config.expand-env**, for example
    *password_file: ${CREDENTIALS_DIRECTORY}/lokihouse.clickhouse-password*.

# SEE ALSO

Documentation: <https://bzed.github.io/lokihouse/>; Loki configuration
reference for the *ruler*, *ruler_storage* and *limits_config* blocks:
<https://grafana.com/docs/loki/latest/configure/>.
