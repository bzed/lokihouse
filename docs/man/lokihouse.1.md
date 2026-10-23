% lokihouse 1 "October 2026" "lokihouse" "User Commands"

# NAME

lokihouse - Loki-compatible log store on ClickHouse

# SYNOPSIS

**lokihouse** [**-config.file** *path*] [**-config.expand-env**] [**-schema.migrate**] [**-version**]

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

**-schema.migrate**
:   Migrate the ClickHouse schema to the current version and exit (0 on success,
    1 on error). Needed once after upgrading from a release with schema
    version 1 (before 0.9): stop every lokihouse instance first and make sure
    the disks have free space for a copy of the largest table. A normal start
    against a version 1 database exits 1 and names this option. A migration
    that was interrupted finishes on the next run; on a current database the
    option does nothing.
    A configuration that reads the password from a systemd credential needs
    *CREDENTIALS_DIRECTORY*, which only a unit provides: run
    *systemd-run --pty --wait --collect -p ImportCredential='lokihouse.\*'
    /usr/bin/lokihouse -config.file ... -config.expand-env -schema.migrate*.

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
:   Configuration used by the Debian package's *lokihouse.service*. It lists
    every option with its default, commented out.

*/usr/share/doc/lokihouse/examples/lokihouse.example.yaml*
:   An unmodified copy of the shipped configuration.

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
