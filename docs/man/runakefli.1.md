% runakefli 1 "October 2026" "runakefli" "User Commands"

# NAME

runakefli - Loki®-compatible log store on ClickHouse®

# SYNOPSIS

**runakefli** [**-config.file** *path*] [**-config.expand-env**] [**-schema.migrate**] [**-version**]

# DESCRIPTION

**runakefli** serves Loki's HTTP API (push, LogQL queries, labels,
series, tail, index statistics, deletes and the ruler) and stores the logs in
ClickHouse. Clients such as Grafana®, Promtail, Alloy and logcli talk to it as
they would to Loki.

On start it creates the database and tables it needs, then listens on
*server.http_listen_address*:*server.http_listen_port* (default port 3100).

# OPTIONS

**-config.file** *path*
:   YAML configuration file (default *runakefli.yaml*). Unknown keys are an error.

**-config.expand-env**
:   Expand *${VAR}* and *${VAR:-default}* in the configuration file before
    parsing it, as Loki does.

**-schema.migrate**
:   Migrate the ClickHouse schema to the current version and exit (0 on success,
    1 on error). Needed once after upgrading from a release with schema
    version 1 (before 0.9) or 2 (before 0.11): stop every runakefli instance
    first. From version 1 the disks need free space for a copy of the largest
    table; from version 2 the migration only alters *logs* (no copy, no extra
    disk space) and does not re-index existing data. A normal start
    against a database of an older schema version exits 1 and names this option. A migration
    that was interrupted finishes on the next run; on a current database the
    option does nothing.
    A configuration that reads the password from a systemd credential needs
    *CREDENTIALS_DIRECTORY*, which only a unit provides: run
    *systemd-run --pty --wait --collect -p ImportCredential='runakefli.\*'
    /usr/bin/runakefli -config.file ... -config.expand-env -schema.migrate*.

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

*/etc/runakefli/runakefli.yaml*
:   Configuration used by the Debian package's *runakefli.service*. It lists
    every option with its default, commented out.

*/usr/share/doc/runakefli/examples/runakefli.example.yaml*
:   An unmodified copy of the shipped configuration.

*/var/lib/runakefli*
:   Working directory of the service: temporary rule files and the ruler WAL.

# ENVIRONMENT

**NOTIFY_SOCKET**
:   Set by systemd for *Type=notify* services; runakefli reports readiness and
    shutdown on it.

**CREDENTIALS_DIRECTORY**
:   Set by systemd when credentials are imported; reference it from the
    configuration with **-config.expand-env**, for example
    *password_file: ${CREDENTIALS_DIRECTORY}/runakefli.clickhouse-password*.

# SEE ALSO

Documentation: <https://runakefli.app/>; Loki configuration
reference for the *ruler*, *ruler_storage* and *limits_config* blocks:
<https://grafana.com/docs/loki/latest/configure/>.
