# Security policy

lokihouse is a release candidate; only the latest release and `main` are
supported.

## Reporting a vulnerability

Please do not open a public issue. Report it privately, either

- through GitHub: <https://github.com/bzed/lokihouse/security/advisories/new>, or
- by e-mail to bernd@bzed.de.

Include the version (`lokihouse -version`), the configuration keys involved
(without secrets) and a way to reproduce it. You will get an answer within a
week. Fixes are released as soon as they are ready and credited in
[`CHANGELOG.md`](CHANGELOG.md) unless you prefer otherwise.

## Scope

In scope: the lokihouse binary, its HTTP API, the Debian package, the container
image and the apt repository. Vulnerabilities in Grafana Loki or ClickHouse
themselves belong to those projects; if lokihouse reuses the affected Loki code
at its pinned commit (`LOKI_COMMIT` in the `Makefile`), tell us as well.

## Verifying downloads

- The apt repository is signed; the key fingerprint is in the
  [installation guide](docs/install/index.md).
- Release `.deb` files come with `SHA256SUMS`.
- Container images are signed with cosign (keyless, GitHub OIDC) and carry an
  SBOM and provenance attestation:

  ```console
  $ cosign verify ghcr.io/bzed/lokihouse:<version> \
      --certificate-identity-regexp '^https://github.com/bzed/lokihouse/\.github/workflows/' \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```
