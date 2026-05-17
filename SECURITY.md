# Security Policy

## Reporting vulnerabilities

Report suspected security vulnerabilities by emailing the address listed in the [LICENSE](LICENSE) file with subject line `[SECURITY] ironflow-go — <one-line summary>`. Include:

- Affected import path(s) and module version (e.g., `github.com/sahina/ironflow-go/ironflow@v0.22.4`).
- Reproduction steps or proof-of-concept.
- Impact assessment (auth bypass, RCE, info disclosure, etc.).
- Disclosure timeline expectations.

We acknowledge receipt within 3 business days and aim to provide a triage status within 7. Public disclosure happens after a fix ships, with a CVE issued if applicable.

**Please do not file public GitHub issues for security matters.**

## Verifying release provenance

Each release tag on this repository carries an annotated message containing the engine-side commit SHA the snapshot was built from:

```bash
git fetch --tags
git for-each-ref --format='%(contents)' refs/tags/v<version>
```

This correlates a mirror release to the private engine commit and is the forensic trail used during incident response.

## Supported versions

The latest minor release receives security patches. Older minors are supported on a best-effort basis until the next major release.
