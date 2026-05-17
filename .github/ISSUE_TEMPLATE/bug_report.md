---
name: SDK bug report
about: Report a bug in github.com/sahina/ironflow-go
title: "[bug] "
labels: bug
---

<!--
This template is for bugs in the published Go SDK.

  - Engine/server bugs (HTTP API, workflow execution, etc.) → email the
    support address in LICENSE. The engine is closed source; the issue
    needs to be triaged in the private repo.
  - Security issues → see SECURITY.md. Do NOT open a public issue.
-->

## Module + version

- Import path: `github.com/sahina/ironflow-go/<ironflow|ironflow/agent|api/ironflow/v1>`
- Module version: `v0.x.y`
- Go version: `go1.x.y` (output of `go version`)
- OS / arch: `darwin/arm64 | linux/amd64 | ...`

## What happened

<!-- Steps to reproduce, expected vs actual behavior -->

## Minimal repro

<!-- Smallest code snippet that demonstrates the bug. Inline or link to a
gist / branch. Issues without a repro are hard to act on. -->

```go
```

## Engine version

<!-- If known, the Ironflow engine version your client is talking to.
Find via the dashboard or `ironflow version`. -->
