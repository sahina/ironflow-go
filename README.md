# Ironflow — Go SDK (Public Source Mirror)

This repository hosts the **public source** of the [Ironflow](https://ironflow.run) Go SDK.

[![Go Reference](https://pkg.go.dev/badge/github.com/sahina/ironflow-go.svg)](https://pkg.go.dev/github.com/sahina/ironflow-go)

## Packages

| Import path | Path |
|---|---|
| [`github.com/sahina/ironflow-go/ironflow`](https://pkg.go.dev/github.com/sahina/ironflow-go/ironflow) | [`ironflow/`](ironflow) |
| [`github.com/sahina/ironflow-go/ironflow/agent`](https://pkg.go.dev/github.com/sahina/ironflow-go/ironflow/agent) | [`ironflow/agent/`](ironflow/agent) |
| [`github.com/sahina/ironflow-go/api/ironflow/v1`](https://pkg.go.dev/github.com/sahina/ironflow-go/api/ironflow/v1) | [`api/ironflow/v1/`](api/ironflow/v1) |

All packages version in lockstep with the Ironflow engine release.

## Installing

```bash
go get github.com/sahina/ironflow-go/ironflow@latest
```

Requires Go 1.25+.

## What lives here

- `.go` source for the SDK package above
- Generated proto + Connect-RPC code (`api/ironflow/v1/`) vendored from the engine repo
- `go.mod`, `go.sum` regenerated at each release
- `LICENSE`, issue templates, security policy

## Where the engine source lives

The Ironflow engine is **closed source** and lives at `sahina/ironflow` (private). This mirror exists so that:

- `go get` and `pkg.go.dev` resolve against public source
- README source links (`/blob/main/...`) resolve to public source
- The Go module proxy can index and cache the module

## Building locally

```bash
go build ./...
go test ./...
```

Requires Go 1.25+.

## Read-only mirror

This repo is **read-only**. Pull requests will be closed without review. Source changes land in the engine repo and are synced here at each release.

## Bug reports

- SDK bugs (in `github.com/sahina/ironflow-go/...`) → [open an issue here](https://github.com/sahina/ironflow-go/issues/new/choose)
- Engine/server bugs → email the support address in [LICENSE](LICENSE)
- Security issues → see [SECURITY.md](SECURITY.md) — do **not** open a public issue

## Verifying release provenance

Each release tag on this repository carries an annotated message containing the engine-side commit SHA the snapshot was built from. Inspect with:

```bash
git fetch --tags
git tag -v v<version>      # signature (if available)
git for-each-ref --format='%(contents)' refs/tags/v<version>
```

This forensic trail correlates a mirror release to the private engine commit. The mirror's Git history is squash-snapshot per release (no engine commit messages leak through).

## License

See [LICENSE](LICENSE) — SPDX: `LicenseRef-Ironflow-EULA`.
