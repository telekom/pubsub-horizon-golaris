<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# pubsub-horizon-golaris

Circuit breaker and event republishing service for the Horizon event streaming platform.

## Files

| File                | What                                    | When to read                                    |
| ------------------- | --------------------------------------- | ----------------------------------------------- |
| `main.go`           | Entry point, runs Cobra CLI             | Tracing startup flow                            |
| `go.mod`            | Go module dependencies                  | Adding or updating dependencies                 |
| `go.sum`            | Dependency checksums                    | Debugging dependency resolution                 |
| `Dockerfile`        | Multi-stage Docker build                | Modifying container build, base image           |
| `README.md`         | Public project docs, build/run guide    | Onboarding, understanding project purpose       |
| `CODEOWNERS`        | GitHub code ownership rules             | Modifying review requirements                   |
| `REUSE.toml`        | REUSE licensing metadata                | Updating license annotations                    |
| `CODE_OF_CONDUCT.md`| Contributor covenant                    | Never edit directly                             |

## Subdirectories

| Directory   | What                                          | When to read                                         |
| ----------- | --------------------------------------------- | ---------------------------------------------------- |
| `cmd/`      | Cobra CLI commands (root, serve, init)         | Adding CLI commands, modifying startup sequence       |
| `internal/` | All application logic (see internal/CLAUDE.md) | Any feature work, debugging, or testing               |
| `LICENSES/` | License files (Apache-2.0)                     | Never edit directly                                   |
| `.github/`  | Dependabot config, CI workflows                | Modifying CI pipelines, dependency update rules       |

## Build

```bash
go build
```

## Test

```bash
go test -tags testing ./...
```
