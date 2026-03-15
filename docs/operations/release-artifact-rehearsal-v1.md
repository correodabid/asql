# ASQL Release Artifact Rehearsal v1

Date: 2026-03-15

## Purpose

This note explains how to rehearse the `release-artifacts` lane locally before a tagged release.

It is not a replacement for the CI release job.
It is a quick confidence check that:
- binaries still build for the intended targets,
- checksums can be generated,
- the release bundle shape is still coherent.

Use it together with:
- [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md)
- [release-artifact-gate-result-v1.0.0-rc1-template.md](release-artifact-gate-result-v1.0.0-rc1-template.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)
- [.github/workflows/ci.yml](../../.github/workflows/ci.yml)

## CI source of truth

The canonical release artifact lane is:
- [../../.github/workflows/ci.yml](../../.github/workflows/ci.yml)
- job: `release-artifacts`

That job is currently responsible for:
- building release binaries,
- generating checksums,
- generating the release SBOM,
- signing checksums with Sigstore/cosign,
- uploading the release bundle artifact,
- publishing GitHub release assets.

## Local rehearsal scope

A local rehearsal should at minimum verify:
- cross-target builds still succeed,
- checksums can be produced,
- output filenames match the expected release bundle shape.

If `syft` and `cosign` are installed locally, you can extend the rehearsal to SBOM and signature generation.

## Minimum rehearsal commands

Example:

```bash
rm -rf /tmp/asql-release-rehearsal && mkdir -p /tmp/asql-release-rehearsal
GOOS=linux GOARCH=amd64 go build -o /tmp/asql-release-rehearsal/asqld-linux-amd64 ./cmd/asqld
GOOS=linux GOARCH=amd64 go build -o /tmp/asql-release-rehearsal/asqlctl-linux-amd64 ./cmd/asqlctl
GOOS=darwin GOARCH=arm64 go build -o /tmp/asql-release-rehearsal/asqld-darwin-arm64 ./cmd/asqld
GOOS=darwin GOARCH=arm64 go build -o /tmp/asql-release-rehearsal/asqlctl-darwin-arm64 ./cmd/asqlctl
cd /tmp/asql-release-rehearsal
sha256sum asqld-linux-amd64 asqlctl-linux-amd64 asqld-darwin-arm64 asqlctl-darwin-arm64 > checksums.txt
```

## Optional local extensions

If available locally:

### SBOM

```bash
syft dir:. -o spdx-json > /tmp/asql-release-rehearsal/sbom.spdx.json
```

### Signature

```bash
cosign sign-blob --yes \
  --output-signature /tmp/asql-release-rehearsal/checksums.txt.sig \
  --output-certificate /tmp/asql-release-rehearsal/checksums.txt.pem \
  /tmp/asql-release-rehearsal/checksums.txt
```

## Current local rehearsal note

Current observed local tool availability in this work window:
- `sha256sum`: available
- `syft`: not available
- `cosign`: not available

That means local rehearsal can currently prove build + checksum shape, while SBOM/signature generation remains a CI-backed lane unless the tools are installed locally.

## What to record in the RC bundle

When a local rehearsal is used, record:
- output directory used,
- binaries produced,
- whether checksums were generated,
- whether SBOM/signatures were skipped locally and deferred to CI,
- the CI workflow/job that remains the source of truth.

## Bottom line

A local rehearsal is useful for catching packaging regressions early.
The final release artifact evidence should still come from the tagged CI `release-artifacts` job.

When that CI lane completes, record the final result in:
- [release-artifact-gate-result-v1.0.0-rc1-template.md](release-artifact-gate-result-v1.0.0-rc1-template.md)