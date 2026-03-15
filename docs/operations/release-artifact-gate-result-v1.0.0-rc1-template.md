# ASQL Release Artifact Gate Result — v1.0.0-rc1 Template

Status: template
Use for: recording the final CI-backed result of the `release-artifacts` lane for the first release candidate.

Use together with:
- [release-artifact-rehearsal-v1.md](release-artifact-rehearsal-v1.md)
- [release-evidence-bundle-v1.0.0-rc1-working-draft.md](release-evidence-bundle-v1.0.0-rc1-working-draft.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)
- [../../.github/workflows/ci.yml](../../.github/workflows/ci.yml)

---

Release/tag: `v1.0.0-rc1`
Date: `<yyyy-mm-dd>`
Commit: `<sha>`
Workflow run URL: `<url>`
Decision: `green | blocked`

## 1. CI lane summary

Workflow:
- `.github/workflows/ci.yml`
- job: `release-artifacts`

Prerequisites satisfied:
- `test`: `yes | no`
- `race`: `yes | no`
- `security`: `yes | no`

Job result:
- `release-artifacts`: `pass | fail`

Notes:
- `<summary>`

## 2. Built binaries

Expected outputs:
- `asqld-linux-amd64`: `yes | no`
- `asqlctl-linux-amd64`: `yes | no`
- `asqld-darwin-arm64`: `yes | no`
- `asqlctl-darwin-arm64`: `yes | no`

Notes:
- `<summary>`

## 3. Checksums

- `checksums.txt` generated: `yes | no`
- checksum file included in uploaded/published assets: `yes | no`

Notes:
- `<summary>`

## 4. SBOM

- `dist/sbom.spdx.json` generated: `yes | no`
- SBOM included in uploaded/published assets: `yes | no`

Notes:
- `<summary>`

## 5. Signatures

- `dist/checksums.txt.sig` generated: `yes | no`
- `dist/checksums.txt.pem` generated: `yes | no`
- signature outputs included in uploaded/published assets: `yes | no`

Notes:
- `<summary>`

## 6. Uploaded bundle

- `release-bundle` artifact uploaded: `yes | no`
- artifact contents reviewed: `yes | no`

Notes:
- `<summary>`

## 7. Published release assets

- GitHub release assets published: `yes | no`
- release notes attached/updated: `yes | no`
- links to docs/getting-started/compatibility included: `yes | no`

Notes:
- `<summary>`

## 8. Final artifact gate decision

Result: `green | blocked`

Blockers:
- `<none or list>`

Follow-up:
- `<none or list>`
