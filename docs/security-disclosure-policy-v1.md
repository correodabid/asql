# ASQL Security Vulnerability Disclosure Policy (v1)

Date: 2026-03-01
Applies to: ASQL source code, release artifacts, and official containers.

## 1) Purpose

This policy defines how security vulnerabilities are reported, triaged, fixed, and disclosed in a coordinated and responsible way.

## 2) Reporting a vulnerability

Please report suspected vulnerabilities through a private channel:
- Email: **security@asql.dev**
- Subject: `ASQL Security Report: <short title>`

Please avoid opening public issues for unpatched vulnerabilities.

Recommended report contents:
- affected version(s) and deployment mode;
- vulnerability category and impact;
- proof of concept or reproduction steps;
- potential mitigations/workarounds.

## 3) Coordinated disclosure timeline targets

Target handling objectives:
- Acknowledge report within **3 business days**.
- Initial triage/severity assessment within **7 business days**.
- Provide remediation plan and affected-version scope as soon as validated.

For critical vulnerabilities with active exploitation risk, ASQL may use accelerated patch and disclosure timelines.

## 4) Severity model

ASQL uses a practical severity model aligned with CVSS-informed triage:
- **Critical**: remote compromise, data integrity loss, or auth bypass with severe impact.
- **High**: significant confidentiality/integrity/availability impact.
- **Medium**: constrained exploitability or limited impact.
- **Low**: minor impact or hard-to-exploit findings.

## 5) Remediation and advisory process

When a report is confirmed:
1. Create internal tracked security issue.
2. Validate impacted versions and mitigations.
3. Prepare and test patch release.
4. Publish security advisory with:
   - impact summary,
   - fixed versions,
   - upgrade guidance,
   - temporary mitigations when applicable.

## 6) Supported versions for security fixes

Security fixes are provided for:
- latest GA minor version;
- previous GA minor version when feasible and risk-appropriate.

Older versions may require upgrade before remediation is available.

## 7) Safe harbor for good-faith research

ASQL supports good-faith security research. If you:
- act in good faith,
- avoid privacy violations and service disruption,
- do not exfiltrate or retain data beyond what is required for proof,
- and report findings privately,

ASQL will not pursue legal action for your research activities within these boundaries.

## 8) Public disclosure

Public disclosure should occur after:
- a fix is available, or
- maintainers and reporter agree on a coordinated date.

If users are exposed to active risk, ASQL may publish interim mitigation guidance before final patch details.

## 9) Out-of-scope reports

The following are typically out of scope unless they demonstrate concrete security impact:
- theoretical findings without exploit path;
- denial-of-service requiring unrealistic resources;
- vulnerabilities in unsupported third-party infrastructure.

## 10) Credits

With reporter consent, validated reports may be acknowledged in security advisories.
