# Security Policy

## Supported Versions

AetherBus-Tachyon is under active development. Security fixes are applied to the latest commit on the `main` branch.

| Version / Branch | Supported |
|---|---|
| `main` (latest) | ✅ |
| Older commits / forks | ❌ |

## Reporting a Vulnerability

Please report vulnerabilities privately and avoid opening a public issue for sensitive findings.

- Email: **security@aetherbus.dev**
- Subject: `AetherBus-Tachyon Security Report`
- Recommended details:
  - Affected component/file
  - Reproduction steps or proof-of-concept
  - Impact assessment (confidentiality / integrity / availability)
  - Suggested mitigation (if available)

## Response Targets

- Initial acknowledgement: within **72 hours**
- Triage and severity classification: within **7 business days**
- Fix or mitigation plan: as soon as validated, based on severity and operational risk

## Coordinated Disclosure

- Please allow maintainers reasonable time to validate and patch before public disclosure.
- After remediation, maintainers may publish a security advisory and credit the reporter (optional).

## Security Hardening Notes

When deploying AetherBus-Tachyon in production:

- Enable operator authentication for admin endpoints (`ADMIN_TOKEN`).
- Use encrypted transport tunnels or a private network boundary for ZeroMQ traffic.
- Persist and protect WAL, DLQ, and audit files with least-privilege filesystem permissions.
- Export append-only audit records to immutable retention storage where required.
- Monitor retry spikes, dead-letter growth, and consumer heartbeat anomalies.
