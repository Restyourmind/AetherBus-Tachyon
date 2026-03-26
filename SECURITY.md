# Security Policy

## Supported Versions

We actively support the latest minor version of AetherBus-Tachyon.

| Version | Supported          |
| ------- | ------------------ |
| latest  | ✅ Yes             |
| older   | ⚠️ Best effort     |

---

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

- 📧 Email: security@your-domain.com
- 🔒 Or open a private security advisory on GitHub

Please DO NOT open public issues for security vulnerabilities.

---

## What to Include

Please include as much detail as possible:

- Description of the vulnerability
- Steps to reproduce
- Impact assessment (e.g. DoS, data leak, privilege escalation)
- Suggested fix (if any)

---

## Response Timeline

- Initial response: within 48 hours
- Triage: within 3–5 days
- Fix & disclosure: depends on severity

---

## Security Practices

This project follows:

- Defense-in-depth design
- Backpressure & rate limiting for traffic control
- Isolation between delivery classes (priority-aware scheduling)
- Regular dependency updates

---

## Disclosure Policy

- We follow responsible disclosure
- Credit will be given to reporters (unless anonymity requested)
- CVE may be assigned for critical issues

---

## Scope

In-scope:

- Core message bus / scheduling logic
- Delivery guarantees (retry, ordering, fairness)
- Resource exhaustion / DoS vectors

Out-of-scope:

- Misconfiguration by users
- Issues in third-party dependencies (report upstream)

---

Thank you for helping keep AetherBus-Tachyon secure 🙏
