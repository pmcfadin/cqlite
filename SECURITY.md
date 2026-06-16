# Security Policy

## Supported Versions

CQLite is pre-1.0 and under active development. Security fixes are applied to the
latest released minor version. We recommend always running the most recent
release.

| Version | Supported          |
| ------- | ------------------ |
| 0.11.x  | :white_check_mark: |
| < 0.11  | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Instead, use one of the private channels below:

1. **GitHub Security Advisories (preferred)** — open a private report at
   <https://github.com/pmcfadin/cqlite/security/advisories/new>. This keeps the
   discussion private until a fix is released.
2. **Email** — send details to **pmcfadin@gmail.com** with `[CQLite Security]` in
   the subject line.

Please include as much of the following as you can:

- The component affected (e.g. SSTable parser, write engine, a language binding).
- A description of the issue and its potential impact.
- Steps to reproduce, ideally with a minimal SSTable fixture or input.
- Any known mitigations.

## What to Expect

- **Acknowledgement** of your report within 5 business days.
- An assessment and, where confirmed, a plan and timeline for a fix.
- Credit for the discovery once a fix is released, unless you prefer to remain
  anonymous.

## Scope

CQLite reads and writes untrusted, on-disk Cassandra SSTable files. Reports of
particular interest include memory-safety issues, panics or crashes on malformed
input, and resource-exhaustion (denial-of-service) conditions when parsing
hostile files. Thank you for helping keep CQLite and its users safe.
