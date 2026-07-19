# Security Policy

## Reporting
Email security@your-org.example with details. Do not open public issues for
vulnerabilities. We aim to acknowledge within 2 business days.

## Practices in this repo
- Secrets are never committed; `.env` is git-ignored and `detect-private-key`
  runs in pre-commit.
- Kubernetes secrets are placeholders for SealedSecrets / External Secrets.
- CodeQL scans run on every push and weekly.
- Least-privilege Postgres replication role.
