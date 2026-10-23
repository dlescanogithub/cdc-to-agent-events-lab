# Security

This repository is a synthetic lab. It ships no credentials, no customer data, and no live model calls.

## Reporting

Report security issues through GitHub private vulnerability reporting: on the Security tab, choose "Report a vulnerability". You can also contact the owner listed in `.github/CODEOWNERS`. Do not file a public issue for a suspected credential leak.

## What is in scope

- Accidental secret material in the tree. `python -m cdc_lab.secretscan` scans the worktree.
- Logs and traces must omit direct identifiers (email, phone, name, address) and redact secret-shaped substrings.
- The agent path drafts text and refuses to send it until a named approver marks the draft approved.

## What is out of scope

- The optional `docker-compose.yml` password `local-only` is a placeholder for a fictional database on localhost. It is not a production credential.
- The in-memory broker and SQLite file are process-local. They are not a security boundary.
- No vulnerability reward or production deployment is attached to this lab.
