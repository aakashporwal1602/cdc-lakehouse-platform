# ADR-0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-01-05

## Context
We need a durable, reviewable record of significant architectural choices so new
engineers can understand *why*, not just *what*.

## Decision
Use lightweight ADRs (Michael Nygard format) stored in `docs/adr/`, one file per
decision, immutable once accepted (superseded rather than edited).

## Consequences
Every architecturally significant change ships with an ADR in the same PR.
