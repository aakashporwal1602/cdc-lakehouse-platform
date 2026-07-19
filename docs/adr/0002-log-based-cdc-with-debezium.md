# ADR-0002: Log-based CDC with Debezium

- **Status:** Accepted
- **Date:** 2026-01-06

## Context
We must replicate INSERT/UPDATE/DELETE from Postgres with correct ordering and
minimal source impact.

## Decision
Use Debezium logical decoding (`pgoutput`) with `REPLICA IDENTITY FULL` and a
scoped publication over the five commerce tables.

## Consequences
+ Captures deletes and full before-images; near-zero source load; exact ordering.
- Requires `wal_level=logical`; a stalled consumer holds the slot (must monitor).

## Alternatives
Query-based polling (misses deletes/intermediate states), triggers/outbox
(intrusive to app schema).
