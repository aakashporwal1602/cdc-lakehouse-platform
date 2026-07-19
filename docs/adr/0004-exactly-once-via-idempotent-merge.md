# ADR-0004: Exactly-once via idempotent, LSN-guarded MERGE

- **Status:** Accepted
- **Date:** 2026-01-10

## Context
End-to-end transactional exactly-once across Kafka + Iceberg is impractical.

## Decision
Combine at-least-once Kafka delivery + Spark checkpointing with an idempotent
`MERGE INTO` guarded by `incoming.source_lsn > stored.source_lsn`. Commit Iceberg
snapshot before committing Kafka offsets.

## Consequences
+ Replays are safe; observable state is exactly-once; robust to crashes.
- Requires a monotonic ordering column (LSN) on every event.
