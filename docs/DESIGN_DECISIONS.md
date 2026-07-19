# Design Decisions, Scalability, Bottlenecks & Tradeoffs

This document explains *why* the platform is built the way it is. Each decision
lists the choice, the alternatives considered, and the tradeoff accepted.

## 1. Why log-based CDC (Debezium) over query-based CDC

**Choice:** capture changes from the Postgres write-ahead log via logical
decoding (`pgoutput`).

**Alternatives:** periodic `SELECT ... WHERE updated_at > :watermark` polling, or
trigger-based capture into an outbox table.

**Why:** log-based CDC captures *every* change including deletes and intermediate
updates, imposes near-zero load on the source (it reads the WAL the DB already
writes), and preserves exact ordering via the LSN. Query-based polling misses
deletes and hard-deletes, misses intermediate states between polls, and adds
scan load. Trigger/outbox is reliable but intrusive to the application schema.

**Tradeoff:** requires `wal_level=logical`, a replication slot, and `REPLICA
IDENTITY FULL` (more WAL volume). A stalled consumer holds the slot and prevents
WAL cleanup — monitored via `cdc_end_to_end_lag_seconds` and slot-size alerts.

## 2. Why Kafka in the middle

Kafka decouples capture speed from processing speed, provides a replayable,
partitioned, durable buffer, and lets multiple independent consumers read the
same change stream. It absorbs source bursts and lets us reprocess history by
resetting offsets. The alternative — Debezium writing straight to the lake —
couples ingestion to transform availability and loses replay.

## 3. Why Iceberg over Delta / Hudi / raw Parquet

**Choice:** Apache Iceberg tables via a REST catalog.

**Why:** ACID snapshot isolation (atomic commits are the backbone of our
exactly-once story), hidden partitioning + partition evolution, native schema
evolution (add/rename/reorder without rewrites), engine-agnostic reads (Spark
writes, Trino reads the same tables), and time travel for debugging/backfills.
Raw Parquet has no atomic commit or schema evolution; Delta is excellent but more
tightly tied to the Spark/Databricks ecosystem; Hudi is strong for upserts but
operationally heavier. Iceberg's clean multi-engine story (Spark + Trino) fits a
polyglot stack best.

**Tradeoff:** streaming writes create many small files; mitigated by the nightly
`rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` maintenance DAG.

## 4. Why the Medallion architecture

- **Bronze** is an immutable, append-only replica of the change log. It is the
  system of record for *replay*: Silver and Gold can always be rebuilt from it.
- **Silver** collapses the log to current state per primary key. This is where
  dedup, ordering, and tombstoning live — the correctness core.
- **Gold** is business-shaped (facts/dims, marts) and optimized for BI.

Separating raw from conformed from curated means each concern is independently
testable, reprocessable, and ownable by a different team.

## 5. Exactly-once semantics — how it actually works

True end-to-end exactly-once across heterogeneous systems is impossible; we
achieve **effectively-once** (at-least-once delivery + idempotent apply):

1. Spark reads a bounded Kafka batch `[o_start, o_end)`.
2. It writes the result to Iceberg as a single **atomic snapshot commit**.
3. Only then does it commit Kafka offsets to the checkpoint.

If the job crashes between (2) and (3), the batch replays. Because the Silver
apply is an **idempotent, LSN-guarded `MERGE INTO`** (only apply if incoming LSN
> stored LSN), replaying produces the identical table state. Bronze appends are
made safe by de-duplicating on `(kafka_partition, kafka_offset)` when read into
Silver.

## 6. Deduplication, ordering & out-of-order handling

Within each micro-batch we rank rows per primary key by `(source_lsn,
source_ts_ms)` descending and keep rank 1. LSN is a monotonic source-side commit
order, so even if Kafka delivers records out of order (across partitions, or on
replay), the newest committed version always wins. The MERGE's `s.source_lsn >
t.source_lsn` guard extends this ordering guarantee *across* batches.

## 7. Schema evolution

Producer side: Schema Registry enforces `BACKWARD` compatibility so a bad
producer change is rejected before it reaches consumers. Consumer side: Bronze
decodes with the latest reader schema, and Iceberg evolves the table (new columns
appear, are back-filled null) without rewriting data. Renames/reorders are
metadata-only in Iceberg.

## 8. Configuration & clean architecture

Everything is config-driven: `configs/tables.yml` is the single source of truth
for primary keys, ordering, partitioning, and DQ suites, so **adding a replicated
table requires zero code changes**. The streaming engine uses the Template Method
pattern (`StreamingJob` base owns lifecycle; Bronze/Silver override
`transform`/`sink`). Settings flow through one typed `Settings` object
(Pydantic) — nothing reads `os.environ` directly. This is SOLID in practice:
SRP (one job = one table = one responsibility), OCP (extend via config), DIP
(validators/factories are injected, so unit tests need no live infra).

## 9. Scalability

| Hop | Scale lever | Notes |
|-----|-------------|-------|
| Postgres | read replica for snapshots; publication filtering | single logical slot is the ceiling — see bottlenecks |
| Kafka | partitions (hash by PK), brokers | ordering preserved *within* a PK by keying on PK |
| Connect | distributed workers (x2+), `tasks.max` | HA capture; rebalances on failure |
| Spark | executors, `maxOffsetsPerTrigger`, shuffle partitions | back-pressure via bounded batches |
| Iceberg | async compaction, partitioning | keeps file counts and metadata small |
| Trino | worker autoscaling (HPA) | scales with query concurrency |

Keying Kafka topics by primary key guarantees all changes to one row land on one
partition, preserving per-key order while scaling throughput horizontally.

## 10. Bottlenecks (and mitigations)

- **Single logical replication slot** — Postgres decodes the WAL single-threaded
  per slot. Mitigation: filter the publication to only replicated tables, use
  `pgoutput`, and at extreme scale shard into multiple slots/publications by
  table group. This is the hardest ceiling in any log-based CDC design.
- **Small-file explosion** in Iceberg from frequent micro-batches. Mitigation:
  nightly compaction DAG + `write.distribution-mode=hash`.
- **Connect DLQ growth** on poison messages. Mitigation: `errors.tolerance=all`
  with a dead-letter topic + alerting; never block the stream on one bad row.
- **Stateful shuffle** in dedup at high cardinality. Mitigation: partition-by-PK
  and bounded batch sizes; watermarks cap state.

## 11. Tradeoffs accepted

- **Micro-batch (seconds) vs. true streaming (sub-second):** we accept seconds of
  latency to get Iceberg's atomic commit and far simpler operations. If a use
  case needs sub-second, Flink + Iceberg is the escalation path (see §13).
- **Effectively-once vs. transactional exactly-once:** full XA across Kafka +
  Iceberg would be brittle and slow; idempotent MERGE gets the same *observable*
  result more robustly.
- **REST catalog vs. Hive metastore:** REST is simpler and cloud-native but is a
  newer component; we accept that in exchange for operational simplicity.

## 12. Failure modes & recovery

| Failure | Behaviour | Recovery |
|---------|-----------|----------|
| Spark job crash | offsets not committed | auto-restart replays batch; MERGE idempotent |
| Connect worker dies | partition rebalances | second worker takes over (HA) |
| Bad schema change | rejected by registry | producer fixes; no consumer impact |
| Poison message | routed to DLQ | inspect + reprocess from DLQ |
| Corrupt Silver | rebuild from Bronze | replay Bronze → Silver from snapshot 0 |
| Source failover | slot preserved on primary | resume from last LSN |

## 13. Future improvements

- **Flink** streaming for sub-second SLAs on the hottest tables.
- **Iceberg WAP (write-audit-publish) branching** so Gold is validated on a
  branch before being fast-forwarded to `main` — DQ gates block publish.
- **Automated backfill/replay tooling** with a UI over Kafka offset + Iceberg
  time-travel.
- **Data contracts as code** (dbt contracts + schema registry) enforced in CI.
- **Cross-region DR**: MirrorMaker 2 for Kafka, Iceberg metadata replication.
- **Cost governance**: partition/heat analysis to auto-tune compaction cadence.
- **PII tokenization** in Silver with column-level access policies in Trino.
