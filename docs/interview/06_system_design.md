# System design & operations

**Q: Design a CDC pipeline for 10k writes/sec across 200 tables.**
A: Log-based CDC with publications sharded by table group across multiple slots;
Kafka topics keyed by PK with partitions sized to throughput; distributed Connect
for HA; Spark Structured Streaming with bounded batches per table (or a generic
multiplexed job driven by the table registry); Iceberg MoR + aggressive
compaction; Trino for serving. Watch the single-slot ceiling and small files.

**Q: What are your SLAs and how do you measure them?**
A: End-to-end lag (source commit → lakehouse commit) exported as
`cdc_end_to_end_lag_seconds`, alerting at >5 min; batch duration p95; DQ pass
rate; DLQ size. All in Grafana off Prometheus.

**Q: How do you backfill / reprocess?**
A: Reset the Silver stream checkpoint and replay Bronze from snapshot 0 (or a
timestamp) through the idempotent MERGE — no source impact. For a schema/model
change, rebuild Gold via dbt from Silver.

**Q: How do you handle PII / governance?**
A: Tokenize/hash PII in Silver, apply column-level access control in Trino,
retain raw PII in Bronze under stricter access, and honor deletes end-to-end
(the DELETE tombstone removes the row from Silver/Gold).

**Q: How do you deploy safely?**
A: CI runs lint/type/unit/integration + docker build; images to GHCR; Kustomize
overlays per environment; roll out streaming jobs one table at a time; validate
Gold on an Iceberg branch (WAP) before publishing.

**Q: Where are the bottlenecks and single points of failure?**
A: The Postgres logical slot (single-threaded, holds WAL) is the main ceiling and
SPOF for capture; mitigated by monitoring and slot sharding. Everything
downstream (Kafka, Connect, Spark, Trino) scales horizontally and is HA.
