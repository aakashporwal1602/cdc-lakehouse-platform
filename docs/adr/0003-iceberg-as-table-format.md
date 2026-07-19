# ADR-0003: Apache Iceberg as the table format

- **Status:** Accepted
- **Date:** 2026-01-08

## Context
The lakehouse needs ACID commits (for exactly-once), schema evolution, and
multi-engine access (Spark writes, Trino reads).

## Decision
Adopt Apache Iceberg (format v2, merge-on-read) via a REST catalog on S3/MinIO.

## Consequences
+ Atomic snapshot commits, hidden partitioning, native schema evolution, time
  travel, engine-agnostic reads.
- Streaming writes create small files → nightly compaction/expiry DAG required.

## Alternatives
Delta Lake (Spark-centric), Hudi (heavier ops), raw Parquet (no ACID/evolution).
