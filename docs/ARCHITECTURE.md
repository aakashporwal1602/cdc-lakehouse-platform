# Architecture

All diagrams below render natively on GitHub (Mermaid). Source files live in
[`docs/diagrams/`](diagrams/).

## System architecture

```mermaid
flowchart LR
    subgraph Source["OLTP Source"]
        PG[(PostgreSQL)]
    end
    subgraph Capture["Change Capture"]
        DBZ[Debezium / Connect]
        SR[Schema Registry]
    end
    subgraph Bus["Streaming Bus"]
        K[(Kafka)]
        DLQ[(DLQ)]
    end
    subgraph Compute["Stream Processing"]
        SB[Spark Bronze] --> SS[Spark Silver] --> SG[Gold dbt/Spark]
    end
    subgraph Lake["Iceberg on S3/MinIO"]
        BR[(bronze)] --> SI[(silver)] --> GO[(gold)]
    end
    subgraph Serve["Serving"]
        TR[Trino] --> SUP[Superset]
    end
    PG -->|WAL| DBZ
    DBZ <--> SR
    DBZ --> K
    DBZ -.errors.-> DLQ
    K --> SB
    SB --> BR
    SS --> SI
    SG --> GO
    GO --> TR
    SI --> TR
```

## CDC flow (INSERT / UPDATE / DELETE)

```mermaid
flowchart TD
    A[Row mutated] --> B[WAL entry]
    B --> C[Debezium reads slot]
    C --> D{op}
    D -- c/r --> E[after image]
    D -- u --> F[before + after]
    D -- d --> G[before + tombstone]
    E --> H[Avro + registry] --> I[(Kafka)]
    F --> H
    G --> H
    I --> J[Bronze append] --> K[Silver dedup by PK, order by LSN]
    K --> L{delete?}
    L -- yes --> M[MERGE DELETE]
    L -- no --> N[MERGE upsert if LSN newer]
    M --> O[(silver current state)]
    N --> O
```

## Exactly-once sequence

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant S as Spark
    participant CP as Checkpoint
    participant IB as Iceberg
    S->>CP: read committed offsets
    S->>K: fetch batch
    K-->>S: change events
    S->>S: dedup + order by LSN
    S->>IB: MERGE (idempotent, LSN-guarded)
    IB-->>S: snapshot committed
    S->>CP: commit offsets
    Note over S,IB: crash before offset commit → safe replay
```

## Medallion

```mermaid
flowchart LR
    B[🥉 Bronze raw/immutable] --> S[🥈 Silver dedup/current] --> G[🥇 Gold marts]
```
