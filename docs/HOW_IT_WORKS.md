# How It Works — The Complete Story (explained simply)

This document explains the **entire** CDC Lakehouse Platform from scratch, in
plain language, with analogies — and then answers every *why* and *how* behind
each piece. If you read this top to bottom, you will understand not just *what*
the project does, but *why every part exists*.

---

## Part 0 — The one-sentence idea

> Whenever something changes in our main app database, we want a **copy of that
> change** to instantly flow into a big "analytics warehouse" so people can build
> dashboards and ask questions — **without ever slowing down or touching the main
> app database.**

That's it. Everything else is just the machinery to do this **reliably, in near
real-time, and without ever losing or duplicating a single change.**

---

## Part 1 — A story you already understand

Imagine a **busy toy shop**.

- There's a **notebook** at the counter where the shopkeeper writes every sale,
  every new toy added, every price change. This notebook is the **main database
  (Postgres)**. It must always be fast, because customers are waiting.

- The shop owner also wants to know things like *"Which toys sell best?"*,
  *"How much money did we make today?"*, *"Which shelves are running empty?"*.
  But if the owner keeps grabbing the counter notebook to do maths, the line at
  the counter stops moving. **Bad idea.**

- So instead, we hire a **team of helpers**. Every time the shopkeeper writes
  something in the notebook, a helper **quietly copies that exact change** onto a
  conveyor belt. Other helpers pick it up, clean it, organise it, and put neat
  summaries on a **big whiteboard** the owner can look at anytime.

- The counter notebook is **never disturbed.** The owner gets fresh answers.
  Everybody's happy.

**This project is exactly that team of helpers — but for computer data.** Now
let's meet each helper.

---

## Part 2 — Meet the helpers (every tool, why it exists, how it works)

### 🗒️ Postgres — the counter notebook (the source database)

- **What it is:** the app's main database. Our example has 5 tables: `customers`,
  `products`, `inventory`, `orders`, `payments`.
- **Why it's here:** it's the *source of truth* for live app data. It's built to
  be fast for the app, not for heavy analytics.
- **How we use it:** we turn on a special mode called **logical replication**.
  Think of it as the notebook automatically keeping a **carbon copy** of every
  line as it's written. That carbon copy is called the **WAL** (Write-Ahead Log)
  — a perfect, ordered list of *every* change (inserts, updates, deletes).

> **Why not just re-read the whole notebook every hour?** Because that's slow,
> misses deletes, and misses quick changes that happen between reads. Reading the
> WAL catches **every single change, in exact order, with almost no extra work.**

### 🕵️ Debezium — the helper who copies changes off the notebook

- **What it is:** a "change reader" that plugs into Postgres and reads the WAL.
- **Why it's here:** it turns raw database changes into tidy **messages** like:
  *"Order #55 changed: status went from 'pending' to 'shipped', at time T,
  position 12345."*
- **How it works:** it watches the WAL and, for every change, produces a little
  envelope with:
  - `op` — what happened: **c** = created (insert), **u** = updated, **d** =
    deleted, **r** = read (first-time snapshot).
  - `before` — what the row looked like before (for updates/deletes).
  - `after` — what it looks like now (for inserts/updates).
  - `source.lsn` — a **position number** in the log (super important later!).
  - `ts_ms` — when it happened.

> **This is what "CDC" means** — Change Data Capture. We *capture* every *change*
> to the *data*.

### 📬 Kafka — the conveyor belt

- **What it is:** a super-reliable, super-fast **message conveyor belt**.
- **Why it's here:** Debezium produces changes very fast. The helpers who clean
  and organise data might be slower, or might briefly go on a break. Kafka sits
  in the middle so nobody has to wait for anybody. It **remembers** every message
  for a while, so a helper can catch up later or even **re-read old messages**.
- **How it works:** messages are grouped into **topics** (one per table, e.g.
  `commerce.public.orders`). Each topic is split into **partitions** (parallel
  belts) so many helpers can work at once. Messages keep their order *within* a
  partition.

> **Why a belt in the middle at all?** Decoupling. If we removed Kafka and wired
> Debezium straight to the cleaners, then whenever a cleaner crashed, the whole
> pipeline would jam — and we could never "replay" history. Kafka gives us a
> **buffer + a rewind button.**

### 🧾 Schema Registry — the rulebook for message shapes

- **What it is:** a small service that stores the **exact shape** (schema) of the
  messages, using a compact format called **Avro**.
- **Why it's here:** so the sender and the readers always agree on what a message
  looks like — and so we can **change the shape safely later** (e.g. add a new
  column) without breaking anyone.
- **How it works:** every message carries a tiny id pointing to its schema in the
  registry. Readers look up the id to understand the bytes.

### ⚙️ Spark Structured Streaming — the cleaning & organising crew

- **What it is:** the powerful engine that reads messages off Kafka and processes
  them in small batches (every ~30 seconds). This is where the real work happens.
- **Why it's here:** raw change messages are messy and full of duplicates and
  out-of-order arrivals. Spark turns them into clean, correct tables.
- **How it works:** it runs two jobs:
  - **Bronze job** — takes each raw message and just **saves it as-is** (an
    immutable diary of everything that ever happened).
  - **Silver job** — takes the messages, figures out the **latest truth** for each
    row, and keeps only that (the clean, current picture).

We'll go deep on the tricky bits (duplicates, ordering, deletes) in Part 4.

### 🧊 Apache Iceberg — the big organised filing cabinet (the "lakehouse")

- **What it is:** the storage format for our analytics tables. Data physically
  lives as files in object storage (MinIO here; S3/GCS in the cloud), but Iceberg
  adds a **smart catalog** on top so it behaves like real database tables.
- **Why it's here:** plain files can't do "update this one row" or "undo the last
  change" safely. Iceberg gives us **ACID transactions** (all-or-nothing saves),
  **schema evolution** (add columns without rewriting everything), and **time
  travel** (look at the table as it was yesterday).
- **How it works:** every save creates a new immutable **snapshot**. Readers
  always see a complete, consistent version — never a half-written mess.

> **Why not just use Postgres for analytics too?** Because analytics queries
> ("sum of all sales this year") would hammer the app database and slow down
> customers. Iceberg is built for huge scans and cheap storage; Postgres is built
> for fast small app queries. Different jobs, different tools.

### 🗄️ MinIO — the warehouse building where files are stored

- **What it is:** an S3-compatible object store running locally.
- **Why it's here:** Iceberg needs somewhere cheap and huge to put its files.
  MinIO plays the role of Amazon S3 on your laptop.

### 🔍 Trino — the librarian who answers questions in plain SQL

- **What it is:** a fast query engine that reads the Iceberg tables using normal
  SQL (`SELECT ... FROM ...`).
- **Why it's here:** dashboards and analysts speak SQL. Trino lets them query the
  lakehouse **without needing Spark**, and it's optimised for fast reads.
- **How it works:** you ask a question in SQL; Trino figures out which Iceberg
  files to read, reads only what's needed, and returns the answer.

### 📊 Superset — the whiteboard with charts

- **What it is:** the dashboard tool business people actually look at.
- **Why it's here:** numbers in a table are boring; charts tell a story. Superset
  connects to Trino and draws bar charts, line charts, big-number tiles, etc.

### 🧑‍✈️ Airflow — the shift manager

- **What it is:** a scheduler that runs jobs on a timetable and in the right order.
- **Why it's here:** some work isn't streaming — it's periodic. "Rebuild the Gold
  summaries every 30 minutes." "Clean up old files every night at 3 AM." Airflow
  makes sure these happen automatically, retries them if they fail, and shows a
  history.

### 🧱 dbt — the recipe book for summaries

- **What it is:** a tool for writing the **Gold** business summaries as simple SQL
  "models" that build on each other.
- **Why it's here:** it makes the business logic readable, testable, and
  documented — instead of hiding it in giant scripts.

### ✅ Great Expectations — the quality inspector

- **What it is:** a data-quality checker.
- **Why it's here:** to catch bad data before it reaches dashboards. It asserts
  rules like *"order_id must never be empty"*, *"amount must be ≥ 0"*, *"status
  must be one of these 5 values."* If a rule breaks, it raises an alarm and can
  block the pipeline.

### 📈 Prometheus + Grafana — the health monitors

- **Prometheus** collects numbers about the pipeline (events/second, how long
  each batch took, how far behind we are).
- **Grafana** draws those numbers as live dashboards so you can *see* the pipeline
  breathing and get alerted if something's wrong.
- **Why they're here:** in production you must **know** if data stops flowing —
  before your boss does.

---

## Part 3 — The life story of one change (end-to-end flow)

Let's follow **one single change** from start to finish. Say a customer's order
#1 goes from `pending` to `delivered`.

1. **App updates Postgres.** `UPDATE orders SET status='delivered' WHERE
   order_id=1;`. Postgres writes this to its WAL (the carbon copy).

2. **Debezium notices** the new WAL entry. It builds a message:
   `{ op: "u", before: {status:"pending"...}, after: {status:"delivered"...},
   source: { lsn: 12345, ts_ms: ... } }` and encodes it as Avro (using the
   Schema Registry).

3. **Debezium drops the message on Kafka**, on the topic
   `commerce.public.orders`, on the partition decided by the order's key. Kafka
   stores it durably and hands out its position (offset).

4. **The Bronze Spark job** (reading that topic) picks up the message in its next
   30-second batch. It decodes the Avro, and **appends the whole envelope** to the
   Iceberg table `bronze.orders`. Nothing is changed or removed — Bronze is a
   pure diary. It records Kafka coordinates too (partition/offset) for lineage.

5. **The Silver Spark job** (also reading that topic) picks up the same message.
   It:
   - **flattens** the envelope to a plain row (takes the `after` image),
   - lifts out the `lsn` (log position) and timestamp for ordering,
   - **de-duplicates**: if this batch has several changes to order #1, it keeps
     only the newest one (highest `lsn`),
   - runs a **MERGE** into `silver.orders`: "if order #1 exists, update it;
     otherwise insert it" — but only if the incoming change is newer.
   Result: `silver.orders` now shows order #1 as `delivered`, **exactly once, no
   duplicates.**

6. **The Gold job / dbt** later reads `silver.orders` and rebuilds business
   summaries like `order_metrics` (orders and revenue grouped by status/day).

7. **Trino** can now answer `SELECT status FROM silver.orders WHERE order_id=1`
   → `delivered`. And `SELECT * FROM gold.order_metrics` shows updated totals.

8. **Superset** refreshes its charts from Trino. **Grafana** shows the event
   flowing through as a blip on the "events/sec" graph.

Total time from step 1 to step 7: **a few seconds.** And the app database (step 1)
never felt a thing.

---

## Part 4 — The four hard problems (and how we beat them)

Copying data sounds easy. Doing it **correctly** is where the real engineering is.
Here are the four classic problems and how this project solves each.

### Problem 1: Duplicates 🪞
**Why it happens:** networks and crashes mean a message can be delivered **more
than once**. If we blindly applied every message, order #1 might get inserted
twice.

**How we solve it:** Silver uses an **idempotent MERGE** keyed on the **primary
key**. "Idempotent" means *doing it twice gives the same result as doing it once.*
MERGE says "update if exists, else insert" — so re-applying the same change just
overwrites with the same values. No duplicate rows, ever.

### Problem 2: Out-of-order arrivals ⏱️
**Why it happens:** with parallel belts (partitions) and retries, an **older**
change can sometimes arrive **after** a newer one. If we just took "the last one
we saw", we might overwrite `delivered` with a stale `pending`.

**How we solve it:** every change carries the **LSN** — the exact position in the
source log, which only ever goes **up**. We order by LSN, not by arrival time.
Within a batch we keep the row with the highest LSN, and the MERGE only updates if
the incoming LSN is **newer** than what's stored. So a late, older change can
never win. **Truth is decided by source order, not delivery luck.**

### Problem 3: Deletes / tombstones 🪦
**Why it happens:** if someone deletes a row in Postgres, a naive copy would still
show it in analytics forever.

**How we solve it:** Debezium sends a delete event (`op: "d"`) carrying the
`before` image (so we know *which* row). Silver's MERGE has a rule: *"when the
incoming change is a delete, remove that row from the table."* So deletes flow all
the way through and the analytics stays honest.

### Problem 4: Exactly-once ⚖️ (the crown jewel)
**Why it's hard:** across separate systems (Kafka + Iceberg), guaranteeing every
change lands **once and only once** — even through crashes — is genuinely
difficult. Perfect "transactional exactly-once" across different systems is
basically impossible.

**How we solve it (the clever trick):** we combine two things —
1. **At-least-once delivery** (Kafka + Spark checkpoints guarantee we never *lose*
   a message; worst case we re-read some), plus
2. **Idempotent apply** (the LSN-guarded MERGE means re-applying a message changes
   nothing).

The order of operations each batch is: read a bounded chunk from Kafka → write to
Iceberg as **one atomic snapshot** → **only then** save the Kafka position in the
checkpoint. If the job crashes in the middle, on restart it simply **re-reads that
chunk** — and because the MERGE is idempotent, the final table is **identical**.
The observable result is **effectively exactly-once.**

> This single idea — *at-least-once + idempotent = effectively exactly-once* — is
> the most important sentence in the whole project. Memorise it.

### Bonus problem: Schema changes 🔧
**Why it happens:** apps evolve. Someone adds a `discount_code` column to
`orders`. A rigid pipeline would break.

**How we solve it:** the **Schema Registry** enforces that changes are *backward
compatible* (old readers still work), and **Iceberg** can add the new column to
the table **without rewriting old data**. Old rows just show `null` for the new
column. Nothing breaks. This is called **schema evolution.**

---

## Part 5 — Why the "Medallion" (Bronze → Silver → Gold)?

We don't dump everything into one messy table. We use **three layers**, like
refining ore into jewellery:

- 🥉 **Bronze — raw & immutable.** Every change, exactly as it arrived, appended
  forever. *Why keep raw junk?* Because it's the **safety net**: if we ever find a
  bug in our cleaning logic, we can **replay Bronze** and rebuild everything
  correctly — without touching the source database again. It's also a perfect
  audit log.

- 🥈 **Silver — clean & current.** One row per real-world thing, deduplicated,
  ordered, deletes applied. This is "the truth, right now." Most engineers query
  Silver.

- 🥇 **Gold — business-ready summaries.** Pre-computed answers to business
  questions: revenue per day, stock health, customer lifetime value. Fast for
  dashboards because the heavy maths is already done.

> **Why separate them?** Each layer has one job, can be tested on its own, can be
> rebuilt on its own, and can be owned by a different team. Raw stays raw
> (trustworthy), clean stays clean (usable), summaries stay fast (for BI).

---

## Part 6 — The supporting cast (orchestration, quality, monitoring)

### How Airflow fits in
Streaming (Bronze/Silver) runs **continuously** on its own. Airflow handles the
**periodic** work:
- `cdc_medallion` DAG (every 30 min): check the connector is healthy → rebuild
  Gold → run dbt → run data-quality gates. If a step fails, it retries with
  exponential backoff.
- `cdc_iceberg_maintenance` DAG (nightly 3 AM): compact small files, expire old
  snapshots, delete orphan files — housekeeping so the lakehouse stays fast.

> **Why housekeeping?** Streaming writes create *many tiny files*. Too many tiny
> files makes reads slow. Compaction merges them into fewer big files.

### How data quality works
Great Expectations reads simple rule files (JSON "suites") and turns each rule
into a `COUNT` query run **inside Trino** (so the data never leaves the
warehouse). Zero violations = pass. Any violation = fail the gate + increment a
Prometheus counter + block bad data from flowing to Gold/dashboards.

### How monitoring works
Each Spark batch **pushes numbers** (events processed, rows per batch, batch
duration) to a **Pushgateway**. **Prometheus** scrapes those numbers every 15
seconds. **Grafana** draws them live. Alerts fire if lag gets too high, if data
stops flowing, or if quality checks fail.

---

## Part 7 — Why these specific tools? (the "why not X" answers)

| Choice | Why this, not the obvious alternative |
|--------|----------------------------------------|
| **Log-based CDC (Debezium)** | vs. polling the DB every few minutes — polling misses deletes and in-between changes, and adds load. Reading the log catches everything, cheaply. |
| **Kafka in the middle** | vs. Debezium → Spark directly — direct wiring jams when a consumer is down and can't replay history. Kafka is a durable buffer with a rewind button. |
| **Iceberg** | vs. plain Parquet files — files can't do safe updates, deletes, or transactions. vs. Delta/Hudi — Iceberg has the cleanest multi-engine story (Spark writes, Trino reads the same tables). |
| **Spark Structured Streaming** | vs. hand-written consumers — Spark gives checkpointing, exactly-once plumbing, and scale for free. |
| **Trino for serving** | vs. querying with Spark — Trino is faster for interactive SQL and what BI tools expect. |
| **Micro-batches (seconds)** | vs. true sub-second streaming — micro-batches make Iceberg's atomic commits and exactly-once simple. Seconds of latency is fine for analytics. |

---

## Part 8 — Where the limits are (honest engineering)

- **The single Postgres replication slot** is the main bottleneck: it reads the
  log single-threaded. At massive scale you'd split tables across multiple slots.
- **Small files** from frequent streaming writes — solved by the nightly
  compaction DAG.
- **Latency is seconds, not milliseconds** — a deliberate trade for simplicity and
  correctness. If you truly needed sub-second, you'd swap Spark for Flink.

Being able to *name your own system's limits* is what separates a senior engineer
from a junior one.

---

## Part 9 — How to explain this in 60 seconds (interview version)

> "It's a Change Data Capture pipeline. Debezium reads Postgres's write-ahead log
> and streams every insert/update/delete into Kafka as Avro. Spark Structured
> Streaming consumes Kafka into an Iceberg lakehouse using a medallion layout:
> Bronze is the raw immutable change log, Silver is the deduplicated current state
> built with an idempotent, LSN-guarded MERGE, and Gold is business marts built
> with dbt. Trino serves SQL to Superset, Airflow orchestrates the batch parts,
> Great Expectations guards quality, and Prometheus plus Grafana handle
> observability. Exactly-once is achieved with at-least-once delivery plus an
> idempotent apply, so replays are safe. It's fully containerised with Docker
> Compose and has Kubernetes manifests and CI/CD."

---

## Part 10 — Mini-glossary (plain words)

- **CDC (Change Data Capture):** noticing and copying every change in a database.
- **WAL (Write-Ahead Log):** the database's own ordered list of every change.
- **LSN (Log Sequence Number):** a position number in that log; only goes up. Used
  to decide which change is newer.
- **Topic / partition (Kafka):** a named conveyor belt, split into parallel lanes.
- **Offset:** a message's position on the belt.
- **Avro:** a compact way to encode messages with a known shape.
- **Schema:** the agreed shape/columns of the data.
- **Idempotent:** doing it many times = doing it once (safe to retry).
- **MERGE (upsert):** "update if it exists, else insert."
- **Tombstone:** a marker that says "this row was deleted."
- **Snapshot (Iceberg):** a complete, consistent version of a table at a moment.
- **Checkpoint:** saved progress so a job can resume after a crash.
- **Exactly-once (effectively):** every change lands once — no loss, no duplicates
  — even through crashes.
- **Medallion:** the Bronze (raw) → Silver (clean) → Gold (summaries) layering.
- **Idempotent + at-least-once = effectively exactly-once:** the core trick.

---

## Where to go next

- Architecture diagrams: [ARCHITECTURE.md](ARCHITECTURE.md)
- Design decisions & trade-offs: [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md)
- Setup & running: [../SETUP.md](../SETUP.md)
- Interview Q&A per module: [interview/](interview/)
