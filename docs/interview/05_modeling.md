# Medallion & data modeling

**Q: Why Bronze/Silver/Gold?**
A: Separation of concerns. Bronze = immutable raw log (replay source of truth);
Silver = clean, deduplicated current state; Gold = business-shaped marts. Each is
independently testable, reprocessable, and ownable.

**Q: Bronze is append-only — why keep raw forever?**
A: It's the audit log and the replay source: Silver/Gold can always be rebuilt
from Bronze after a bug fix or model change, without re-reading the source DB.

**Q: How is Silver kept as current state?**
A: An idempotent MERGE per primary key applies the latest version and DELETEs on
tombstones. It's effectively an SCD Type 1 (overwrite) table; SCD Type 2 history
would add validity ranges if required.

**Q: How would you model the Gold layer?**
A: Star schema: conformed dimensions (`dim_customer`) and fact tables
(`fct_sales` at captured-payment grain), plus pre-aggregated marts
(`revenue_daily`, `inventory_health`) for BI performance. Grain is defined
explicitly per fact.

**Q: How do you test data quality?**
A: Great Expectations suites compiled to Trino SQL (uniqueness, not-null, range,
set membership) run as an Airflow gate; failures block dbt publish and increment
`cdc_dq_validation_failures_total`.
