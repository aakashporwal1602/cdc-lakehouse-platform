# dbt — Gold marts

Transforms Silver Iceberg tables into conformed business marts consumed by
Superset. `staging/` cleans + renames; `marts/` builds star-schema facts & dims.

```bash
dbt deps && dbt build --profiles-dir .
```
