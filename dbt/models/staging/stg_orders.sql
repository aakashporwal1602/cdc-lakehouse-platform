with src as (select * from {{ source('silver', 'orders') }})
select
    cast(order_id as bigint)      as order_id,
    cast(customer_id as bigint)   as customer_id,
    status,
    cast(order_total as decimal(12,2)) as order_total,
    currency,
    order_ts,
    silver_updated_at
from src
