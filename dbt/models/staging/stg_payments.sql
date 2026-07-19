with src as (select * from {{ source('silver', 'payments') }})
select
    cast(payment_id as bigint)    as payment_id,
    cast(order_id as bigint)      as order_id,
    method,
    cast(amount as decimal(12,2)) as amount,
    status,
    payment_ts
from src
