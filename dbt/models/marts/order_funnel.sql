select
    status,
    count(*)          as orders,
    sum(order_total)  as gross_order_value,
    avg(order_total)  as avg_order_value
from {{ ref('stg_orders') }}
group by status
