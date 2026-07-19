-- Share of orders reaching a terminal happy-path state.
select
    date(order_ts) as order_date,
    count(*)                                                        as total_orders,
    sum(case when status='delivered' then 1 else 0 end)            as delivered,
    sum(case when status='cancelled' then 1 else 0 end)            as cancelled,
    cast(sum(case when status='delivered' then 1 else 0 end) as double)
      / nullif(count(*),0)                                          as delivery_rate
from {{ ref('stg_orders') }}
group by date(order_ts)
