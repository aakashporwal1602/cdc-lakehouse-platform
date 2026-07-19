select
    date(payment_ts)          as sale_date,
    count(*)                  as sales,
    count(distinct customer_id) as buyers,
    sum(sale_amount)          as revenue,
    avg(sale_amount)          as avg_ticket
from {{ ref('fct_sales') }}
group by date(payment_ts)
