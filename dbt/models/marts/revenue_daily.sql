select
    date(p.payment_ts)        as revenue_date,
    sum(case when p.status='captured' then p.amount else 0 end)  as captured_revenue,
    sum(case when p.status='refunded' then p.amount else 0 end)  as refunded_revenue,
    sum(case when p.status='captured' then p.amount else 0 end)
      - sum(case when p.status='refunded' then p.amount else 0 end) as net_revenue
from {{ ref('stg_payments') }} p
group by date(p.payment_ts)
