-- Customer 360: RFM + lifetime value from order/payment behaviour.
with orders as (
    select customer_id,
           count(*)                              as order_count,
           sum(order_total)                      as gross_ordered,
           max(order_ts)                         as last_order_ts
    from {{ ref('stg_orders') }}
    group by customer_id
),
paid as (
    select o.customer_id,
           sum(case when p.status = 'captured' then p.amount else 0 end) as lifetime_value
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p on p.order_id = o.order_id
    group by o.customer_id
)
select
    d.customer_id,
    d.email,
    d.tier,
    d.country,
    d.tenure_days,
    coalesce(o.order_count, 0)               as order_count,
    coalesce(o.gross_ordered, 0)             as gross_ordered,
    coalesce(pd.lifetime_value, 0)           as lifetime_value,
    o.last_order_ts,
    date_diff('day', o.last_order_ts, current_timestamp) as recency_days,
    case
        when coalesce(pd.lifetime_value,0) > 5000 then 'high_value'
        when coalesce(pd.lifetime_value,0) > 1000 then 'mid_value'
        else 'low_value'
    end as value_segment
from {{ ref('dim_customer') }} d
left join orders o  on o.customer_id = d.customer_id
left join paid pd   on pd.customer_id = d.customer_id
