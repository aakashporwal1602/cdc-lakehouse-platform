-- Sales fact grain: one row per captured payment (recognised sale).
select
    p.payment_id,
    o.order_id,
    o.customer_id,
    o.order_ts,
    p.payment_ts,
    p.method                              as payment_method,
    p.amount                              as sale_amount,
    o.currency,
    o.status                              as order_status
from {{ ref('stg_payments') }} p
join {{ ref('stg_orders') }} o on o.order_id = p.order_id
where p.status = 'captured'
