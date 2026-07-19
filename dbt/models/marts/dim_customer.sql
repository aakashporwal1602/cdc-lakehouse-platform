-- Conformed customer dimension.
select
    c.customer_id,
    c.email,
    c.full_name,
    c.tier,
    c.country,
    c.is_active,
    c.created_at,
    date_diff('day', c.created_at, current_timestamp) as tenure_days
from {{ ref('stg_customers') }} c
