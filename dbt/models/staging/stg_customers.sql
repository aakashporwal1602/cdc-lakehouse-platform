with src as (select * from {{ source('silver', 'customers') }})
select
    cast(customer_id as bigint)   as customer_id,
    lower(email)                  as email,
    full_name,
    tier,
    country,
    is_active,
    created_at,
    updated_at,
    silver_updated_at
from src
