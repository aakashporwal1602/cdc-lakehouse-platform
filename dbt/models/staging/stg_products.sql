with src as (select * from {{ source('silver', 'products') }})
select
    cast(product_id as bigint)    as product_id,
    sku, name, category,
    cast(unit_price as decimal(12,2)) as unit_price,
    is_active
from src
