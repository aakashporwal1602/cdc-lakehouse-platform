with src as (select * from {{ source('silver', 'inventory') }})
select
    cast(inventory_id as bigint)  as inventory_id,
    cast(product_id as bigint)    as product_id,
    warehouse_id,
    quantity_on_hand,
    reorder_level
from src
