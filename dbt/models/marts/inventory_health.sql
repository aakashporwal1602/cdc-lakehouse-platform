select
    i.product_id,
    p.name        as product_name,
    p.category,
    i.warehouse_id,
    i.quantity_on_hand,
    i.reorder_level,
    (i.quantity_on_hand <= i.reorder_level) as needs_reorder,
    case
        when i.quantity_on_hand = 0 then 'stockout'
        when i.quantity_on_hand <= i.reorder_level then 'low'
        else 'healthy'
    end as stock_status
from {{ ref('stg_inventory') }} i
join {{ ref('stg_products') }} p on p.product_id = i.product_id
