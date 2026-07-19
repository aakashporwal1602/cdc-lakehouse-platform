select
    c.value_segment,
    c.tier,
    count(distinct s.customer_id) as customers,
    sum(s.sale_amount)            as revenue
from {{ ref('fct_sales') }} s
join {{ ref('customer_360') }} c on c.customer_id = s.customer_id
group by c.value_segment, c.tier
