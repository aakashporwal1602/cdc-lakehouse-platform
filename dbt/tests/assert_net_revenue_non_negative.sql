-- Custom singular test: net revenue should never be negative on a given day.
select revenue_date, net_revenue
from {{ ref('revenue_daily') }}
where net_revenue < 0
