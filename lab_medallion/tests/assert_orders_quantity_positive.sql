-- A quantity must be strictly positive: the test fails if this query returns rows
select
    order_id,
    quantity
from {{ ref('stg_orders') }}
where quantity <= 0