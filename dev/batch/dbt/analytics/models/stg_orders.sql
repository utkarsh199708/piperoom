select
    id as order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('warehouse', 'orders') }}
