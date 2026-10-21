-- analytics.fct_revenue: the sink table of the daily-revenue pipeline
-- (dev/owners.yaml); loaded_at is its freshness column.
{{ config(materialized='table') }}

select
    cast(date_trunc('day', updated_at) as date) as revenue_date,
    count(*) as order_count,
    coalesce(sum(amount), 0) as revenue,
    now() as loaded_at
from {{ ref('stg_orders') }}
where status <> 'cancelled'
group by 1
