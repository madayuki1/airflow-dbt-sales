{{
  config(
    materialized = 'table',
    )
}}

with
monthly_revenue as (
    select 
        to_char(date_trunc('month', ordered_at), 'YYYY-MM') as order_month,
        sum(order_total) as total_revenue
    from
        {{ source('jaffle_shop_staging', 'stg_orders') }}
    group by
        order_month
)

select 
    * 
from 
    monthly_revenue
order by order_month