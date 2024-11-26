{{
  config(
    materialized = 'table',
    )
}}

with 
stg_stores as (
  select 
    id,
    name,
    tax_rate
  from {{ source('jaffle_shop_staging', 'stg_stores') }}
),

agg_orders as (
  select 
    store_id,
    sum(order_total) as store_revenue
  from
    {{ source('jaffle_shop_staging', 'stg_orders') }}
  group by store_id
)

select 
  ss.name,
  ao.store_revenue
from 
  agg_orders ao
join stg_stores ss on ss.id = ao.store_id
order by ao.store_revenue desc