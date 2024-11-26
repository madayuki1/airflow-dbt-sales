{{
  config(
    materialized = 'table',
    )
}}

with raw_items as (
    select * 
    from {{ source('jaffle_shop_raw', 'raw_items') }}
),

orders_surrogate as (
    select id, natural_key
    from {{ ref('stg_orders') }}
),

products_surrogate as (
    select id, sku
    from {{ ref('stg_products') }}
),

stg_items as (
    select 
        row_number() over () as id,
        id as natural_key,
        order_id,
        sku
    from raw_items
)

select 
    si.id,
    si.natural_key,
    os.id as order_id,
    si.order_id as order_natural_key,
    ps.id as product_id,
    si.sku as sku
from 
    stg_items si
join orders_surrogate os
on os.natural_key = si.order_id
join products_surrogate ps
on ps.sku = si.sku