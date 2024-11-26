{{
  config(
    materialized = 'table',
    )
}}

with raw_supplies as (
    select * 
    from {{ source('jaffle_shop_raw', 'raw_supplies') }}
),

products_surrogate as (
    select id, sku
    from {{ ref('stg_products') }}
),

stg_supplies as (
    select 
        row_number() over () as id, 
        id as natural_key,
        name,
        cost,
        perishable,
        sku,
        created_at,
        updated_at
    from raw_supplies
)

select 
    ss.id, 
    ss.natural_key,
    ss.name,
    ss.cost,
    ss.perishable,
    ps.id as product_id,
    ss.sku,
    ss.created_at,
    ss.updated_at
from stg_supplies ss
join products_surrogate ps 
on ps.sku = ss.sku