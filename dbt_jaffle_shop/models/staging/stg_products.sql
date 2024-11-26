{{
  config(
    materialized = 'table',
    )
}}

with raw_products as (
    select * 
    from {{ source('jaffle_shop_raw', 'raw_products') }}
),

stg_products as (
    select 
        row_number() over () as id,
        *
    from raw_products
)

select *
from stg_products