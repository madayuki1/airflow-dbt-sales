{{
  config(
    materialized = 'table',
    )
}}

with raw_stores as (
    select * from {{ source('jaffle_shop_raw', 'raw_stores') }}
),

stg_stores as (
    select 
        row_number() over () as id, 
        id as natural_key,
        name,
        opened_at,
        tax_rate,
        created_at,
        updated_at
    from
        raw_stores
)

select * 
from stg_stores