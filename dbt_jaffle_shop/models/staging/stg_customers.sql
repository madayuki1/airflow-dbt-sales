{{
  config(
    materialized = 'table'
    )
}}

with raw_customers as (
    select * from {{ source('jaffle_shop_raw', 'raw_customers') }}
),

stg_customers as (
    select 
        row_number() over () as id, 
        id as natural_key,
        name,
        created_at,
        updated_at
    from raw_customers
)

select * 
from stg_customers