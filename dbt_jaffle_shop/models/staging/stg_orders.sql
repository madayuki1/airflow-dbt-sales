{{
  config(
    materialized = 'table',
    )
}}

with raw_orders as(
    select * from {{ source('jaffle_shop_raw', 'raw_orders') }}
),

customer_surrogate as (
    select 
        id, natural_key
    from {{ ref('stg_customers') }}
),

store_surrogate as (
    select
        id, natural_key
    from {{ ref('stg_stores') }}
),

stg_orders as (
    select
        row_number() over () as id,
        id as natural_key,
        customer,
        ordered_at,
        store_id,
        subtotal,
        tax_paid,
        order_total,
        created_at,
        updated_at
    from raw_orders
)

select 
    so.id,
    so.natural_key,
    cs.id as customer_id,
    so.customer as customer_natural_key,
    so.ordered_at,
    ss.id as store_id,
    so.subtotal,
    so.tax_paid,
    so.order_total,
    so.created_at,
    so.updated_at
from stg_orders so
join customer_surrogate cs
on cs.natural_key = so.customer
join store_surrogate ss
on ss.natural_key = so.store_id