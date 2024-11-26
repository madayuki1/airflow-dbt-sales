{{
  config(
    materialized = 'table',
    )
}}  

with
stg_orders as (
    select 
        id,
        customer_id,
        ordered_at,
        subtotal,
        tax_paid,
        order_total
    from {{ source('jaffle_shop_staging', 'stg_orders') }}
),

stg_customer as (
    select 
        id,
        name
    from {{ source('jaffle_shop_staging', 'stg_customers') }}
),

customer_spending as (
    select 
        customer_id,
        sum(order_total) as total_spending
    from stg_orders
    group by customer_id
)

select 
    sc.name,
    cs.total_spending,
    rank() over(order by total_spending desc) as spending_rank
from customer_spending cs
join stg_customer sc on cs.customer_id = sc.id