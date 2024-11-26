{{
  config(
    materialized = 'table',
    )
}}

with
product as(
    select 
        id,
        sku,
        name,
        type,
        price
    from
        {{ source('jaffle_shop_staging', 'stg_products') }}
),

product_sold as (
    select
        product_id,
        count(product_id) as sold_amount
    from
        {{ source('jaffle_shop_staging', 'stg_items') }}
    group by 
        product_id
),

product_performance as (
    select 
        ps.product_id,
        sum(ps.sold_amount * p.price) as total_revenue
    from
        product_sold ps
    join product p on p.id = ps.product_id
    group by 
        ps.product_id
)

select 
    pp.product_id,
    p.name,
    pp.total_revenue,
    rank() over(order by pp.total_revenue desc) as product_ranking
from 
    product_performance pp
join 
    product p on p.id = pp.product_id