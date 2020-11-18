with 
    orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from {{ source('jaffle_shop', 'orders') }}
)

, final as (
    select 
          orders.order_id
        , orders.customer_id
        , orders.order_date
        , orders.status
    from    
        orders
)

select * from final