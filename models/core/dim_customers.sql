with
     customers as
     (
        select
               *
          from
               {{ ref ('stg_customers') }}
     )
   
   , orders as
     (
        select
               *
          from
               {{ ref ('stg_orders') }}
     )
   
   , succesful_order_payments as
     (
        select
               order_id
               , count(*) as total_payments
               , sum(order_amount) as order_amount
          from
               {{ ref ('stg_payments') }}
          where
               order_status = 'success'
          group by 
               order_id
     )
   
   , customer_orders as
     (
        select
               a.customer_id
             , min (a.order_date)          as first_order_date
             , max (a.order_date)          as most_recent_order_date
             , count (a.order_id)          as number_of_orders
             , sum (b.total_payments)      as total_payments
             , sum (b.order_amount)        as lifetime_value
          from
               orders a
     left join
               succesful_order_payments b
         using
               (order_id)
      group by
               1
     )
   
   , final as
     (
        select
               customers.customer_id
             , customers.first_name
             , customers.last_name
             , customer_orders.first_order_date
             , customer_orders.most_recent_order_date
             , coalesce (customer_orders.total_payments,0)    as total_payments
             , coalesce (customer_orders.number_of_orders, 0) as number_of_orders
             , coalesce (customer_orders.lifetime_value, 0)   as lifetime_value
          from
               customers
     left join
               customer_orders
         using
               (customer_id)
     )

select
     *
from
     final