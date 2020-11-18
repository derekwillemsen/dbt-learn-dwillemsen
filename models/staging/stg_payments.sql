select
     id                               as payment_id
   , orderid                          as order_id
   , status                           as order_status
   , created::date                    as created_at
   , (amount / 100) ::numeric (20, 2) as order_amount
from
     {{ source ('stripe', 'payment') }} 
qualify 
     row_number() over (partition by id order by _batched_at desc) = 1