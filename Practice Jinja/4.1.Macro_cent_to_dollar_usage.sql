select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    -- amount is stored in cents, convert it to dollars
    {{cent_to_dollars(column_name='amount',decimal_places=4)}} as amount,
    created as created_at

from {{source('exerp', 'payment')}}

