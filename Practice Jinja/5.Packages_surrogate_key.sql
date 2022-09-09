   select
        {{dbt_utils.surrogate_key(['user_id','order_date'])}} as id,
        user_id ,
        order_date,
        count(*)

    from {{source('exerp' , 'orders')}}
    group by 1,2,3
