 --  with customers as(
    select
        id as customer_id,
        first_name,
        last_name

    from {{source('exerp', 'customers')}} -- dbt_test.stg.customers
  -- )
  -- select * from customers



