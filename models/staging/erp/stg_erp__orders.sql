with
    source_order as (
        select
            cast(id as int) pk_order
            , cast(employeeid as int) as fk_employee
            , cast(customerid as string) as fk_customer
            , cast(shipvia as int) as fk_shipvia
            , cast(orderdate as date) as order_date
            , cast(freight as numeric) as freight
            , cast(shipname as string) as nm_ship_name
            , cast(shipcity as string) as ship_city
            , cast(shipregion as string) as ship_region
            , cast(shipcountry as string) as ship_country
            , cast(shippeddate as date) as shipped_date
            , cast(requireddate as date) as required_date
        from {{ source('erp', '_order_') }}
    )
select *
from source_order