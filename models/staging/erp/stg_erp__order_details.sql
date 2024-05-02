with
    source_order_details as (
        select
            cast(orderid as int) as fk_order
            , cast(productid as int) as fk_product
            , cast(discount as numeric(18,2)) as discount_perc
            , cast(unitprice as numeric(18,2)) as unit_price
            , cast(quantity as int) as quantity
        from {{ source('erp', 'orderdetail') }}
    )
select *
from source_order_details