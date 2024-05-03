with
    orders as (
        select *
        from {{ ref('stg_erp__orders') }}
    )

    , order_details as (
        select *
        from {{ ref('stg_erp__order_details') }}
    )

    , joined as (
        select
        order_details.FK_ORDER
        , order_details.FK_PRODUCT
        , orders.FK_EMPLOYEE
        , orders.FK_CUSTOMER
        , orders.FK_SHIPVIA
        , order_details.DISCOUNT_PERC
        , order_details.UNIT_PRICE
        , order_details.QUANTITY
        , orders.ORDER_DATE
        , orders.FREIGHT
        , orders.NM_SHIP_NAME
        , orders.SHIP_CITY
        , orders.SHIP_REGION
        , orders.SHIP_COUNTRY
        , orders.SHIPPED_DATE
        , orders.REQUIRED_DATE
        from order_details
        left join orders on order_details.fk_order = orders.pk_order
    )

    , primary_key as (
        select 
            cast(FK_ORDER as varchar) || '-' || FK_PRODUCT::varchar as sk_sales
            , *
        from joined
    )

select *
from primary_key