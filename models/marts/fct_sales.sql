with
    int_orders_per_items as (
        select *
        from {{ ref('int_orders_per_items') }}
    )

    , dim_products as (
        select *
        from {{ ref('dim_products') }}
    )

    , dim_employees as (
        select *
        from {{ ref('dim_employees') }}
    )

    , joined as (
        select 
            fact.SK_SALES
            , fact.FK_ORDER as NF_number
            , fact.FK_PRODUCT
            , fact.FK_EMPLOYEE
            , fact.FK_CUSTOMER
            , fact.FK_SHIPVIA
            , fact.DISCOUNT_PERC
            , fact.UNIT_PRICE
            , fact.QUANTITY
            , fact.ORDER_DATE
            , fact.FREIGHT
            , fact.NM_SHIP_NAME
            , fact.SHIP_CITY
            , fact.SHIP_REGION
            , fact.SHIP_COUNTRY
            , fact.SHIPPED_DATE
            , fact.REQUIRED_DATE        
            , dim_products.NM_PRODUCT
            , dim_products.QUANTITY_PER_UNIT
            , dim_products.IS_DISCONTINUED
            , dim_products.NAME_CATEGORY
            , dim_products.DESCRIPTION_CATEGORY
            , dim_products.NM_SUPPLIER
            , dim_products.CITY_SUPPLIER
            , dim_products.COUNTRY_SUPPLIER
            , dim_employees.NM_EMPLOYEE
            , dim_employees.TITLE_EMPLOYEE
            , dim_employees.DT_HIREDATE
            , dim_employees.NM_MANAGER
        from int_orders_per_items as fact
        left join dim_products on fact.fk_product = dim_products.pk_product
        left join dim_employees on fact.fk_employee = dim_employees.pk_employee
    )

    , metrics as (
        select 
            SK_SALES
            , FK_PRODUCT
            , FK_EMPLOYEE
            , FK_CUSTOMER
            , FK_SHIPVIA
            , NF_number
            , DISCOUNT_PERC
            , UNIT_PRICE
            , QUANTITY
            , ORDER_DATE
            , NM_SHIP_NAME
            , SHIP_CITY
            , SHIP_REGION
            , SHIP_COUNTRY
            , SHIPPED_DATE
            , REQUIRED_DATE        
            , NM_PRODUCT
            , QUANTITY_PER_UNIT
            , IS_DISCONTINUED
            , NAME_CATEGORY
            , DESCRIPTION_CATEGORY
            , NM_SUPPLIER
            , CITY_SUPPLIER
            , COUNTRY_SUPPLIER
            , NM_EMPLOYEE
            , TITLE_EMPLOYEE
            , DT_HIREDATE
            , NM_MANAGER 
        , quantity * unit_price as groos_value
        , quantity * (1 - discount_perc) * unit_price  as net_value
        , cast(
            (dt_hiredate - order_date) / 365
            as numeric (18,2)
        ) as seniority_in_years
        , cast(
            freight / count(NF_number) over (partition by NF_number)
            as numeric (18,2)
            ) as prorated_shipping
        from joined
    )

select * 
from metrics