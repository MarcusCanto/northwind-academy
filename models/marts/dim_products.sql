with 
    product as (
        select *
        from {{ ref('stg_erp__product') }}
    )

    , category as (
        select *
        from {{ ref('stg_erp__categorys') }}
    )
    
    , supplier as (
        select *
        from {{ ref('stg_erp__supplier') }}
    )

    , joined as (
        select
        product.PK_PRODUCT
        , product.NM_PRODUCT
        , product.QUANTITY_PER_UNIT
        , product.PRICE_PER_UNIT
        , product.UNIT_IN_STOCK
        , product.UNIT_PER_ORDER
        , product.ORDER_LEVEL
        , product.IS_DISCONTINUED
        , category.NAME_CATEGORY
        , category.DESCRIPTION_CATEGORY
        , supplier.NM_SUPPLIER
        , supplier.CITY_SUPPLIER
        , supplier.COUNTRY_SUPPLIER
        from product
        left join category on product.fk_category = category.pk_category
        left join supplier on product.fk_supplier = supplier.pk_supplier
    )

select *
from joined
