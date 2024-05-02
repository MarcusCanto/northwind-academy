with
    source_product as (
        select 
            cast(ID as int) as pk_product
            , cast(SUPPLIERID as int) as fk_supplier
            , cast(CATEGORYID as int) as fk_category
            , cast(PRODUCTNAME as string) as nm_product
            , cast(QUANTITYPERUNIT as string) as quantity_per_unit
            , cast(UNITPRICE as numeric(18, 2)) as price_per_unit
            , cast(UNITSINSTOCK as int ) as unit_in_stock
            , cast(UNITSONORDER as int ) as unit_per_order
            , cast(REORDERLEVEL as int ) as order_level
            , case 
                when DISCONTINUED = 0 then false
                when DISCONTINUED = 1 then true
              end as is_discontinued
        from {{ source('erp', 'product') }}
    )
select *
from source_product