with
    source_supplier as (
        select 
            cast(C1 as varchar) as pk_supplier
            , cast(C2 as varchar) as nm_supplier
            , cast(C5 as varchar) as city_supplier
            , cast(C9 as varchar) as country_supplier
        from {{ source('erp', 'supplier') }}
        where c1 != 'id'
    )
select *
from source_supplier