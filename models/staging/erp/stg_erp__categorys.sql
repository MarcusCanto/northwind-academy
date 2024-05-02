with
    source_category as (
        select 
            cast(ID as int) as pk_category
            , cast(CATEGORYNAME as varchar) as name_category
            , cast(DESCRIPTION as varchar) as description_category
        from {{ source('erp', 'category') }}
    )
select *
from source_category