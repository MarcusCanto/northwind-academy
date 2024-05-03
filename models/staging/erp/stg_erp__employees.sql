with
    source_employees as (
        select
            cast(id as int) as pk_employee
            , cast(reportsto as int) as fk_manager -- Coluna "reportsto" é o id que o funcionário tem que de superior
            , cast(firstname as string) || ' ' || cast(lastname as string) as nm_employee
            , cast(title as string) as title_employee
            , cast(birthdate as date) as birthdate_employee
            , cast(hiredate as date) as dt_hiredate
        from {{ source('erp', 'employee') }}
    )
select *
from source_employees