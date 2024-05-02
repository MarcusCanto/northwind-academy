with
    employees as (
        select *
        from {{ ref('stg_erp__employees') }}
    )

    , joined as (
        select 
            employees.PK_EMPLOYEE
            , employees.FK_MANAGER
            , employees.NM_EMPLOYEE
            , employees.TITLE_EMPLOYEE
            , employees.BIRTHDATE_EMPLOYEE
            , employees.DT_HIREDATE
            , managers.NM_EMPLOYEE as NM_MANAGER
            , managers.TITLE_EMPLOYEE as TITLE_MANAGER
        from employees
        left join employees as managers 
            on employees.fk_manager = managers.pk_employee
    )
select *
from joined