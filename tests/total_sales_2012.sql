with
    sales_in_2012 as (
        select sum(groos_value) as total_gross_value
        from {{ ref('fct_sales') }}
        where order_date between '2012-01-01'and '2012-12-31'
    )

select total_gross_value
from sales_in_2012
where total_gross_value not between 226298 and 226299