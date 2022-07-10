-- Создание представления "mart.f_customer_retention"
create or replace view de.mart.f_customer_retention as

with sales_per_week_cte as
(
    select id,
        fs.date_id,
        item_id,
        customer_id,
        city_id,
        quantity,
        payment_amount,
        status,
        week_of_year
from
    mart.f_sales as fs
    left join mart.d_calendar as dc
        on fs.date_id = dc.date_id
),

shipped_per_week_cte as
(
    select
        customer_id,
        week_of_year,
        sum(payment_amount) as payment_amount_sum,
        count(customer_id) as purchase_count
    from
        sales_per_week_cte
    where
        status = 'shipped'
    group by
        customer_id,
        week_of_year
),

new_customers_count_cte as
(
    select
        week_of_year,
        count(customer_id) as "new_customers_count"
    from
        shipped_per_week_cte
    where
        purchase_count = 1
    group by
        week_of_year
),

returning_customers_count_cte as
(
    select
        week_of_year,
        count(customer_id) as "returning_customers_count"
    from
        shipped_per_week_cte
    where
        purchase_count > 1
    group by
        week_of_year
),

refunds_cte as
(
    select
        customer_id,
        week_of_year,
        count(customer_id) as "refunds_count"
    from
        sales_per_week_cte
    where
        status = 'refunded'
    group by
        customer_id,
        week_of_year
),

refunded_customer_count_cte as
(
    select
        week_of_year,
        count(customer_id) as "refunded_customer_count"
    from
        refunds_cte
    group by
        week_of_year
),

item_id_cte as
(
    select
        week_of_year,
        array_agg(item_id) as "item_id"
    from
        sales_per_week_cte
    group by
        week_of_year
),

new_customers_revenue_cte as
(
    select
        week_of_year,
        sum(payment_amount_sum) as "new_customers_revenue"
    from
        shipped_per_week_cte
    where
        purchase_count = 1
    group by
        week_of_year
),

returning_customers_revenue_cte as
(
    select
        week_of_year,
        sum(payment_amount_sum) as "returning_customers_revenue"
    from
        shipped_per_week_cte
    where
        purchase_count > 1
    group by
        week_of_year
),

customers_refunded_cte as
(
    select
        week_of_year,
        sum(refunds_count) as "customers_refunded"
    from
        refunds_cte
    group by
        week_of_year
)

--

select
    week_of_year as "period_id",
    'weekly' as "period_name",
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
from
    new_customers_count_cte
    left join returning_customers_count_cte using (week_of_year)
    left join refunded_customer_count_cte using (week_of_year)
    left join item_id_cte using (week_of_year)
    left join new_customers_revenue_cte using (week_of_year)
    left join returning_customers_revenue_cte using (week_of_year)
    left join customers_refunded_cte using (week_of_year);
