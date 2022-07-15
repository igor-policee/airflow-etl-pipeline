-- Создание витрины
drop table if exists
    mart.f_customer_retention cascade;

create table
    mart.f_customer_retention
(
    week_of_year integer,
    new_customers_count integer,
    returning_customers_count integer,
    refunded_customer_count integer,
    purchase_refunds_count integer,
    new_customers_revenue numeric(14, 2),
    returning_customers_revenue numeric(14, 2)
);

insert into
    mart.f_customer_retention
with sales_per_week_cte as
(
    select
        id,
        fs.date_id,
        item_id,
        customer_id,
        city_id,
        quantity,
        payment_amount,
        status,
        date_actual,
        week_of_year
from
    mart.f_sales as fs
    left join mart.d_calendar as dc
        on fs.date_id = dc.date_id
),

purchases_cte as
(
    select
        customer_id,
        date_actual,
        week_of_year,
        status,
        payment_amount,
        row_number()  over (partition by customer_id, week_of_year order by date_actual::date asc) as "purchase_number"
    from
    (
        select
            customer_id,
            date_actual,
            week_of_year,
            status,
            payment_amount
        from
            sales_per_week_cte
        where
            status = 'shipped'
    ) as st0
),

-- Кол-во новых клиентов
new_customers_count_cte as
(
    select
        week_of_year,
        count(customer_id) as "new_customers_count"
    from
        purchases_cte
    where
        purchase_number = 1
    group by
        week_of_year
),

-- Кол-во вернувшихся клиентов
returning_customers_count_cte as
(
    select
        week_of_year,
        count(distinct customer_id) as "returning_customers_count"
    from
        purchases_cte
    where
        purchase_number > 1
    group by
        week_of_year
),

-- Кол-во уникальных клиентов, оформивших возврат
refunded_customer_count_cte as
(
    select
        week_of_year,
        count(distinct customer_id) as "refunded_customer_count"
    from
        sales_per_week_cte
    where
        status = 'refunded'
    group by
        week_of_year
),

-- Кол-во возвратов клиентов
purchase_refunds_count_cte as
(
    select
        week_of_year,
        count(customer_id) as "purchase_refunds_count"
    from
        sales_per_week_cte
    where
        status = 'refunded'
    group by
        week_of_year
),

-- Доход от новых клиентов
new_customers_revenue_cte as
(
    select
        week_of_year,
        sum(payment_amount) as "new_customers_revenue"
    from
        purchases_cte
    where
        status = 'shipped'
        and purchase_number = 1
    group by
        week_of_year
),

-- Доход от вернувшихся клиентов
returning_customers_revenue_cte as
(
    select
        week_of_year,
        sum(payment_amount) as "returning_customers_revenue"
    from
        purchases_cte
    where
        status = 'shipped'
        and purchase_number > 1
    group by
        week_of_year
)

-- Сборка витрины
select
    week_of_year,
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    purchase_refunds_count,
    new_customers_revenue,
    returning_customers_revenue
from
    new_customers_count_cte
    left join returning_customers_count_cte using (week_of_year)
    left join refunded_customer_count_cte using (week_of_year)
    left join purchase_refunds_count_cte using (week_of_year)
    left join new_customers_revenue_cte using (week_of_year)
    left join returning_customers_revenue_cte using (week_of_year);
