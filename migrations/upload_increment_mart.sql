-- Заполнение таблицы: "mart.d_item" <- "staging.user_order_log"
insert into
    mart.d_item
    (item_id, item_name, created_date)
select
    item_id,
    item_name,
    min(date_time)::date
from
    staging.user_order_log
where
    item_id not in
    (
        select
            item_id
        from
            mart.d_item
    )
group by
    item_id,
    item_name;

-- Заполнение таблицы: "mart.d_customer" <- "staging.user_order_log"
insert into
    mart.d_customer
    (customer_id, first_name, last_name, city_id, created_date)
select
    customer_id,
    first_name,
    last_name,
    max(city_id),
    min(date_time)::date
from
    staging.user_order_log
where
    customer_id not in
    (
        select
            customer_id
        from
            mart.d_customer
    )
group by
    customer_id,
    first_name,
    last_name;

-- Заполнение таблицы: "mart.d_city" <- "staging.user_order_log"
insert into
    mart.d_city
    (city_id, city_name, created_date)
select
    city_id,
    city_name,
    min(date_time)::date
from
    staging.user_order_log
where
    city_id not in
    (
        select
            city_id
        from
            mart.d_city
    )
group by
    city_id,
    city_name;

-- Заполнение таблицы: "mart.f_sales" <- "staging.user_order_log"
insert into
    mart.f_sales
    (date_id, item_id, customer_id, city_id, quantity, payment_amount, status, created_date)
select
    dc.date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status,
    date_time::date
from
    staging.user_order_log uol
    left join mart.d_calendar as dc
        on uol.date_time::date = dc.date_actual
where
    uol.date_time::date = '{{ ds }}';
    
-- Замена значений NULL -> 'shipped' в таблице "de.mart.f_sales"
update
    de.mart.f_sales
set
    status = 'shipped'
where
    status is null;

-- Пересчет колонки "payment_amount" в таблице "de.mart.f_sales" с учетом статусов
update
    de.mart.f_sales
set
    payment_amount = -1 * payment_amount
where
    status = 'refunded'
    and payment_amount > 0
;