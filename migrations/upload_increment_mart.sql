-- Filling in the table: "mart.d_item" <- "staging.user_order_log"
insert into
    mart.d_item
    (item_id, item_name)
select
    item_id,
    item_name
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

-- Filling in the table: "mart.d_customer" <- "staging.user_order_log"
insert into
    mart.d_customer
    (customer_id, first_name, last_name, city_id)
select
    customer_id,
    first_name,
    last_name,
    max(city_id)
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

-- Filling in the table: "mart.d_city" <- "staging.user_order_log"
insert into
    mart.d_city
    (city_id, city_name)
select
    city_id,
    city_name
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

-- Filling in the table: "mart.f_sales" <- "staging.user_order_log"
insert into
    mart.f_sales
    (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select
    dc.date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status
from
    staging.user_order_log uol
    left join mart.d_calendar as dc
        on uol.date_time::date = dc.date_actual
where
    uol.date_time::date = '{{ ds }}';
    
-- Replacing NULL -> 'shipped' values in the "de.mart.f_sales" table
update
    de.mart.f_sales
set
    status = 'shipped'
where
    status is null;

-- Recalculation of the "payment_amount" column in the "de.mart.f_sales" table, taking into account the statuses
update
    de.mart.f_sales
set
    payment_amount = -1 * payment_amount
where
    status = 'refunded'
    and payment_amount > 0;
