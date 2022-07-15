-- Перемещение данных из raw staging в staging
insert into
    staging.user_order_log
    (date_time, city_id, city_name, customer_id, first_name,
     last_name, item_id, item_name, quantity, payment_amount)
select
    date_time,
    city_id,
    city_name,
    customer_id,
    first_name,
    last_name,
    item_id,
    item_name,
    quantity,
    payment_amount
from
    staging.user_order_log_raw
where
    date_time::date = '{{ ds }}'::date;

delete from
    staging.user_order_log_raw
where
    date_time::date = '{{ ds }}'::date;

---

insert into
    staging.user_order_log
    (date_time, city_id, city_name, customer_id, first_name,
     last_name, item_id, item_name, quantity, payment_amount, status)
select
    date_time,
    city_id,
    city_name,
    customer_id,
    first_name,
    last_name,
    item_id,
    item_name,
    quantity,
    payment_amount,
    status
from
    staging.user_order_log_raw_with_status
where
    date_time::date = '{{ ds }}'::date;

delete from
    staging.user_order_log_raw_with_status
where
    date_time::date = '{{ ds }}'::date;
