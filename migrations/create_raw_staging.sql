-- Создание таблицы "staging.user_order_log_raw" для загрузки данных из файла "user_orders_log_inc.csv" (s3)
create table if not exists
    staging.user_order_log_raw
(
    id             bigint,
    date_time      timestamp,
    city_id        bigint,
    city_name      varchar(100),
    customer_id    integer,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        bigint,
    item_name      varchar(100),
    quantity       bigint,
    payment_amount numeric(10, 2)
);

create table if not exists
    staging.user_order_log_raw_with_status
(
    id             bigint,
    date_time      timestamp,
    city_id        bigint,
    city_name      varchar(100),
    customer_id    integer,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        bigint,
    item_name      varchar(100),
    quantity       bigint,
    payment_amount numeric(10, 2),
    status         varchar(10)
);
