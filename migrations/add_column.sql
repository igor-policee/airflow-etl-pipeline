-- Создание колонки "status" в таблице de.staging.user_order_log
alter table
    staging.user_order_log
add column if not exists
    "status" varchar(10);

-- Создание колонки "status" в таблице de.mart.f_sales
alter table
    de.mart.f_sales
add column if not exists
    "status" varchar(10);

-- Создание технических колонок "created_date" для организации идемпотентности в таблицах
-- "mart.d_city", "mart.d_customer", "mart.d_item", "de.mart.f_sales"
alter table
    de.mart.d_city
add column if not exists
    "created_date" date;

alter table
    de.mart.d_customer
add column if not exists
    "created_date" date;

alter table
    de.mart.d_item
add column if not exists
    "created_date" date;

alter table
    de.mart.f_sales
add column if not exists
    "created_date" date;
