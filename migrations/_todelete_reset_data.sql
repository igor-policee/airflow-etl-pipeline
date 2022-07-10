-- Удаление данных из таблиц "mart.d_city", "mart.d_customer", "mart.d_item", "de.mart.f_sales"
truncate table
    de.mart.d_city
restart identity cascade;

truncate table
    de.mart.d_customer
restart identity cascade;

truncate table
    de.mart.d_item
restart identity cascade;

truncate table
    de.mart.f_sales
restart identity cascade;

-- Удаление данных из таблицы "staging.user_order_log"
truncate table
    de.staging.user_order_log
restart identity cascade;

-- Удаление колонки "status" в таблице "staging.user_order_log"
alter table
    de.staging.user_order_log
drop column if exists
    "status";

-- Удаление колонки "status" в таблице "mart.f_sales"
alter table
    de.mart.f_sales
drop column if exists
    "status";

-- Удаление технических колонок "created_date" в таблицах
-- "mart.d_city", "mart.d_customer", "mart.d_item", "de.mart.f_sales"
alter table
    de.mart.d_city
drop column if exists
    "created_date";

alter table
    de.mart.d_customer
drop column if exists
    "created_date";

alter table
    de.mart.d_item
drop column if exists
    "created_date";

alter table
    de.mart.f_sales
drop column if exists
    "created_date";
