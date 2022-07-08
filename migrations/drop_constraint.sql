-- Удаление ограничений первичного ключа в таблицах
alter table
    mart.d_city
drop constraint if exists
    d_city_pkey cascade;

alter table 
    mart.d_customer
drop constraint if exists
    d_customer_pkey cascade;

alter table
    mart.d_item
drop constraint if exists
    d_item_pkey cascade;

alter table
    mart.f_sales
drop constraint if exists 
    f_sales_pkey cascade;

alter table
    staging.user_order_log
drop constraint if exists
    user_order_log_pkey cascade;
