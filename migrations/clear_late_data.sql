-- Удаление данных за текущий день (логическая дата задачи) из таблиц
-- "de.mart.f_sales", "mart.d_city", "mart.d_customer", "mart.d_item"
delete from
    de.mart.f_sales
where
    de.mart.f_sales.created_date = '{{ ds }}'::date;

delete from
    de.mart.d_city
where
    de.mart.d_city.created_date = '{{ ds }}'::date;

delete from
    de.mart.d_customer
where
    de.mart.d_customer.created_date = '{{ ds }}'::date;

delete from
    de.mart.d_item
where
    de.mart.d_item.created_date = '{{ ds }}'::date;
    
-- Удаление данных за текущий день (логическая дата задачи) из таблицы "staging.user_order_log"
delete from
    staging.user_order_log
where
    staging.user_order_log.date_time::date = '{{ ds }}'::date;