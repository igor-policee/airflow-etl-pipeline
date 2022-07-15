-- Удаление данных за текущий день (логическая дата задачи) из таблицы "de.mart.f_sales"
delete from
    mart.f_sales
where 
    id in
(
    select
        id
    from
        mart.f_sales as fs
        left join mart.d_calendar as dc
            on fs.date_id = dc.date_id
    where
        date_actual::date = '{{ ds }}'::date
);

-- Удаление данных за текущий день (логическая дата задачи) из таблицы "staging.user_order_log"
delete from
    staging.user_order_log
where
    date_time::date = '{{ ds }}'::date;
    
-- Удаление данных за текущий день (логическая дата задачи) из таблиц "staging.user_order_log_raw" и "staging.user_order_log_raw_with_status"
delete from
    staging.user_order_log_raw
where
    date_time::date = '{{ ds }}'::date;

delete from
    staging.user_order_log_raw_with_status
where
    date_time::date = '{{ ds }}'::date;