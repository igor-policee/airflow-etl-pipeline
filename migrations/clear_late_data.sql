-- Deleting data for the current day (logical date of the task) from the table "de.mart.f_sales"
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

-- Deleting data for the current day (logical date of the task) from the table "staging.user_order_log"
delete from
    staging.user_order_log
where
    date_time::date = '{{ ds }}'::date;
    
-- Deleting data for the current day (logical date of the task) from the tables "staging.user_order_log_raw", "staging.user_order_log_raw_with_status"
delete from
    staging.user_order_log_raw
where
    date_time::date = '{{ ds }}'::date;

delete from
    staging.user_order_log_raw_with_status
where
    date_time::date = '{{ ds }}'::date;
