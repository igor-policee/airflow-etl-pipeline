-- Creating column "status" in the table de.staging.user_order_log
alter table
    staging.user_order_log
add column if not exists
    "status" varchar(10);

-- Creating column "status" в таблице de.mart.f_sales
alter table
    de.mart.f_sales
add column if not exists
    "status" varchar(10);
