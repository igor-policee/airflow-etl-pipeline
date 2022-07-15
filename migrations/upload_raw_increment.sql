-- Загрузка данных в таблицы "staging.user_order_log_raw_with_status", "staging.user_order_log_raw"
copy
    {{ ti.xcom_pull(key="target_table") }}
from
    '{{ ti.xcom_pull(key="local_filename") }}'
delimiter
    ','
csv header;