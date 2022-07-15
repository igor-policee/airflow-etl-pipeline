import json
import requests
import time
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


task_logger = logging.getLogger('airflow.task')  # logging

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'
nickname = 'igor_polishchuk'
cohort = '3'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    task_logger.info('Request to generate report and get task_id')  # logging

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)

    task_logger.info(f'Response is {response.content}')  # logging
    task_logger.info(f'task_id={task_id}')  # logging


def get_report_id(ti):
    task_logger.info('Request to get report_id')  # logging

    task_id = ti.xcom_pull(key='task_id')
    report_id = None

    for _ in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()

        task_logger.info(f'Response is {response.content}')  # logging

        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        task_logger.error('Failed to get the report_id')  # logging
        raise TimeoutError('Failed to get the report_id')

    ti.xcom_push(key='report_id', value=report_id)

    task_logger.info(f'report_id={report_id}')  # logging


def get_increment_id(date, ti):
    task_logger.info('Request to get increment_id')  # logging

    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00', headers=headers)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')  # logging

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)

    task_logger.info(f'increment_id={increment_id}')  # logging


def get_data_file(date, filename, ti):
    task_logger.info('Request file from s3')  # logging

    if increment_id := ti.xcom_pull(key='increment_id'):
        s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

        task_logger.info(f'File reference: {s3_filename}')  # logging

        local_filename = f'/opt/airflow/{date.replace("-", "_")}__{filename}'
        df = pd.read_csv(s3_filename)
        if 'status' in df.columns:
            ti.xcom_push(key='target_table', value='staging.user_order_log_raw_with_status')
        else:
            ti.xcom_push(key='target_table', value='staging.user_order_log_raw')

        df.to_csv(local_filename, sep=',', header=True, index=False)
        task_logger.info(f'Local file saved: {local_filename}')  # logging

        ti.xcom_push(key='local_filename', value=local_filename)

    else:
        task_logger.error(f'increment_id={increment_id} therefore, it is not possible to request data')  # logging
        raise FileNotFoundError('File is not available')


args = {
    'owner': 'student',
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': True,
    'start_date': datetime.now() - timedelta(days=8),
    'end_date': datetime.now() - timedelta(days=1),
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

business_dt = '{{ ds }}'

with DAG(dag_id='upload_increment_data',
         default_args=args,
         description='Loading data increments for the current date') as dag:

    task_create_raw_staging = PostgresOperator(
        task_id='create_raw_staging',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/create_raw_staging.sql')

    task_add_column = PostgresOperator(
        task_id='add_column',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/add_column.sql')

    task_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    task_get_report_id = PythonOperator(
        task_id='get_report_id',
        python_callable=get_report_id)

    task_get_increment_id = PythonOperator(
        task_id='get_increment_id',
        python_callable=get_increment_id,
        op_kwargs={'date': business_dt})

    task_get_data_file = PythonOperator(
        task_id='get_data_file',
        python_callable=get_data_file,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv'})

    task_clear_late_data = PostgresOperator(
        task_id='clear_late_data',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/clear_late_data.sql')

    task_upload_raw_increment = PostgresOperator(
        task_id='upload_raw_increment',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/upload_raw_increment.sql')

    task_remove_data_file = BashOperator(
        task_id='remove_data_file',
        bash_command='rm -f {{ ti.xcom_pull(key="local_filename") }}')

    task_move_increment_to_staging = BashOperator(
        task_id='move_increment_to_staging',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/move_increment_to_staging.sql')

    task_upload_increment_to_mart = PostgresOperator(
        task_id='upload_increment_to_mart',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/upload_increment_mart.sql')

    task_create_datamart = PostgresOperator(
        task_id='create_datamart',
        postgres_conn_id=postgres_conn_id,
        sql='migrations/create_f_customer_retention.sql')


    [task_create_raw_staging, task_add_column] \
    >> task_generate_report \
    >> task_get_report_id \
    >> task_get_increment_id \
    >> task_get_data_file \
    >> task_clear_late_data \
    >> task_upload_raw_increment \
    >> task_remove_data_file \
    >> task_move_increment_to_staging \
    >> task_upload_increment_to_mart
