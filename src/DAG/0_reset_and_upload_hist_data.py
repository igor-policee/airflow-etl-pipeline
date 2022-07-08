import json
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

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
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for _ in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def upload_historical_data_to_staging(filename, pg_table, pg_schema, ti):
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}'

    df = pd.read_csv(s3_filename)
    df.drop_duplicates(subset=['id'])

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

with DAG(
        dag_id='reset_and_upload_historical_data',
        default_args=args,
        description='Deleting all data and loading historical data in staging and mart',
        catchup=True,
        start_date=datetime.now(),
        schedule_interval='@once') as dag:

    task_reset_data = PostgresOperator(
        task_id='reset_data',
        postgres_conn_id=postgres_conn_id,
        sql="migrations/reset_data.sql")

    task_drop_constraint = PostgresOperator(
        task_id='drop_constraint',
        postgres_conn_id=postgres_conn_id,
        sql="migrations/drop_constraint.sql")

    task_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    task_get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    task_upload_historical_data_to_staging = PythonOperator(
        task_id='upload_historical_data_to_staging',
        python_callable=upload_historical_data_to_staging,
        op_kwargs={'filename': 'user_orders_log.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})
    
    task_add_column = PostgresOperator(
        task_id='add_column',
        postgres_conn_id=postgres_conn_id,
        sql="migrations/add_column.sql")
    
    task_upload_historical_data_to_mart = PostgresOperator(
        task_id='upload_historical_data_to_mart',
        postgres_conn_id=postgres_conn_id,
        sql="migrations/upload_historical_mart.sql")


    [task_reset_data, task_drop_constraint] \
    >> task_generate_report \
    >> task_get_report \
    >> task_upload_historical_data_to_staging \
    >> task_add_column \
    >> task_upload_historical_data_to_mart