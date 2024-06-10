from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

start_date = datetime(2023, 9, 1)
dag_interval = timedelta(days=1)


dag = DAG(
    'load_data_to_stg',
    start_date=start_date,
    schedule_interval=dag_interval,
    catchup=False
)


def load_data_to_stg(**kwargs):
    api_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
    headers = {
        'X-Nickname': 'stefankinaos',
        'X-Cohort': '16',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }

    limit = 50
    offset = 0
    sort_field = 'sort_field'
    sort_direction = 'asc'

    while True:
        params = {
            'limit': limit,
            'offset': offset,
            'sort_field': sort_field,
            'sort_direction': sort_direction
        }

        response = requests.get(api_url, headers=headers, params=params)
        data = response.json()

        if not data:
            break  # Завершаем загрузку, если нет данных

        # Здесь вставьте код для обработки данных и загрузки в STG-таблицы

        offset += limit


load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data_to_stg,
    provide_context=True,
    dag=dag
)

load_data_task
