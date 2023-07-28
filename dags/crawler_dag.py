from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from src.utils.fact_movies_crawler import crawl_box_office

# def greet(date):
#     print(f"hello world, {date}")

start_date = datetime(2023, 7, 22)
default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_fact',
    description='crawler fact data from box office site',
    start_date=datetime(2023, 7, 23),
    schedule_interval='0 0 * * *'
) as dag:
    

    task1 = PythonOperator(
        task_id = 'crawl_fact_data',
        python_callable=crawl_box_office,
        op_kwargs={'date': '2023-07-22'}
    )

    task1