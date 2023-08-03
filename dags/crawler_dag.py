from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.macros import ds_format

from crawler import crawl_box_office, crawl_imdb




default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_data',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 7, 25),
    #end_date=datetime(2023, 7, 31),
    schedule_interval='@daily'  
    
) as dag:


    crawl_fact_data = PythonOperator(
        task_id = f'crawl_fact_data',
        python_callable=crawl_box_office,
        op_kwargs={'date': '{{ ds }}'}
    )


    crawl_dim_data = PythonOperator(
        task_id = f'crawl_dim_data',
        python_callable=crawl_imdb,
        op_kwargs={'date': '{{ ds }}'}
    )


    [crawl_fact_data, crawl_dim_data]

    

        