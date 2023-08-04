from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.macros import ds_format
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

    create_dim_table = PostgresOperator(
        task_id = 'connect_to_postgresql',
        postgres_conn_id='postgre_localhost',
        sql = """
                create table if not exists test (
                    dt date,
                    dag_id character,
                    primary key(dt, dag_id)
                )
            """
    )
    # create_fact_table = PostgresOperator()

    [crawl_fact_data, crawl_dim_data] >> create_dim_table

    

        