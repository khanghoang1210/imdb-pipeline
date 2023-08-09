from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from crawler import crawl_box_office, crawl_imdb
import json


def read_and_insert_fact_data(**kwargs):
    ti = kwargs['ti']

    crawled_data = ti.xcom_pull(task_ids='crawl_fact_data')
    data = f"""{crawled_data}"""
    data_clean = data.replace("'", '"')
    json_fact_data = json.loads(data_clean)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    for item in json_fact_data:
        sql = """
        INSERT INTO movie_revenue (rank, revenue, partition_date, id)
        VALUES (%s, %s, %s, %s)
        """
        pg_hook.run(sql, parameters=(item['rank'], item['revenue'], item['partition_date'], item['id']))



def read_and_insert_dim_data(**kwargs):
    ti = kwargs['ti']
    crawled_data = ti.xcom_pull(task_ids='crawl_dim_data')
    
    data = f"""{crawled_data}"""
    data_clean = data.replace("'", '"')
    json_dim_data = json.loads(data_clean)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    
    for item in json_dim_data:
        sql = """
            insert into movies (title, movie_id, url, director)
            values (%s, %s, %s, %s)
            """
        pg_hook.run(sql, parameters=(item['title'], item['movie_id'], item['url'], item['director']))

default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_data',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 8, 4),
    end_date=datetime(2023, 8, 6),
    schedule_interval='@daily'  
    
) as dag:


    crawl_fact_data = PythonOperator(
        task_id = 'crawl_fact_data',
        python_callable=crawl_box_office,
        op_kwargs={'date': '{{ ds }}'},
        provide_context = True,
        do_xcom_push=True
    )


    crawl_dim_data = PythonOperator(
        task_id = 'crawl_dim_data',
        python_callable=crawl_imdb,
        op_kwargs={'date': '{{ ds }}'},
        provide_context = True,
        do_xcom_push=True
    )


    create_fact_table = PostgresOperator(
        task_id='create_fact_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS movie_revenue (
            rank integer,
            revenue text,
            partition_date text,
            id text,
            primary key(partition_date, id)
        )
        """
    )

    create_dim_table = PostgresOperator(
        task_id='create_dim_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS movies (
            title text,
            movie_id text,
            url text,
            director text,
            primary key(movie_id)
        )
        """
    )
    
    insert_fact_data_to_postgres = PythonOperator(
    task_id='insert_fact_data_to_postgres',
    python_callable=read_and_insert_fact_data,
    provide_context=True,
    op_kwargs={} 
    )

    insert_dim_data_to_postgres = PythonOperator(
        task_id = 'insert_dim_data_to_postgres',
        python_callable=read_and_insert_dim_data,
        provide_context = True,
        op_kwargs={}
    )
    
    crawl_fact_data >> create_fact_table >> insert_fact_data_to_postgres
    crawl_dim_data >> create_dim_table >> insert_dim_data_to_postgres

        