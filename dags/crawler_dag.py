from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from crawler import crawl_box_office, crawl_imdb
import json


def read_and_insert_data_to_postgres(**kwargs):
    ti = kwargs['ti']

    crawled_data = ti.xcom_pull(task_ids='crawl_fact_data')
    data = f"""{crawled_data}"""
    data_clean = data.replace("'", '"')
    json_data = json.loads(data_clean)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    for item in json_data:
        sql = """
        INSERT INTO test_fact (rank, revenue, partition_date, id)
        VALUES (%s, %s, %s, %s)
        """
        pg_hook.run(sql, parameters=(item['rank'], item['revenue'], item['partition_date'], item['id']))


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


    # crawl_dim_data = PythonOperator(
    #     task_id = 'crawl_dim_data',
    #     python_callable=crawl_imdb,
    #     op_kwargs={'date': '{{ ds }}'}
    # )

    # save_data = PythonOperator(
    #     task_id='save',
    #     python_callable=save_fact_data,
    #     op_kwargs={'data': data}
    # )
    # save_data
    create = PostgresOperator(
        task_id='create',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS test_fact (
            rank integer,
            revenue text,
            partition_date text,
            id text,
            primary key(partition_date, id)
        )
        """
    )
    
    insert_data_to_postgres = PythonOperator(
    task_id='insert_data_to_postgres',
    python_callable=read_and_insert_data_to_postgres,
    provide_context=True,
    op_kwargs={} 
    )
    
    crawl_fact_data>>create >> insert_data_to_postgres


        