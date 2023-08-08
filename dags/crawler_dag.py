from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from crawler import crawl_box_office, crawl_imdb, create_fact_table
import psycopg2


def create_fact():
    connection = psycopg2.connect(
        host="postgres",
        port='5432',
        database="postgres",
        user="khanghoang",
        password="12102003"
    )
    
    cursor = connection.cursor()

    cursor.execute(""" create table if not exists fact_test(
                rank text, revenue text, partition_date text,
                id text); """)
    connection.commit()
    cursor.close()
    connection.close()

default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_data',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 8, 8),
    #end_date=datetime(2023, 6, 4),
    schedule_interval='@daily'  
    
) as dag:


    # crawl_fact_data = PythonOperator(
    #     task_id = 'crawl_fact_data',
    #     python_callable=crawl_box_office,
    #     op_kwargs={'date': '{{ ds }}'}
    # )


    # crawl_dim_data = PythonOperator(
    #     task_id = 'crawl_dim_data',
    #     python_callable=crawl_imdb,
    #     op_kwargs={'date': '{{ ds }}'}
    # )
    create_fact_table_task = PythonOperator(
        task_id='create_fact_table_task',
        python_callable=create_fact
    )
    # create_dim_table = PostgresOperator(
    #     task_id = 'create_dim_table',
    #     postgres_conn_id='postgres_localhost',
    #     sql = """
    #             create table if not exists test_dim (
    #                 dt date,
    #                 dag_id character,
    #                 primary key(dt, dag_id)
    #             )
    #         """
    # )

#     insert_fact_data = PostgresOperator(
#     task_id="insert_data_to_fact_table",
#     postgres_conn_id='postgres_localhost',
#     sql="""
#         INSERT INTO test_fact (rank, revenue, partition_date, id)
#         VALUES (%s, %s, %s, %s)
#     """,
#     # Loop through the list of dictionaries and provide values for each record
#     parameters=[
#         (record['rank'], record['revenue'], record['partition_date'], record['id'])
#         for record in crawl_fact_data.output
#     ]
# )
    # create_fact_table = PostgresOperator()

    create_fact_table_task


        