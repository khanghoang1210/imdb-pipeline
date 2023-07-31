from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG


from crawler import crawl_box_office, crawl_imdb

    
start_date = date(2023, 7, 27)

default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_fact',
    description='crawler fact data from box office site',
    start_date=datetime(2023, 7, 26),
    schedule_interval='0 0 * * 4'
) as dag:
    

    # task1 = PythonOperator(
    #     task_id = 'crawl_fact_data',
    #     python_callable=crawl_box_office,
    #     op_kwargs={'date': start_date}
    # )

    # task1

    task2 = PythonOperator(
        task_id = 'crawl_dim_data',
        python_callable=crawl_imdb,
        provide_context = True
    )
    task2