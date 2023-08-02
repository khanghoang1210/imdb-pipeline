from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow import DAG


from crawler import crawl_box_office, crawl_imdb

    
# start_date = datetime(2023, 7, 27)
# end_date = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)

# Hàm để chuyển đổi định dạng ngày sang 'dd-mm-yyyy'
def format_date_to_ddmmyyyy(ds):
    return datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y')


default_args = {
    'owner' : 'khanghoang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    default_args=default_args,
    dag_id='crawl_data_v1',
    description='crawler data from box office and imdb',
    start_date=datetime(2023, 7, 25),
    #end_date=datetime(2023, 7, 31),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # exc_date = '{{ ds }}'
    # exc_date_formatted = exc_date.date()

    task1 = PythonOperator(
        task_id = f'crawl_fact_data',
        python_callable=crawl_box_office,
        op_kwargs={'date': "{{ ti.xcom_pull(task_ids='format_date')['formatted_date'] }}"}
    )


    task2 = PythonOperator(
        task_id = f'crawl_dim_data',
        python_callable=crawl_imdb,
        op_kwargs={'date': "{{ ti.xcom_pull(task_ids='format_date')['formatted_date'] }}"}
    )

    format_date_task = PythonOperator(
    task_id='format_date',
    python_callable=format_date_to_ddmmyyyy,
    provide_context=True,
    op_args=["{{ ds }}"]
)

    format_date_task >> task1
    format_date_task >> task2

    

        