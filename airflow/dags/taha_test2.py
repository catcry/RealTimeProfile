
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
import re
from airflow.models import Variable
from airflow.operators.python import PythonOperator




default_args = {
    'owner': 'taha',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}



with DAG(
    dag_id = 'taha_test2',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    get_weeks1 = PostgresOperator(
        task_id='get_weeks',
        sql="""
            select data_date as t2date from core.partition_date_create_times
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    def pull_function(**kwargs):
        global formatted_dates
        pulled_data = kwargs['ti'].xcom_pull(task_ids='get_weeks', key='return_value')
        formatted_dates = [date[0].strftime("%Y-%m-%d") for date in pulled_data]
        kwargs['ti'].xcom_push(key='formatted_dates', value=formatted_dates)
        
        
    
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True,
        dag=dag,
    )

    def for_loop(**kwargs):
        x = 0
        results = []
        formatted_dates = kwargs['ti'].xcom_pull(task_ids='pull_task', key='formatted_dates')
        logging.info(formatted_dates)
        for t2date in formatted_dates:
            logging.info(t2date)
            x += 1
            task_id = f'get_weeks_loop_{x}'
            sql_query = """
                select dat from core.test_table where dat='{t2date}';
            """.format(t2date=t2date)

            get_weeks_loop = PostgresOperator(
                task_id=task_id,
                sql=sql_query,
                postgres_conn_id='greenplum_conn',
                dag=dag,
            )

            # Execute the task
            tcrmdate = get_weeks_loop.execute(context=kwargs)
            #logging.info(result[0][0])
            #logging.info("heeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeey")

            

        

    for_loop_task = PythonOperator(
        task_id='for_loop_task',
        python_callable=for_loop,
        provide_context=True,
        dag=dag,
    )

    get_weeks1 >> pull_task >> for_loop_task







