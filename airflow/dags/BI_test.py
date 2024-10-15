import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


def file_list():

    files = os.listdir("/home/airflow/social_link/airflow")
    return files






with DAG(
    dag_id = 'BI_test',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    files_list = PythonOperator(
        task_id='files',
        python_callable=file_list,
        provide_context=True,
        dag=dag
    )



    def for_loop(**kwargs):
        x = 0
        
        files = kwargs['ti'].xcom_pull(task_ids='files', key='return_value')
        #logging.info(formatted_dates)
        for f in files:
            #logging.info(t2date)
            x += 1
            task_id = f'java_run_{x}'
            

            run_java = BashOperator(
                task_id=f'{task_id}',
                bash_command=f'/app/jdk-17.0.10/bin/java /root/Main.java {f}',
                do_xcom_push = True,
                dag=dag
                )

            
            result = run_java.execute(context=kwargs)


    for_loop_task = PythonOperator(
        task_id='for_loop_task',
        python_callable=for_loop,
        provide_context=True,
        dag=dag,
    )


    
    files_list >> for_loop_task





