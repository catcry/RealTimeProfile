from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Mahdis',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}

def get_name():
    return "Jerry"

def greet(age, ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello {name}, i am {age} years old")

with DAG(
    dag_id='mhds_test',
    default_args=default_args,
    schedule_interval=None,  # run manually
    catchup=False,
    tags=['example'],
) as dag:

    hello_task = BashOperator(
        task_id='HelloWorld',
        bash_command='echo "hello world"',
    )
    hello_task2 = BashOperator(
        task_id='HelloWorld2',
        bash_command='echo "hello world"',
    )
    greet_task = PythonOperator(
        task_id= 'greet',
        python_callable=greet,
        op_kwargs={'age':20}
    )
    name_task = PythonOperator(
        task_id= 'get_name',
        python_callable=get_name
    )
 

    hello_task >> hello_task2 >> name_task >> greet_task
