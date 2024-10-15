from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator


default_args = {
    'owner': 'tahoora',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'crm_data_check',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahooramajlesi@gmail.com', 
        subject='FAA check crm node', 
        html_content='Run started with workflow_run_id of {{ var.value.workflow_run_id }}',
        dag=dag 
    ) 

    list_number_of_crm_files = BashOperator(
        task_id='list_number_of_crm_files',
        bash_command='ssh gpadmin@192.168.5.230 "ls /backup/TO_FAA/ | grep RTD_CUST | wc -l"',
        do_xcom_push = True,
        dag=dag
    )

    def check_count(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='list_number_of_crm_files')
        if '0' in value:
            return 'crm_data_not_found'
        else:
            return 'list_crm_files'

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_count,
        dag=dag
    )

    list_crm_files = BashOperator(
        task_id='list_crm_files',
        bash_command='ssh gpadmin@192.168.5.230 `ls /backup/TO_FAA/input | grep RTD_CUST | tr "\n" `',
        do_xcom_push = True,
        dag = dag
    )

    def set_files_value(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='list_crm_files')
        Variable.set('files', value)

    set_files_task = PythonOperator(
        task_id='set_files_task',
        python_callable=set_files_value,
        provide_context=True,
        dag=dag
    )

    crm_data_not_found = EmailOperator(
        task_id='crm_data_not_found', 
        to='tahooramajlesi@gmail.com', 
        subject='FAA crm check', 
        html_content='Warning, no crm data found',
        dag=dag
    )

    crm_data_found = EmailOperator(
        task_id='crm_data_found', 
        to='tahooramajlesi@gmail.com', 
        subject='FAA crm check', 
        html_content='Crm data found following files {{ var.value.files }}',
        dag=dag
    )

    task_end = DummyOperator(task_id='task_end')

    send_email0 >> list_number_of_crm_files >> branch_op >> [crm_data_not_found, list_crm_files] >> task_end
    list_crm_files >> set_files_task >> crm_data_found