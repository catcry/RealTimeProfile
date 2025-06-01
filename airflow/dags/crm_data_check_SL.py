from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'crm_data_check_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='FAA check crm node', 
        html_content='Run started {{ var.value.workflow_run_id }}',
        dag=dag 
    ) 

    list_number_of_crm_files = BashOperator(
        task_id='list_number_of_crm_files',
        bash_command="ls /backup/TO_FAA/input/ | grep RTD_CUST | wc -l",
        do_xcom_push = True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    def check_count_of_crm_files(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='list_number_of_crm_files')
        if '0' in value:
            return 'crm_data_not_found'
        else:
            return 'list_crm_files'

    check_count_of_crm_files_task = BranchPythonOperator(
        task_id='check_count_of_crm_files_task',
        python_callable=check_count_of_crm_files,
        dag=dag
    )

    list_crm_files = BashOperator(
        task_id='list_crm_files',
        bash_command="ls /backup/TO_FAA/input | grep RTD_CUST | tr '\n' ,",
        do_xcom_push = True,
        dag = dag
    )

    crm_data_found = EmailOperator(
        task_id='crm_data_found', 
        to='tahamiri02@gmail.com', 
        subject='FAA crm check', 
        html_content="crm data found following files {{ task_instance.xcom_pull(task_ids='list_crm_files') }}",
        dag=dag
    )

    crm_data_not_found = EmailOperator(
        task_id='crm_data_not_found', 
        to='tahamiri02@gmail.com', 
        subject='FAA crm check', 
        html_content='Warning, no crm data found',
        dag=dag
    )


    send_email0 >> list_number_of_crm_files >> check_count_of_crm_files_task
    check_count_of_crm_files_task >> list_crm_files >> crm_data_found
    check_count_of_crm_files_task >> crm_data_not_found 