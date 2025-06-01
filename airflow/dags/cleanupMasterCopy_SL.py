from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.models import Variable


default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'CleanUpMasterCopy_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='FAA cleanup master copy folder', 
        html_content='Cleanup mastercopy started {{ var.value.workflow_run_id }}' 
    ) 

    clean_up_master_copy = BashOperator(
    task_id='clean_up_master_copy',
    bash_command='bash /backup/TO_FAA/cleanupmastercopy.sh ',
    do_xcom_push = True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
    )

    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com', 
        subject='FAA cleanup master copy folder', 
        html_content='Cleanup mastercopy finished {{ var.value.workflow_run_id }}' 
    ) 

    send_email0 >> clean_up_master_copy >> send_email1