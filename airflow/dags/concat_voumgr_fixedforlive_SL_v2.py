from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
import logging
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 9, 18),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'concat_voumgr_fixedforlive_SL_v2',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='FAA concat voumgr node', 
        html_content='Run started with workflow run id of {{ var.value.workflow_run_id }}' 
    )

    move_delayed_files = BashOperator(
        task_id="move_delayed_files",
        bash_command='bash {{ var.value.rtdmed1path }}/movelatefilesback.sh {{ var.value.rtdmed1path }} $(date +%Y%m%d) $(date -d yesterday +%Y%m%d)',
        dag=dag
    )

    concatenate = BashOperator(
        task_id='concatenate',
        bash_command='bash {{ var.value.rtdmed1path }}/cat_vou_mgr_paramterized.sh {{ var.value.rtdmed1path }} {{ var.value.rtdmed1path }} $(date -d yesterday +%Y%m%d)',
        dag=dag
    )

    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com', 
        subject='FAA concat voumgr node', 
        html_content='Run finished with workflow_run_id of {{ var.value.workflow_run_id }}',
    )

    

    send_email0 >> move_delayed_files >> concatenate >> send_email1
    

