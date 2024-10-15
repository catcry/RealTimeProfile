from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable



default_args = {
    'owner': 'tahoora',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'concat_voumgr_fixedforlive',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='FAA concat voumgr node', 
        html_content='Run started with workflow_run_id of {{ var.value.workflow_run_id }}' 
    ) 

    move_delayed_files = BashOperator(
        task_id="move_delayed_files",
        bash_command='ssh gpadmin@192.168.5.230 "bash {{ var.value.rtdmed1path }}/movelatefilesback.sh {{ var.value.rtdmed1path }} \$(date +"\%Y\%m\%d") \$(date -d yesterday +"\%Y\%m\%d")"',
        do_xcom_push = True,
        dag=dag
    )

    def check_exit_status(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='move_delayed_files')
        if value != 'completed':
            logging.info("Error in the move")
            raise ValueError("Error in the move")

    check_exit_status_task = PythonOperator(
        task_id='check_exit_status_task',
        python_callable=check_exit_status,
        provide_context=True,
        dag=dag
    )

    concatenate = BashOperator(
        task_id='concatenate',
        bash_command='ssh gpadmin@192.168.5.230 "bash {{ var.value.rtdmed1path }}/cat_vou_mgr_paramterized.sh {{ var.value.rtdmed1path }} \$(date -d yesterday +"\%Y\%m\%d")"'
    )

    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahooramajlesi@gmail.com', 
        subject='FAA concat voumgr node', 
        html_content='Run finished with workflow_run_id of {{ var.value.workflow_run_id }}' 
    )

    send_email0 >> move_delayed_files >> check_exit_status_task >> concatenate >> send_email1