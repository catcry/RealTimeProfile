from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'tahoora',
    'start_date': datetime(2024, 4, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'Run_Mci_sql',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    
    run_data_loader = BashOperator(
    task_id='create_database',
    bash_command='ssh gpadmin@192.168.5.230 "bash /home/gpadmin/gp_analyze/dataloading/scripts/run_mci_sql.sh"',
    do_xcom_push = True,
    dag=dag
    )

    run_data_loader

