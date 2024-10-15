from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'BI_DAG',
    default_args=default_args,
    description='for daily loading bi data in greenplum and couchbase.',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='Daily Airflow Report', 
        html_content='loading bi data started with workflow_run_id of {{ var.value.workflow_run_id }}' 
    ) 


    data_loader_gp = BashOperator(
        task_id='data_loader_gp',
        bash_command='ssh gpadmin@192.168.5.230 "cp -r ~/input_bkp/input ~/gp_analyze/TO_FAA/ && nohup bash /home/gpadmin/gp_analyze/dataloading/database_loader.sh"',
        do_xcom_push = True,
        dag=dag
    )


    check_operator_own_name = PostgresOperator( 
        task_id='check_operator_own_name', 
        sql="SELECT * FROM data.check_operator_own_name('{{ var.value.operator_own_name }}');",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    ) 



