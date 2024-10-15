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


with DAG(
    dag_id = 'DataLoadingMain_Mci_v2',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='Run dataloader started with workflow_run_id of {{ var.value.workflow_run_id }}' 
    )

    file_count = PostgresOperator( 
        task_id='file_count', 
        sql="""
            select count(a.count) from
            (select count(source_file)
             from tmp.topup
             group by source_file
             union
             select count(source_file)
             from tmp.cdr
             group by source_file
             union
             select count(source_file)
             from tmp.customer_care
             group by source_file) as a
            ;
        """,
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    send_email0


# {{ ti.xcom_pull(task_ids="", key="") }}