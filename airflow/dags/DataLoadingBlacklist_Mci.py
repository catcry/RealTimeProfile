from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'taha',
    'start_date': datetime(2024, 3, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id = 'DataLoadingBlacklist_Mci',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    tmp_blacklist_staging = PostgresOperator( 
        task_id='tmp_blacklist_staging', 
        sql="select data.tmp_blacklist_staging('{{ var.value.workflow_run_id }}');",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    data_blacklist = PostgresOperator( 
        task_id='data_blacklist', 
        sql="select data.blacklist();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )


    tmp_blacklist_staging >> data_blacklist