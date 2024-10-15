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
    dag_id = 'DataLoadingCDROptimized_Mci',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    create_cdr_partitions = PostgresOperator( 
        task_id='create_cdr_partitions', 
        sql="select core.create_cdr_partitions();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    data_cdr = PostgresOperator( 
        task_id='data_cdr', 
        sql="select data.cdr();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    aliases_update = PostgresOperator( 
        task_id='aliases_update', 
        sql="select aliases.update();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )


    create_cdr_partitions >> data_cdr >> aliases_update