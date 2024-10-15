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
    dag_id = 'DataLoadingTopup_Mci',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    create_product_takeup_partitions = PostgresOperator( 
        task_id='create_product_takeup_partitions', 
        sql="SELECT core.create_product_takeup_partitions()",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    data_product_takeup = PostgresOperator(
        task_id='data_product_takeup',
        sql="SELECT data.product_takeup()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )