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
    create_topup_partitions = PostgresOperator( 
        task_id='create_topup_partitions', 
        sql="select core.create_topup_partitions();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    aliases_network_topup = PostgresOperator( 
        task_id='aliases_network_topup', 
        sql="select core.create_topup_partitions();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    data_topup = PostgresOperator( 
        task_id='data_topup', 
        sql="select data.topup();",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )


    create_topup_partitions >> aliases_network_topup >> data_topup