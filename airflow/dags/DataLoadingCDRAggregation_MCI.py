from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
import re
from airflow.models import Variable
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'taha',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}




with DAG(
    dag_id = 'DataLoadingCDRAggregation_MCI',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    check_ready_for_aggregation = PostgresOperator(
        task_id='check_ready_for_aggregation',
        sql="""
            select data.check_ready_for_aggregation();
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aggregate_check = PostgresOperator(
        task_id='aggregate_check',
        sql="""
            select data.cdr_aggregation_validate();
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aggregate_check = PostgresOperator(
        task_id='aggregate_check',
        sql="""
            select data.cdr_aggregation_validate();
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    