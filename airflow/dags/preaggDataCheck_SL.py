from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task




default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}



def check_xcom(**kwargs):
    
    logging.info(kwargs['ti'].xcom_pull(task_ids='last_preagg_date2', key='return_value'))
    
    if kwargs['ti'].xcom_pull(task_ids='last_preagg_date2', key='return_value')[0][0] == 'true':
        return 'preagg_data_not_found'
    else:
        return 'end'


with DAG(
    dag_id = 'PreAgg_data_check_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    
    last_preagg_date2 = PostgresOperator(
        task_id='last_preagg_date2',
        sql="SELECT case when DATE_PART('day', now()::timestamp - (select max(data_date)::timestamp from data.processed_data where data_type like '%pre_aggregates%')) > 30 then 'true' else 'false' end as test;",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )
    
    check_xcom_data = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_xcom
    )
    


    end = EmptyOperator(task_id='end')



    preagg_data_not_found = EmailOperator( 
        task_id='preagg_data_not_found', 
        to='tahamiri02@gmail.com', 
        subject='FAAPREAGG CHECK', 
        html_content='Preaggreagates data is more than 30 days old, please advise BI team to produce new data {{ var.value.workflow_run_id}}' 
    )


    last_preagg_date2 >> check_xcom_data >> [preagg_data_not_found, end]
    