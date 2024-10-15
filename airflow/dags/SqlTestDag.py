from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'taha',
    'start_date': datetime(2024, 3, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id = 'sql_test',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:

    sql_task = PostgresOperator( 
        task_id='sql_task', 
        sql="SELECT * FROM data.check_operator_own_name('{{ var.value.operator_own_name }}');",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    ) 
    


    


    

#task = PostgresOperator(
#    task_id='example_task',
#    sql='SELECT * FROM your_table;',
#    postgres_conn_id='greenplum_conn',
#    dag=dag,
#)