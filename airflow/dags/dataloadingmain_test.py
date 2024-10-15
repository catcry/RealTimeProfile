from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import logging


default_args = {
    'owner': 'taha and tahoora',
    'start_date': datetime(2024, 3, 16),
    'retries': 0,
    'retry_delay': timedelta(minutes=600),
}


with DAG(
    dag_id = 'DataLoadingMain_test',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    check_operator_own_name = PostgresOperator( 
        task_id='check_operator_own_name', 
        sql="SELECT * FROM data.check_operator_own_name('{{ var.value.operator_own_name }}');",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    ) 

    run_data_loader = BashOperator(
        task_id='run_data_loader',
        bash_command='ssh gpadmin@192.168.5.230 "rm -rf /home/gpadmin/gp_analyze/dataloading/database_loader_*.log && cp -r ~/input_bkp/input ~/gp_analyze/TO_FAA/ && nohup bash /home/gpadmin/gp_analyze/dataloading/database_loader.sh"',
        do_xcom_push = True,
        dag=dag
    )


    def check_value(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='run_data_loader')
        if value == 5432:
            logging.info("Value is 10. Not found.")
            print("Not found", value)
            raise ValueError("Value is 10. Terminating the flow.")
        elif value == '5432':
            print("Found")
        else:
            print("Value neither 10 nor 12")
            print(value)


    check_result = PythonOperator(
        task_id='check_result',
        python_callable=check_value,
        provide_context=True,
        dag=dag,
    )


    check_operator_own_name >> run_data_loader >> check_result