from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'DataLoadingLookUp_MCI_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    
    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI lookup Dataloader node', 
        html_content='Run lookup dataloader started with workflow_run_id of {{ var.value.workflow_run_id }}' 
    )


    run_data_loader = BashOperator(
        task_id='run_data_loader',
        bash_command='ssh gpadmin@192.168.5.231 "nohup bash /home/gpadmin/dataloading/database_loader_lookup.sh {{ var.value.workflow_run_id }}"',
        do_xcom_push = True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    data_status_reason = PostgresOperator(
        task_id='data_status_reason',
        sql="SELECT data.status_reason()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    data_disconnection_reason = PostgresOperator(
        task_id='data_disconnection_reason',
        sql="SELECT data.disconnection_reason()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    data_product_bonus = PostgresOperator(
        task_id='data_product_bonus',
        sql="SELECT data.product_bonus_new()",
        postgres_conn_id='greenplum_conn',
        dag=dag 
    )

    data_product_discount = PostgresOperator(
        task_id='data_product_discount',
        sql="SELECT data.product_discount_new()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    data_device_tac = PostgresOperator(
        task_id='data_device_tac',
        sql="SELECT data.device_tac()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI lookup Dataloader node', 
        html_content='Run dataloader finished with workflow_run_id of {{ var.value.workflow_run_id }}' 
    )



    send_email0 >> run_data_loader >> data_status_reason >> data_disconnection_reason >> data_product_bonus >> data_product_discount >> data_device_tac >> send_email1