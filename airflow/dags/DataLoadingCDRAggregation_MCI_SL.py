from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import re



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'DataLoadingCDRAggregation_MCI_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    

    check_ready_for_aggregation = PostgresOperator(
        task_id='check_ready_for_aggregation',
        sql="SELECT data.check_ready_for_aggregation()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aggregate_check = PostgresOperator(
        task_id='aggregate_check',
        sql="SELECT data.cdr_aggregation_validate()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    def aggregate_check_result_function(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='aggregate_check')
        if value[0][0] == 1:
            return 'set_start_and_end_date'
        else:
            return 'aggregate_check_zero_result'
    
    aggregate_check_result = BranchPythonOperator(
        task_id='aggregate_check_result',
        python_callable=aggregate_check_result_function,
        dag=dag
    )

    def aggregate_check_zero_result_function():
        logging.error("Aggregation not passed")

    aggregate_check_zero_result = PythonOperator(
        task_id='aggregate_check_zero_result',
        python_callable=aggregate_check_zero_result_function,
        dag=dag
    )

    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com', 
        subject='FAA aggregation node', 
        html_content='Aggregate check not passed {{ var.value.workflow_run_id }}'
    )


    set_start_and_end_date = PostgresOperator(
        task_id='set_start_and_end_date',
        sql="select max(prev_agg_day)+1 as start_date, max(prev_agg_day) + 8 as end_date from tmp.validate_aggregation_results;",
        postgres_conn_id='greenplum_conn',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    send_email2 = EmailOperator( 
        task_id='send_email2', 
        to='tahamiri02@gmail.com', 
        subject='FAA aggregation node', 
        html_content="agg fullweeks started {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][0]}} {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][1]}}"
    )


    tmp_cdr_full_weeks_optimized = PostgresOperator(
        task_id='tmp_cdr_full_weeks_optimized',
        sql="""
            select data.cdr_full_weeks_optimized(
                    '{{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][0]}}'::timestamp without time zone,
                    '{{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][1]}}'::timestamp without time zone
                );
            """,
        postgres_conn_id='greenplum_conn',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )


    aliases_network_cdr =  PostgresOperator(
        task_id='aliases_network_cdr',
        sql="select data.aliases_network_cdr();",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )



    send_email3 = EmailOperator( 
        task_id='send_email3', 
        to='tahamiri02@gmail.com', 
        subject='FAA aggregation node', 
        html_content="agg calltypesweekly started {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][0]}} {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][1]}}"
    )


    data_call_types_weekly =  PostgresOperator(
        task_id='data_call_types_weekly',
        sql="select data.call_types_weekly();",
        postgres_conn_id='greenplum_conn',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )


    aliases_update =  PostgresOperator(
        task_id='aliases_update',
        sql="select aliases.update();",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )



    send_email4 = EmailOperator( 
        task_id='send_email4', 
        to='tahamiri02@gmail.com', 
        subject='FAA aggregation node', 
        html_content="agg insplitweekly started {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][0]}} {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][1]}}"
    )



    in_split_weekly =  PostgresOperator(
        task_id='in_split_weekly',
        sql="select data.in_split_weekly();",
        postgres_conn_id='greenplum_conn',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )


    weekly_cdr_aggregates =  PostgresOperator(
        task_id='weekly_cdr_aggregates',
        sql="select * from data.weekly_cdr_aggregates_wrapper();",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    send_email5 = EmailOperator( 
        task_id='send_email5', 
        to='tahamiri02@gmail.com', 
        subject='FAA aggregation node', 
        html_content="weekly agg completed for {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][0]}} {{task_instance.xcom_pull(task_ids='set_start_and_end_date')[0][1]}}"
    )


    check_ready_for_aggregation >> aggregate_check >> aggregate_check_result
    aggregate_check_result >> aggregate_check_zero_result >> send_email1
    aggregate_check_result >> set_start_and_end_date >> send_email2 >> tmp_cdr_full_weeks_optimized >> aliases_network_cdr >> send_email3 >> data_call_types_weekly >> aliases_update >> send_email4 >> in_split_weekly >> weekly_cdr_aggregates >> send_email5



