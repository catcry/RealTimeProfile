from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'tahoora',
    'start_date': datetime(2024, 3, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=59),
}


with DAG(
    dag_id = 'DataLoadingCommonProlog',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    network_list = PostgresOperator(
        task_id='network_list',
        sql="SELECT * FROM data.network_list('{{ var.value.operator_own_name }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_crm_staging = PostgresOperator(
        task_id='tmp_crm_staging',
        sql="SELECT data.tmp_crm_staging_new('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_string_id_crm = PostgresOperator(
        task_id='aliases_string_id_crm',
        sql="SELECT data.aliases_string_id_crm()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_network_crm = PostgresOperator(
        task_id='aliases_network_crm',
        sql="SELECT data.aliases_network_crm()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    tmp_cdr_staging = PostgresOperator(
        task_id='tmp_cdr_staging',
        sql="SELECT data.tmp_cdr_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    network_list_cdr = PostgresOperator(
        task_id='network_list_cdr',
        sql="SELECT data.network_list_cdr()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_string_id_cdr = PostgresOperator(
        task_id='aliases_string_id_cdr',
        sql="SELECT data.aliases_string_id_cdr()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_topup_staging = PostgresOperator(
        task_id='tmp_topup_staging',
        sql="SELECT data.tmp_topup_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    aliases_string_id_topup = PostgresOperator(
        task_id='aliases_string_id_topup',
        sql="SELECT data.aliases_string_id_topup()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_network_topup = PostgresOperator(
        task_id='aliases_network_topup',
        sql="SELECT data.aliases_network_topup()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_product_takeup_staging = PostgresOperator(
        task_id='tmp_product_takeup_staging',
        sql="SELECT data.tmp_product_takeup_staging_new('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portout_ongoing_staging = PostgresOperator(
        task_id='tmp_portout_ongoing_staging',
        sql="SELECT data.tmp_portout_ongoing_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portout_notported_staging = PostgresOperator(
        task_id='tmp_portout_notported_staging',
        sql="SELECT data.tmp_portout_notported_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portin_ported_staging = PostgresOperator(
        task_id='tmp_portin_ported_staging',
        sql="SELECT data.tmp_portin_ported_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portin_ongoing_staging = PostgresOperator(
        task_id='tmp_portin_ongoing_staging',
        sql="SELECT data.tmp_portin_ongoing_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portin_notported_staging = PostgresOperator(
        task_id='tmp_portin_notported_staging',
        sql = "SELECT data.tmp_portin_notported_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_portablity_staging = PostgresOperator(
        task_id='tmp_portablity_staging',
        sql="SELECT data.tmp_portability_staging()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_string_id_portability = PostgresOperator(
        task_id='aliases_string_id_portability',
        sql="SELECT data.aliases_string_id_portability()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_network_portability = PostgresOperator(
        task_id='aliases_network_portability',
        sql="SELECT data.aliases_network_portability()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_customer_care_staging = PostgresOperator(
        task_id='tmp_customer_care_staging',
        sql="SELECT data.tmp_customer_care_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_string_id_customer_care = PostgresOperator(
        task_id = 'aliases_string_id_customer_care',
        sql="SELECT data.aliases_string_id_customer_care()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    tmp_pre_aggregates_staging = PostgresOperator(
        task_id='tmp_pre_aggregates_staging',
        sql="SELECT data.tmp_pre_aggregates_staging('{{ var.value.workflow_run_id }}')",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    aliases_string_id_pre_aggregates = PostgresOperator(
        task_id='aliases_string_id_pre_aggregates',
        sql="SELECT data.aliases_string_id_pre_aggregates()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    network_list >>  tmp_crm_staging >> aliases_string_id_crm >> aliases_network_crm >> tmp_cdr_staging >> network_list_cdr >> aliases_string_id_cdr >> tmp_topup_staging >> aliases_string_id_topup >> aliases_network_topup >> tmp_product_takeup_staging >> tmp_portout_ongoing_staging >> tmp_portout_notported_staging >> tmp_portin_ported_staging >> tmp_portin_ongoing_staging >> tmp_portin_notported_staging >> tmp_portablity_staging >> aliases_string_id_portability >> aliases_network_portability >> tmp_customer_care_staging >> aliases_string_id_customer_care >> tmp_pre_aggregates_staging >> aliases_string_id_pre_aggregates