from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'DataLoadingMain_MCI_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='Run dataloader started with workflow_run_id of {{ var.value.workflow_run_id }}' 
    )

    check_operator_own_name = PostgresOperator( 
        task_id='check_operator_own_name', 
        sql="SELECT * FROM data.check_operator_own_name('{{ var.value.operator_own_name }}');",
        postgres_conn_id='greenplum_conn', 
        dag=dag 
    )

    run_data_loader = BashOperator(
    task_id='run_data_loader',
    bash_command='ssh gpadmin@192.168.5.231 "nohup bash /home/gpadmin/dataloading/database_loader.sh {{ var.value.workflow_run_id }}"',
    do_xcom_push = True,
    dag=dag
    )


    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='Run Dataloader node completed for {{ var.value.workflow_run_id }}' 
    )


    with TaskGroup("DataLoadingCommonProlog_MCI", tooltip="DataLoadingCommonProlog_MCI") as DataLoadingCommonProlog_MCI:
        
        network_list = PostgresOperator(
            task_id='network_list',
            sql="SELECT data.network_list('{{ var.value.operator_own_name }}')",
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

        aliases_string_id_product_takeup = PostgresOperator(
            task_id='aliases_string_id_product_takeup',
            sql="SELECT data.aliases_string_id_product_takeup()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )
        

        tmp_portout_ported_staging = PostgresOperator(
            task_id='tmp_portout_ported_staging',
            sql="SELECT data.tmp_portout_ported_staging('{{ var.value.workflow_run_id }}')",
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
            sql="SELECT data.tmp_portin_notported_staging('{{ var.value.workflow_run_id }}')",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )


        tmp_portability_staging = PostgresOperator(
            task_id='tmp_portability_staging',
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
            task_id='aliases_string_id_customer_care',
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


        network_list >> tmp_crm_staging >> aliases_string_id_crm >> aliases_network_crm >> tmp_cdr_staging >> network_list_cdr >> aliases_string_id_cdr >> tmp_topup_staging >> aliases_string_id_topup >> aliases_network_topup >> tmp_product_takeup_staging >> aliases_string_id_product_takeup >> tmp_portout_ported_staging >> tmp_portout_ongoing_staging >> tmp_portout_notported_staging >> tmp_portin_ported_staging >> tmp_portin_ongoing_staging >> tmp_portin_notported_staging >> tmp_portability_staging >> aliases_string_id_portability >> aliases_network_portability >> tmp_customer_care_staging >> aliases_string_id_customer_care >> tmp_pre_aggregates_staging >> aliases_string_id_pre_aggregates


    send_email2 = EmailOperator( 
        task_id='send_email2', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='commonprologfinished for {{ var.value.workflow_run_id }}' 
    )

    with TaskGroup("DataLoadingCRM_MCI", tooltip="DataLoadingCRM_MCI") as DataLoadingCRM_MCI:

        create_crm_partitions = PostgresOperator(
            task_id='create_crm_partitions',
            sql="select core.create_crm_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_crm = PostgresOperator(
            task_id='data_crm',
            sql="select data.crm_new()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_in_crm = PostgresOperator(
            task_id='data_in_crm',
            sql="select data.in_crm()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        
        create_crm_partitions >> data_crm >> data_in_crm

    send_email3 = EmailOperator( 
        task_id='send_email3', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='stage crm finished for {{ var.value.workflow_run_id }}' 
    )

    with TaskGroup("DataLoadingBlacklist_MCI", tooltip="DataLoadingBlacklist_MCI") as DataLoadingBlacklist_MCI:

        tmp_blacklist_staging = PostgresOperator(
            task_id='tmp_blacklist_staging',
            sql="select data.tmp_blacklist_staging('{{ var.value.workflow_run_id }}')",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_blacklist = PostgresOperator(
            task_id='data_blacklist',
            sql="select data.blacklist()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        tmp_blacklist_staging >> data_blacklist


    with TaskGroup("DataLoadingCDROptimized_MCI", tooltip="DataLoadingCDROptimized_MCI") as DataLoadingCDROptimized_MCI:

        create_cdr_partitions = PostgresOperator(
            task_id='create_cdr_partitions',
            sql="select core.create_cdr_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_cdr = PostgresOperator(
            task_id='data_cdr',
            sql="select data.cdr()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        aliases_update = PostgresOperator(
            task_id='aliases_update',
            sql="select aliases.update()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        create_cdr_partitions >> data_cdr >> aliases_update


    send_email4 = EmailOperator( 
        task_id='send_email4', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='stage cdr finished for {{ var.value.workflow_run_id }}' 
    )


    with TaskGroup("DataLoadingTopup_MCI", tooltip="DataLoadingTopup_MCI") as DataLoadingTopup_MCI:
        
        create_topup_partitions = PostgresOperator(
            task_id='create_topup_partitions',
            sql="select core.create_topup_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        aliases_network_topup = PostgresOperator(
            task_id='aliases_network_topup',
            sql="select data.aliases_network_topup()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_topup = PostgresOperator(
            task_id='data_topup',
            sql="select data.topup()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        create_topup_partitions >> aliases_network_topup >> data_topup
    


    with TaskGroup("DataLoadingProductTakeup_MCI", tooltip="DataLoadingProductTakeup_MCI") as DataLoadingProductTakeup_MCI:
            

        create_product_takeup_partitions = PostgresOperator(
            task_id='create_product_takeup_partitions',
            sql="select core.create_product_takeup_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )


        data_product_takeup = PostgresOperator(
            task_id='data_product_takeup',
            sql="select data.product_takeup()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )
    
    
        create_product_takeup_partitions >> data_product_takeup
    


    with TaskGroup("DataLoadingPortability_MCI", tooltip="DataLoadingPortability_MCI") as DataLoadingPortability_MCI:

        create_portability_partitions = PostgresOperator(
            task_id='create_portability_partitions',
            sql="select core.create_portability_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_portability = PostgresOperator(
            task_id='data_portability',
            sql="select data.portability()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        
        port_out_requests = PostgresOperator(
            task_id='port_out_requests',
            sql="select data.port_out_requests()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )


        create_portability_partitions >> data_portability >> port_out_requests




    with TaskGroup("DataLoadingCustomerCare_MCI", tooltip="DataLoadingCustomerCare_MCI") as DataLoadingCustomerCare_MCI:
    
        create_customer_care_partitions = PostgresOperator(
            task_id='create_customer_care_partitions',
            sql="select core.create_customer_care_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )


        data_customer_care = PostgresOperator(
            task_id='data_customer_care',
            sql="select data.customer_care()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )


        create_customer_care_partitions >> data_customer_care


    with TaskGroup("DataLoadingPreAggregates_MCI", tooltip="DataLoadingPreAggregates_MCI") as DataLoadingPreAggregates_MCI:

        create_pre_aggregates_partitions = PostgresOperator(
            task_id='create_pre_aggregates_partitions',
            sql="select core.create_pre_aggregates_partitions()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        data_pre_aggregates = PostgresOperator(
            task_id='data_pre_aggregates',
            sql="select data.pre_aggregates()",
            postgres_conn_id='greenplum_conn',
            dag=dag
        )

        create_pre_aggregates_partitions >> data_pre_aggregates 


    send_email5 = EmailOperator( 
        task_id='send_email5', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node', 
        html_content='all stagng finished for {{ var.value.workflow_run_id }}' 
    )


    aliases_update = PostgresOperator(
        task_id='aliases_update',
        sql="select aliases.update()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    data_quality = PostgresOperator(
        task_id='data_quality',
        sql="""
            truncate tmp.data_quality;

            insert into tmp.data_quality (
            select * from data.data_quality
            );

            truncate data.data_quality;

            insert into data.data_quality (data_source, data_date, status, error_count_preloading, error_count_aggregate, error_count_other, error_count_preloading_by_row)
            (
            select data_type, data_date, max(status), sum(error_count_preloading), sum(error_count_aggregate), sum(error_count_other), sum(error_count_preloading_by_row)
            from
            (
            (
            select data_type, data_date, 2 as status, 0 as error_count_preloading, 0 as error_count_aggregate, 0 as error_count_other, 0 as error_count_preloading_by_row from data.processed_data where dataset_id='{{ var.value.workflow_run_id}}'
            )
            union all
            (
            select
            data_type,
            data_date,
            Max(case when severity='WARNING' then 3
            when severity='CRITICAL' then 4
            else -1 --unknown
            end
            ) as status,
            SUM( case when error_code::int between 10000 and 19999 then 1 else 0 end) as error_count_preloading,
            SUM(case when error_code::int between 20000 and 29999 then 1 else 0 end) as error_count_aggregate,
            SUM( case when error_code::int NOT between 10000 and 29999 then 1 else 0 end) as error_count_other,
            (case when SUM(case when error_code::int between 10000 and 19999 then 1 else 0 end) > 0 then 1 else 0 end) as error_count_preloading_by_row
            from tmp.validation_errors group by data_type, data_date, file_row_num
            )

            )b

            group by data_type, data_date
            );

            insert into data.data_quality
            select a.* from tmp.data_quality a
            left outer join data.data_quality b
            on a.data_source = b.data_source
            and a.data_date = b.data_date
            where b.data_source is null;
            """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    create_errors_partitions = PostgresOperator(
        task_id='create_errors_partitions',
        sql="select core.create_validation_errors_partitions()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    validation_errors = PostgresOperator(
        task_id='validation_errors',
        sql="""
            -- INSERT NEW DATA
            insert into data.validation_errors
            (
                data_date,
                data_type,
                file_short_name,
                file_row_num,
                file_column_num,
                error_code,
                error_desc,
                file_full_row,
                severity
            )
            (
            select
                data_date,
                data_type,
                file_short_name,
                file_row_num,
                file_column_num,
                error_code,
                error_desc,
                file_full_row,
                severity
            from tmp.validation_errors
            );
            """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )



    def check_perform_cleanup_value(**kwargs):
        value = Variable.get("perform_cleanup")
        logging.info(f"============================================================================================ {value}")
        if value == "True" or value is True:
            return 'cleanup_old_partitions'
        else:
            return 'send_email6'


    check_perform_cleanup = BranchPythonOperator(
        task_id='check_perform_cleanup',
        python_callable=check_perform_cleanup_value,
        dag=dag
    )



    cleanup_old_partitions = PostgresOperator(
        task_id='cleanup_old_partitions',
        sql="select core.cleanup_partitions()",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    send_email6 = EmailOperator( 
        task_id='send_email6', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node',
        html_content='Dataloading compilited for master_copy in FAA MCI Installation for {{ var.value.workflow_run_id }}' 
    )


    send_email7 = EmailOperator( 
        task_id='send_email7', 
        to='tahamiri02@gmail.com', 
        subject='Fastermind MCI Dataloader node',
        html_content='Dataloading compilited for master_copy in FAA MCI Installation for {{ var.value.workflow_run_id }}' 
    )

    



    send_email0 >> check_operator_own_name >> run_data_loader >> send_email1 >> DataLoadingCommonProlog_MCI >> send_email2 >> DataLoadingCRM_MCI >> send_email3 >> DataLoadingBlacklist_MCI >> DataLoadingCDROptimized_MCI >> send_email4 >> DataLoadingTopup_MCI >> DataLoadingProductTakeup_MCI >> DataLoadingPortability_MCI >> DataLoadingCustomerCare_MCI >> DataLoadingPreAggregates_MCI >> send_email5 >> aliases_update >> data_quality >> create_errors_partitions >> validation_errors >> check_perform_cleanup
    check_perform_cleanup >> cleanup_old_partitions >> send_email7
    check_perform_cleanup >> send_email6
    





# {{ ti.xcom_pull(task_ids="", key="") }}



