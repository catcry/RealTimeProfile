from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator



default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'ScoringFlow_MCI_organized_nochart_SL',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    
    sleep_task = BashOperator(
        task_id='sleep_task',
        bash_command='sleep 3'
    )

    Info_started_scoring = PostgresOperator(
        task_id='Info_started_scoring',
        sql="insert into core_runbean_attributes (runbean_id , attributes_key, attributes) values ('{{ run_id }}', 'checkpoint', 'Started scoring');",
        postgres_conn_id='metaDB',
        dag=dag
    )

    send_email0 = EmailOperator( 
        task_id='send_email0', 
        to='tahamiri02@gmail.com', 
        subject='FAA Scoring run',
        html_content='scoring run started {{ var.value.t2 }}' 
    )

    def Init_parameters_for_scoring_function():
        variables_dict = {
        "calculate_vargroup_output": "false",
        "initialize_desc_var": "false",
        "keys": "",
        "model_fitting":"false",
        "override_model_varables_check":"true",
        "redefine_preprocessing":"false",
        "uc_churn_inactivity_include_postpaid":"true",
        "uc_churn_inactivity_model_id":"14",
        "uc_churn_postpaid_model_id":"-1",
        "uc_postpaid_include_postpaid":"true",
        "uc_portout_model_id":"13",
        "uc_product_model_id":"-1",
        "uc_zero_day_predicition_model_id":"-1",
        "uc_zero_day_predicition_redefine_value_segments":"false",
        "vals":""
        }

        for key, value in variables_dict.items():  

            Variable.set(key, value)

    Init_parameters_for_scoring = PythonOperator(
        task_id='Init_parameters_for_scoring',
        python_callable=Init_parameters_for_scoring_function,
        dag=dag
    )


    """
    uc_include_product
    uc_include_churn_inacticity
    uc_include_churn_postpaid
    uc_include_portout
    uc_include_zero_day_prediction
    """
    Check_use_cases = PostgresOperator(
        task_id='Check_use_cases',
        sql="select active::text from core_categorybean where name_ in ('Best next product prediction', 'Churn inactivity prediction', 'Churn postpaid prediction', 'Zero day prediction', 'Port Out Prediction') order by name_",
        postgres_conn_id='metaDB',
        dag=dag
    )

    Check_fitted_model = PostgresOperator(
        task_id='Check_fitted_model',
        sql="select * from work.check_fitted_model_availability({{ var.value.model_fitting }});",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )


    def split_check_fitted_model(**kwargs):
        xcom_value = kwargs['ti'].xcom_pull(task_ids='Check_fitted_model')
        #logging.info(type(xcom_value[0][0]))
        if xcom_value[0][0] is True:
            return 'Info_Fitting_Models'
        else:
            return 'Info_Applying_Models'


    branching_based_on_fitted_model = BranchPythonOperator(
        task_id='branching_based_on_fitted_model',
        python_callable=split_check_fitted_model,
        dag=dag
    )

    Info_Fitting_Models = PostgresOperator(
        task_id='Info_Fitting_Models',
        sql="update core_runbean_attributes set attributes = 'Fitting model on historical data' where runbean_id = '{{ run_id }}' and attributes_key = 'checkpoint';",
        postgres_conn_id='metaDB',
        dag=dag
    )


    def Init_parameters_for_fit_function():
        variables_dict = {
        "descvar_interval_weeks": "-1",
        "lda_cell_events_input_id": "-1",
        "lda_cell_events_model_options": "No LDA run",
        "lda_cell_events_output_id_old":"-1",
        "lda_input_id":"-1",
        "lda_input_options":"No LDA run",
        "lda_model_options":"No LDA run",
        "lda_output_id_old":"-1",
        "mod_job_id":"-1",
        "redefine_preprocessing":"true",
        "run_descvar":"Do not run",
        "run_fitting":"true",
        "run_type":"Predictors + Fit + Apply",
        "t2":"t2"
        }

        for key, value in variables_dict.items():  

            Variable.set(key, value)

    Init_parameters_for_fit = PythonOperator(
        task_id='Init_parameters_for_fit',
        python_callable=Init_parameters_for_fit_function,
        dag=dag
    )



    with TaskGroup("Run_MCI", tooltip="Run_MCI") as Run_MCI:
        dummy = EmptyOperator(task_id="dummy", dag=dag)



    send_email1 = EmailOperator( 
        task_id='send_email1', 
        to='tahamiri02@gmail.com',
        subject='FAA Scoring run',
        html_content='fitting completed for {{ var.value.t2 }}' 
    )



    Info_Applying_Models = PostgresOperator(
        task_id='Info_Applying_Models',
        sql="update core_runbean_attributes set attributes = 'Applying model on historical data' where runbean_id = '{{ run_id }}' and attributes_key = 'checkpoint';",
        postgres_conn_id='metaDB',
        trigger_rule='none_failed_min_one_success',
        dag=dag
    )


    def Init_parameters_for_apply_function():
        variables_dict = {
        "lda_cell_events_input_id": "-1",
        "lda_cell_events_output_id_old": "-1",
        "lda_input_id": "-1",
        "lda_output_id_old":"-1",
        "mod_job_id":"-1",
        "lda_cell_events_input_options":"No LDA run",
        "lda_cell_events_model_options":"No LDA run",
        "lda_input_options":"No LDA run",
        "lda_model_options":"No LDA run",
        "run_type":"Predictors + Apply",
        "run_descvar":"false",
        "t2":"t2"
        }

        for key, value in variables_dict.items():  

            Variable.set(key, value)

    Init_parameters_for_apply = PythonOperator(
        task_id='Init_parameters_for_apply',
        python_callable=Init_parameters_for_fit_function,
        dag=dag
    )



    with TaskGroup("Run_MCI2", tooltip="Run_MCI2") as Run_MCI2:
        def Init_Datasource1_function():
            Variable.set("DataSource1", "greenplum_conn")

        Init_Datasource1 = PythonOperator(
            task_id='Init_Datasource1',
            python_callable=Init_Datasource1_function,
            dag=dag
        )

        with TaskGroup("Run_Initialize_MCI", tooltip="Run_Initialize_MCI") as Run_Initialize_MCI:
        
            Initialize_generic_parameters = PostgresOperator(
                task_id='Initialize_generic_parameters',
                sql="update core_runbean_attributes set attributes = 'Applying model on historical data' where runbean_id = '{{ run_id }}' and attributes_key = 'checkpoint';",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )
            
            Initialize_generic_parameters



            
        Init_Datasource1 >> Run_Initialize_MCI



    send_email2 = EmailOperator( 
        task_id='send_email2', 
        to='tahamiri02@gmail.com', 
        subject='FAA Scoring run',
        html_content='apply completed for {{ var.value.t2 }}' 
    )

    


    


    
sleep_task >> Info_started_scoring >> send_email0 >> Init_parameters_for_scoring >> Check_use_cases >> Check_fitted_model >> branching_based_on_fitted_model
branching_based_on_fitted_model >> Info_Fitting_Models >> Init_parameters_for_fit >> Run_MCI >> send_email1
branching_based_on_fitted_model >> Info_Applying_Models >> Init_parameters_for_apply >> Run_MCI2 >> send_email2
send_email1 >> Info_Applying_Models