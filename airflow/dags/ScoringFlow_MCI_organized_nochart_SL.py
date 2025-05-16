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
import time



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
        "keys": "''",
        "model_fitting":"false",
        "override_model_variables_check":"true",
        "redefine_preprocessing":"false",
        "uc_churn_inactivity_include_postpaid":"true",
        "uc_churn_inactivity_model_id":"14",
        "uc_churn_postpaid_model_id":"-1",
        "uc_postpaid_include_postpaid":"true",
        "uc_portout_model_id":"13",
        "uc_product_model_id":"-1",
        "uc_zero_day_prediction_model_id":"-1",
        "uc_zero_day_prediction_redefine_value_segments":"false",
        "vals":"''"
        }

        for key, value in variables_dict.items():  

            Variable.set(key, value)

    Init_parameters_for_scoring = PythonOperator(
        task_id='Init_parameters_for_scoring',
        python_callable=Init_parameters_for_scoring_function,
        dag=dag
    )


    Check_use_cases = PostgresOperator(
        task_id='Check_use_cases',
        sql="select active::text from core_categorybean where name_ in ('Best next product prediction', 'Churn inactivity prediction', 'Churn postpaid prediction', 'Zero day prediction', 'Port Out Prediction') order by name_",
        postgres_conn_id='metaDB',
        dag=dag
    )

    def Check_use_cases_output_handler_function(**kwargs):
        
        xcom_value = kwargs['ti'].xcom_pull(task_ids='Check_use_cases')
        Variable.set("uc_include_product", xcom_value[0][0])
        Variable.set("uc_include_churn_inactivity", xcom_value[1][0])
        Variable.set("uc_include_churn_postpaid", xcom_value[2][0])
        Variable.set("uc_include_portout", xcom_value[3][0])
        Variable.set("uc_include_zero_day_prediction", xcom_value[4][0])


    Check_use_cases_output_handler = PythonOperator(
        task_id='Check_use_cases_output_handler',
        python_callable=Check_use_cases_output_handler_function,
        dag=dag
    )
    

    Check_fitted_model = PostgresOperator(
        task_id='Check_fitted_model',
        sql="select * from work.check_fitted_model_availability({{ var.value.model_fitting }});",
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    def Check_fitted_model_output_handler_function(**kwargs):

        xcom_value = kwargs['ti'].xcom_pull(task_ids='Check_fitted_model')
        Variable.set("model_fitting", xcom_value[0][0])


    Check_fitted_model_output_handler = PythonOperator(
        task_id='Check_fitted_model_output_handler',
        python_callable=Check_fitted_model_output_handler_function,
        dag=dag
    )

    def split_check_fitted_model():
        
        if Variable.get("model_fitting") == "True":
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
        python_callable=Init_parameters_for_apply_function,
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
                sql="""SELECT * FROM work.initialize_wf_muc_common(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'source_period_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'post_source_period_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'lag_count' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'max_data_age' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'crm_data_age' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'n_handset_topics' ),
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'common' AND scoring_paras = 'n_cell_events_topics' ),  
                          {{ var.value.override_model_variables_check == "true" }},  
                          ARRAY['churn_inactivity',           'churn_postpaid',           'zero_day_prediction',           'product',           'portout'],
                          ARRAY[{{ var.value.uc_include_churn_inactivity == "true" }}, {{ var.value.uc_include_churn_postpaid == "true" }}, {{ var.value.uc_include_zero_day_prediction == "true" }}, {{ var.value.uc_include_product == "true" }}, {{ var.value.uc_include_portout == "true" }}],
                          '{{ var.value.run_type }}', 
                          (SELECT CASE WHEN '{{ var.value.t2 }}' ~ 't2' THEN NULL ELSE '{{ var.value.t2 }}' END)::date,
                          {{ var.value.mod_job_id | int }}, 
                          '{{ var.value.run_descvar }}',
                          {{ var.value.descvar_interval_weeks | int }},
                          '{{ var.value.lda_model_options }}', 
                          {{ var.value.lda_output_id_old | int }},
                          '{{ var.value.lda_input_options }}',
                          {{ var.value.lda_input_id | int }},
                          '{{ var.value.lda_cell_events_model_options }}', 
                          {{ var.value.lda_cell_events_output_id_old | int }},
                          '{{ var.value.lda_cell_events_input_options }}',
                          {{ var.value.lda_cell_events_input_id | int }},
                          nullif(array[{{ var.value.keys }}], array['']) || array['workflow_run_id', 'calculate_vargroup_output'], 
                          nullif(array[{{ var.value.vals }}], array['']) || array['{{ var.value.workflow_run_id }}', '{{ var.value.calculate_vargroup_output }}'],
                          false
                        );""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )


            def Initialize_generic_parameters_output_handler_function(**kwargs):
                
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.Initialize_generic_parameters')
                Variable.set("mod_job_id", xcom_value[0][0])
                Variable.set("network_id", xcom_value[1][0])
                Variable.set("network_weeks", xcom_value[2][0])
                Variable.set("t1", xcom_value[3][0])
                Variable.set("t2", xcom_value[4][0])
                Variable.set("lag_count", xcom_value[5][0])
                Variable.set("lag_length", xcom_value[6][0])
                Variable.set("source_period_length", xcom_value[7][0])
                Variable.set("calculate_predictors", xcom_value[8][0])
                Variable.set("fit_model", xcom_value[9][0])
                Variable.set("apply_model", xcom_value[10][0])
                Variable.set("run_lda", xcom_value[11][0])
                Variable.set("calculate_lda_input_data", xcom_value[12][0])
                Variable.set("calculate_lda_predictors", xcom_value[13][0])
                Variable.set("lda_input_id", xcom_value[14][0])
                Variable.set("lda_output_id_old", xcom_value[15][0])
                Variable.set("lda_out_id", xcom_value[16][0])
                Variable.set("n_handset_topics", xcom_value[17][0])
                Variable.set("job_use_cases", xcom_value[18][0])
                Variable.set("tcrm", xcom_value[19][0])
                Variable.set("calculate_descvar", xcom_value[20][0])
                Variable.set("force_new_network", xcom_value[21][0])
                Variable.set("calculate_targets", xcom_value[22][0])
                Variable.set("arpu_query", xcom_value[23][0])
                Variable.set("network_2_id", xcom_value[24][0])
                Variable.set("lda_cell_events_input_id", xcom_value[25][0])
                Variable.set("lda_cell_events_output_id_old", xcom_value[26][0])
                Variable.set("lda_cell_events_out_id", xcom_value[27][0])
                Variable.set("n_cell_events_topics", xcom_value[28][0])
                
            



            Initialize_generic_parameters_output_handler = PythonOperator(
                task_id='Initialize_generic_parameters_output_handler',
                python_callable=Initialize_generic_parameters_output_handler_function,
                dag=dag
            )



            join1 = EmptyOperator(task_id="join1", dag=dag)
            join2 = EmptyOperator(task_id="join2", dag=dag)
            join3 = EmptyOperator(task_id="join3", dag=dag)
            join4 = EmptyOperator(task_id="join4", dag=dag)
            join5 = EmptyOperator(task_id="join5", dag=dag)


            def split_run_product_function():
                if Variable.get("uc_include_product") == "true":
                    return 'Run_MCI2.Run_Initialize_MCI.initialize_product'
                else:
                    return 'Run_MCI2.Run_Initialize_MCI.join1'
                
            
            branching_initialize_product = BranchPythonOperator(
                task_id='branching_initialize_product',
                python_callable=split_run_product_function,
                dag=dag
            )





            def split_run_churn_inactivity_function():
                if Variable.get("uc_include_churn_inactivity") == "true":
                    return 'Run_MCI2.Run_Initialize_MCI.initialize_churn_inactivity'
                else:
                    return 'Run_MCI2.Run_Initialize_MCI.join2'
                
            
            branching_initialize_churn_inactivity = BranchPythonOperator(
                task_id='branching_initialize_churn_inactivity',
                python_callable=split_run_churn_inactivity_function,
                dag=dag
            )




            def split_run_churn_postpaid_function():
                if Variable.get("uc_include_churn_postpaid") == "true":
                    return 'Run_MCI2.Run_Initialize_MCI.initialize_churn_postpaid'
                else:
                    return 'Run_MCI2.Run_Initialize_MCI.join3'
                
            
            branching_initialize_churn_postpaid = BranchPythonOperator(
                task_id='branching_initialize_churn_postpaid',
                python_callable=split_run_churn_postpaid_function,
                dag=dag
            )





            def split_run_zero_day_prediction_function():
                if Variable.get("uc_include_zero_day_prediction") == "true":
                    return 'Run_MCI2.Run_Initialize_MCI.initialize_zero_day_prediction'
                else:
                    return 'Run_MCI2.Run_Initialize_MCI.join4'
                
            
            branching_initialize_zero_day_prediction = BranchPythonOperator(
                task_id='branching_initialize_zero_day_prediction',
                python_callable=split_run_zero_day_prediction_function,
                dag=dag
            )





            def split_run_portout_function():
                if Variable.get("uc_include_portout") == "true":
                    return 'Run_MCI2.Run_Initialize_MCI.initialize_portout'
                else:
                    return 'Run_MCI2.Run_Initialize_MCI.join5'
                
            
            branching_initialize_portout = BranchPythonOperator(
                task_id='branching_initialize_portout',
                python_callable=split_run_portout_function,
                dag=dag
            )
            


            



            initialize_product = PostgresOperator(
                task_id='initialize_product',
                sql="""SELECT * FROM work.initialize_product(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'product' AND scoring_paras = 'gap_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'product' AND scoring_paras = 'target_period_length' ), 
                          {{ var.value.mod_job_id | int }},
                          {{ var.value.uc_product_model_id | int }},
                          {{ var.value.redefine_preprocessing == "true" }}
                        )""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )
            

            initialize_churn_inactivity = PostgresOperator(
                task_id='initialize_churn_inactivity',
                sql=""" SELECT * FROM work.initialize_churn_inactivity(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'inactivity' AND scoring_paras = 'gap_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'inactivity' AND scoring_paras = 'target_period_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'inactivity' AND scoring_paras = 'gap2_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'inactivity' AND scoring_paras = 'evaluation_period_length' ),
                          {{ var.value.mod_job_id | int }}, 
                          {{ var.value.uc_churn_inactivity_model_id | int }},
                          {{ var.value.uc_churn_inactivity_include_postpaid == "true" }},
                          {{ var.value.redefine_preprocessing == "true" }}
                        )""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )


            initialize_churn_postpaid = PostgresOperator(
                task_id='initialize_churn_postpaid',
                sql=""" SELECT * FROM work.initialize_churn_postpaid(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'postpaid' AND scoring_paras = 'gap_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'postpaid' AND scoring_paras = 'target_period_length' ),
                          {{ var.value.mod_job_id | int }}, 
                          {{ var.value.uc_churn_postpaid_model_id | int }},
                          {{ var.value.redefine_preprocessing == "true" }}
                        )""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )


            initialize_zero_day_prediction = PostgresOperator(
                task_id='initialize_zero_day_prediction',
                sql=""" SELECT * FROM work.initialize_zero_day_prediction(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'zero_day' AND scoring_paras = 'uc_zero_day_prediction_evaluation_period_length' ), 
                          {{ var.value.mod_job_id | int }}, 
                          {{ var.value.uc_zero_day_prediction_model_id | int }},
                          {{ var.value.uc_zero_day_prediction_redefine_value_segments == "true" }},
                          {{ var.value.redefine_preprocessing == "true" }}
                        )""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )


            initialize_portout = PostgresOperator(
                task_id='initialize_portout',
                sql=""" SELECT * FROM work.initialize_portout(
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'portout' AND scoring_paras = 'gap_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'portout' AND scoring_paras = 'target_period_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'portout' AND scoring_paras = 'gap2_length' ), 
                          (SELECT para_value::integer 
                                  FROM work.scoring_muc_parameters WHERE use_case= 'portout' AND scoring_paras = 'evaluation_period_length' ),
                          {{ var.value.mod_job_id | int }},
                          {{ var.value.uc_portout_model_id | int }},
                          {{ var.value.uc_portout_model_id == "true" }},
                          {{ var.value.redefine_preprocessing == "true" }}
                        )""",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )





            def Initialize_portout_output_handler_function(**kwargs):
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.initialize_portout')
                Variable.set("uc_portout_model_id", xcom_value[0][0])
                Variable.set("uc_portout_template_model_id", xcom_value[1][0])
                

            initialize_portout_output_handler = PythonOperator(
                task_id='Initialize_portout_output_handler',
                python_callable=Initialize_portout_output_handler_function,
                dag=dag
            )



            def initialize_zero_day_prediction_output_handler_function(**kwargs):
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.initialize_zero_day_prediction')
                Variable.set("uc_zero_day_prediction_model_id", xcom_value[0][0])
                Variable.set("uc_zero_day_prediction_template_model_id", xcom_value[1][0])
                

            initialize_zero_day_prediction_output_handler = PythonOperator(
                task_id='initialize_zero_day_prediction_output_handler',
                python_callable=initialize_zero_day_prediction_output_handler_function,
                dag=dag
            )



            def initialize_churn_postpaid_output_handler_function(**kwargs):
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.initialize_churn_postpaid')
                Variable.set("uc_churn_postpaid_model_id", xcom_value[0][0])
                Variable.set("uc_churn_postpaid_template_model_id", xcom_value[1][0])
                

            initialize_churn_postpaid_output_handler = PythonOperator(
                task_id='initialize_churn_postpaid_output_handler',
                python_callable=initialize_churn_postpaid_output_handler_function,
                dag=dag
            )


            def initialize_churn_inactivity_output_handler_function(**kwargs):
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.initialize_churn_inactivity')
                Variable.set("uc_churn_inactivity_model_id", xcom_value[0][0])
                Variable.set("uc_churn_inactivity_template_model_id", xcom_value[1][0])
                

            initialize_churn_inactivity_output_handler = PythonOperator(
                task_id='initialize_churn_inactivity_output_handler',
                python_callable=initialize_churn_inactivity_output_handler_function,
                dag=dag
            )


            def initialize_product_output_handler_function(**kwargs):
                
                xcom_value = kwargs['ti'].xcom_pull(task_ids='Run_MCI2.Run_Initialize_MCI.initialize_product')
                Variable.set("uc_product_model_id", xcom_value[0][0])
                Variable.set("uc_product_template_model_id", xcom_value[1][0])
                

            initialize_product_output_handler = PythonOperator(
                task_id='initialize_product_output_handler',
                python_callable=initialize_product_output_handler_function,
                dag=dag
            )



            
            Initialize_generic_parameters >> Initialize_generic_parameters_output_handler
            
            Initialize_generic_parameters_output_handler >> branching_initialize_product
            Initialize_generic_parameters_output_handler >> branching_initialize_churn_inactivity
            Initialize_generic_parameters_output_handler >> branching_initialize_churn_postpaid
            Initialize_generic_parameters_output_handler >> branching_initialize_zero_day_prediction
            Initialize_generic_parameters_output_handler >> branching_initialize_portout
            
            branching_initialize_product >> initialize_product >> initialize_product_output_handler
            branching_initialize_product >> join1
            
            branching_initialize_churn_inactivity >> initialize_churn_inactivity >> initialize_churn_inactivity_output_handler
            branching_initialize_churn_inactivity >> join2
            
            branching_initialize_churn_postpaid >> initialize_churn_postpaid >> initialize_churn_postpaid_output_handler
            branching_initialize_churn_postpaid >> join3
            
            branching_initialize_zero_day_prediction >> initialize_zero_day_prediction >> initialize_zero_day_prediction_output_handler
            branching_initialize_zero_day_prediction >> join4

            branching_initialize_portout >> initialize_portout >> initialize_portout_output_handler
            branching_initialize_portout >> join5




        
        
        def split_calculate_targets_function():
            if Variable.get("calculate_targets") == "true":
                return 'Run_MCI2.join6'
            else:
                return 'Run_MCI2.branching_calculate_predictors'
            
        branching_calculate_targets = BranchPythonOperator(
                task_id='branching_calculate_targets',
                python_callable=split_calculate_targets_function,
                trigger_rule="all_done",
                dag=dag
            )
        

        def split_calculate_predictors_function():
            if Variable.get("calculate_predictors") == "true":
                return 'Run_MCI2.join7'
            else:
                return 'Run_MCI2.join8'
            
        branching_calculate_predictors = BranchPythonOperator(
                task_id='branching_calculate_predictors',
                python_callable=split_calculate_predictors_function,
                trigger_rule="all_done",
                dag=dag
            )

        
        join6 = EmptyOperator(task_id="join6", dag=dag)
        join7 = EmptyOperator(task_id="join7", dag=dag)
        join8 = EmptyOperator(task_id="join8", dag=dag)

        with TaskGroup("Run_targets_MCI", tooltip="Run_targets_MCI") as Run_targets_MCI:


            join1 = EmptyOperator(task_id="join1", dag=dag)
            join2 = EmptyOperator(task_id="join2", dag=dag)
            join3 = EmptyOperator(task_id="join3", dag=dag)
            join4 = EmptyOperator(task_id="join4", dag=dag)
            join5 = EmptyOperator(task_id="join5", dag=dag)



            def split_create_targets_for_product_function():
                if Variable.get("uc_include_product") == "true":
                    return 'Run_MCI2.Run_targets_MCI.create_targets_for_product'
                else:
                    return 'Run_MCI2.Run_targets_MCI.join1'
                
            
            branching_create_targets_for_product = BranchPythonOperator(
                task_id='branching_create_targets_for_product',
                python_callable=split_create_targets_for_product_function,
                dag=dag
            )





            def split_create_targets_for_churn_inactivity_function():
                if Variable.get("uc_include_churn_inactivity") == "true":
                    return 'Run_MCI2.Run_targets_MCI.create_targets_for_churn_inactivity'
                else:
                    return 'Run_MCI2.Run_targets_MCI.join2'
                
            
            branching_create_targets_for_churn_inactivity = BranchPythonOperator(
                task_id='branching_create_targets_for_churn_inactivity',
                python_callable=split_create_targets_for_churn_inactivity_function,
                dag=dag
            )




            def split_create_targets_for_churn_postpaid_function():
                if Variable.get("uc_include_churn_postpaid") == "true":
                    return 'Run_MCI2.Run_targets_MCI.create_targets_for_churn_postpaid'
                else:
                    return 'Run_MCI2.Run_targets_MCI.join3'
                
            
            branching_create_targets_for_churn_postpaid = BranchPythonOperator(
                task_id='branching_create_targets_for_churn_postpaid',
                python_callable=split_create_targets_for_churn_postpaid_function,
                dag=dag
            )





            def split_create_targets_for_zero_day_prediction_function():
                if Variable.get("uc_include_zero_day_prediction") == "true":
                    return 'Run_MCI2.Run_targets_MCI.create_targets_for_zero_day_prediction'
                else:
                    return 'Run_MCI2.Run_targets_MCI.join4'
                
            
            branching_create_targets_for_zero_day_prediction = BranchPythonOperator(
                task_id='branching_create_targets_for_zero_day_prediction',
                python_callable=split_create_targets_for_zero_day_prediction_function,
                dag=dag
            )





            def split_create_targets_for_portout_function():
                if Variable.get("uc_include_portout") == "true":
                    return 'Run_MCI2.Run_targets_MCI.create_targets_for_portout'
                else:
                    return 'Run_MCI2.Run_targets_MCI.join5'
                
            
            branching_create_targets_for_portout = BranchPythonOperator(
                task_id='branching_create_targets_for_portout',
                python_callable=split_create_targets_for_portout_function,
                dag=dag
            )






            create_targets_for_product = PostgresOperator(
                task_id='create_targets_for_product',
                sql="select * from work.create_target_list_product({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )

            create_targets_for_churn_inactivity = PostgresOperator(
                task_id='create_targets_for_churn_inactivity',
                sql="select * from work.create_target_list_churn_inactivity({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )

            create_targets_for_churn_postpaid = PostgresOperator(
                task_id='create_targets_for_churn_postpaid',
                sql="select * from work.create_target_list_churn_postpaid({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )

            create_targets_for_zero_day_prediction = PostgresOperator(
                task_id='create_targets_for_zero_day_prediction',
                sql="select * from work.create_target_list_zero_day_prediction({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )

            create_targets_for_portout = PostgresOperator(
                task_id='create_targets_for_portout',
                sql="select * from work.create_target_list_portout({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                dag=dag
            )




            combine_target_lists = PostgresOperator(
                task_id='combine_target_lists',
                sql="select * from work.combine_target_lists({{ var.value.mod_job_id | int }})",
                postgres_conn_id='greenplum_conn',
                trigger_rule='none_failed',
                dag=dag
            )






            branching_create_targets_for_product >> create_targets_for_product 
            branching_create_targets_for_product >> join1
            
            branching_create_targets_for_churn_inactivity >> create_targets_for_churn_inactivity 
            branching_create_targets_for_churn_inactivity >> join2
            
            branching_create_targets_for_churn_postpaid >> create_targets_for_churn_postpaid 
            branching_create_targets_for_churn_postpaid >> join3
            
            branching_create_targets_for_zero_day_prediction >> create_targets_for_zero_day_prediction 
            branching_create_targets_for_zero_day_prediction >> join4

            branching_create_targets_for_portout >> create_targets_for_portout
            branching_create_targets_for_portout >> join5

            create_targets_for_product >> combine_target_lists
            join1 >> combine_target_lists
            create_targets_for_churn_inactivity >> combine_target_lists
            join2 >> combine_target_lists
            create_targets_for_churn_postpaid >> combine_target_lists
            join3 >> combine_target_lists
            create_targets_for_zero_day_prediction >> combine_target_lists
            join4 >> combine_target_lists
            create_targets_for_portout >> combine_target_lists
            join5 >> combine_target_lists


        
        

        with TaskGroup("Run_predictors_MCI", tooltip="Run_predictors_MCI") as Run_predictors_MCI:

            with TaskGroup("CalculateCommonPredictors_MCI_nogeoloc", tooltip="CalculateCommonPredictors_MCI_nogeoloc") as CalculateCommonPredictors_MCI_nogeoloc:
                def Disable_LDA_Predictors_function():
                    variables_dict = {
                    "calculate_lda_cell_events_predictors": "false",
                    "run_lda_cell_events": "false",
                    "run_lda": "false",
                    "calculate_lda_predictors":"false"
                    }

                    for key, value in variables_dict.items():  
                    
                        Variable.set(key, value)

                Disable_LDA_Predictors = PythonOperator(
                    task_id='Disable_LDA_Predictors',
                    python_callable=Disable_LDA_Predictors_function,
                    dag=dag
                )

                split = EmptyOperator(task_id="split", dag=dag)

                


                create_predictors1 = PostgresOperator(
                    task_id='create_predictors1',
                    sql="select * from work.create_modelling_data1({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )

                create_predictors4_made = PostgresOperator(
                    task_id='create_predictors4_made',
                    sql="select * from work.create_modelling_data4_made({{ var.value.mod_job_id | int }}, {{ var.value.lag_length | int }}, {{ var.value.lag_count | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )

                create_predictors4_rec = PostgresOperator(
                    task_id='create_predictors4_rec',
                    sql="select * from work.create_modelling_data4_rec({{ var.value.mod_job_id | int }}, {{ var.value.lag_length | int }}, {{ var.value.lag_count | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )

                create_predictors_topup1 = PostgresOperator(
                    task_id='create_predictors_topup1',
                    sql="select * from work.create_modelling_data_topup1({{ var.value.mod_job_id | int }}, {{ var.value.lag_length | int }}, {{ var.value.lag_count | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )

                create_predictors_topup2 = PostgresOperator(
                    task_id='create_predictors_topup2',
                    sql="select * from work.create_modelling_data_topup2({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )

                create_predictors_topup3 = PostgresOperator(
                    task_id='create_predictors_topup3',
                    sql="select * from work.create_modelling_data_topup3({{ var.value.mod_job_id | int }}, {{ var.value.lag_length | int }}, {{ var.value.lag_count | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )


                create_predictors_topup_channels = PostgresOperator(
                    task_id='create_predictors_topup_channels',
                    sql="select * from work.create_modelling_data_topup_channels({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )



                calculate_monthly_arpu = PostgresOperator(
                    task_id='calculate_monthly_arpu',
                    sql="select * from work.calculate_monthly_arpu({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )




                send_email3 = EmailOperator( 
                    task_id='send_email3', 
                    to='tahamiri02@gmail.com', 
                    subject='FAA Scoring run',
                    html_content='starting network_scorer {{ var.value.t2 }}' 
                )



                we_should_create_orbiret_task_check_the_xml_file = EmptyOperator(task_id="we_should_create_orbiret_task_check_the_xml_file", dag=dag)


                send_email4 = EmailOperator( 
                    task_id='send_email4', 
                    to='tahamiri02@gmail.com', 
                    subject='FAA Scoring run',
                    html_content='finished network_scorer {{ var.value.t2 }}' 
                )


                join = EmptyOperator(task_id="join", dag=dag)


                create_predictors_2 = PostgresOperator(
                    task_id='create_predictors_2',
                    sql="select * from work.create_modelling_data2({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )



                create_predictors_3 = PostgresOperator(
                    task_id='create_predictors_3',
                    sql="select * from work.create_modelling_data3({{ var.value.mod_job_id | int }})",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )



                Combine_modelling_data = PostgresOperator(
                    task_id='Combine_modelling_data',
                    sql="""SELECT * FROM work.combine_modelling_data(
                                {{ var.value.mod_job_id | int }},
                                'work.modelling_data_matrix',
                                'work.module_targets,
                                work.modelling_data_matrix_1,
                                work.modelling_data_matrix_geolocation,
                                work.modelling_data_matrix_2,
                                work.modelling_data_matrix_3,
                                work.modelling_data_matrix_4_made,
                                work.modelling_data_matrix_4_rec,
                                work.modelling_data_matrix_topup1,
                                work.modelling_data_matrix_topup2,
                                work.modelling_data_matrix_topup3,
                                work.modelling_data_matrix_topup3a,
                                work.modelling_data_matrix_cell_events_topic,
                                work.modelling_data_matrix_handset_topic,
                                work.modelling_data_matrix_zdp1,
                                work.modelling_data_matrix_zdp2,
                                work.modelling_data_matrix_zdp4',
                                'mod_job_id,alias_id');""",
                    postgres_conn_id='greenplum_conn',
                    dag=dag
                )


                




                Disable_LDA_Predictors >> split 
                split >> create_predictors1
                split >> calculate_monthly_arpu
                create_predictors1 >> create_predictors4_made >> create_predictors4_rec >> create_predictors_topup1 >> create_predictors_topup2 >> create_predictors_topup3 >> create_predictors_topup_channels
                calculate_monthly_arpu >> send_email3 >> we_should_create_orbiret_task_check_the_xml_file >> send_email4
                create_predictors_topup_channels >> join
                send_email4 >> join
                join >> create_predictors_2 >> create_predictors_3 >> Combine_modelling_data
            CalculateCommonPredictors_MCI_nogeoloc
                





            


        
        
            
        Init_Datasource1 >> Run_Initialize_MCI >> branching_calculate_targets
        branching_calculate_targets >> join6
        branching_calculate_targets >> branching_calculate_predictors
        join6 >> Run_targets_MCI
        Run_targets_MCI >> branching_calculate_predictors
        branching_calculate_predictors >> join7
        branching_calculate_predictors >> join8
        join7 >> Run_predictors_MCI >> join8
        

        

    send_email2 = EmailOperator( 
        task_id='send_email2', 
        to='tahamiri02@gmail.com', 
        subject='FAA Scoring run',
        html_content='apply completed for {{ var.value.t2 }}' 
    )

    


    


    
sleep_task >> Info_started_scoring >> send_email0 >> Init_parameters_for_scoring >> Check_use_cases >> Check_use_cases_output_handler >> Check_fitted_model >> Check_fitted_model_output_handler >> branching_based_on_fitted_model
branching_based_on_fitted_model >> Info_Fitting_Models >> Init_parameters_for_fit >> Run_MCI >> send_email1
branching_based_on_fitted_model >> Info_Applying_Models >> Init_parameters_for_apply >> Run_MCI2 >> send_email2
send_email1 >> Info_Applying_Models