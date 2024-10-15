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
    dag_id = 'taha_test',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    get_weeks1 = PostgresOperator(
        task_id='get_weeks',
        sql="""
            select data_date as t2date from core.partition_date_create_times
            where table_name='data.call_types_weekly'
            and data_date > now()::date - '3 months'::interval
            and data_date <= (select max(data_date) - interval '14 days' - interval '28 days'from core.partition_date_create_times where table_name='data.call_types_weekly' and data_date <= now()::date)
            order by data_date;
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )

    def pull_function(**kwargs):
        
        pulled_data = kwargs['ti'].xcom_pull(task_ids='get_weeks', key='return_value')
        formatted_dates = [date[0].strftime("%Y-%m-%d") for date in pulled_data]
        kwargs['ti'].xcom_push(key='formatted_dates', value=formatted_dates)
        
        
    
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True,
        dag=dag,
    )

    def for_loop(**kwargs):
        x = 0
        results = []
        formatted_dates = kwargs['ti'].xcom_pull(task_ids='pull_task', key='formatted_dates')
        #logging.info(formatted_dates)
        for t2date in formatted_dates:
            #logging.info(t2date)
            x += 1
            task_id = f'get_weeks_loop_{x}'
            sql_query = """
                select data_date as tcrmdate from core.partition_date_create_time where table_name='data.in_crm'
                and data_date > now()::Date - '3 months'::interval
                and data_date <= (select max(data_date) - interval '14 days' - interval '28 days' from core.partition_date_create_times where table_name='data_call_types_weekly' and data_date <= now()::date) --at least 6 weeks after exist
                and data_date > '{t2date}'::date - 14
                and data_date <= '{t2date}'::date
                order by data_date desc limit 1;
            """.format(t2date=t2date)

            get_weeks_loop = PostgresOperator(
                task_id=task_id,
                sql=sql_query,
                postgres_conn_id='greenplum_conn',
                dag=dag,
            )

            
            tcrmdate = get_weeks_loop.execute(context=kwargs)
            
            task_id = f'work.create_target_list_churn_inactivity2_{x}'
            sql_query = """
                select * from work.create_target_list_churn_inactivity_laith('{t2date}'::date, '{tcrmdate}'::date)
            """.format(t2date=t2date, tcrmdate=tcrmdate)

            create_target_list_churn_inactivity2 = PostgresOperator(
                task_id=task_id,
                sql=sql_query,
                postgres_conn_id='greenplum_conn',
                dag=dag,
            )

            result = create_target_list_churn_inactivity2.execute(context=kwargs)

            task_id = f'work.create_target_list_portout2_{x}'

            sql_query = """
                select * from work.create_target_list_portout_laith('{t2date}'::date, '{tcrmdate}'::date)
            """.format(t2date=t2date, tcrmdate=tcrmdate)

            create_target_list_portout2 = PostgresOperator(
                task_id=task_id,
                sql=sql_query,
                postgres_conn_id='greenplum_conn',
                dag=dag,
            )
            
            result = create_target_list_churn_inactivity2.execute(context=kwargs)
            
        #logging.info("heeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeey")
        #logging.info(results)

        #kwargs['ti'].xcom_push(key='tcrmdate', value=results)

    for_loop_task = PythonOperator(
        task_id='for_loop_task',
        python_callable=for_loop,
        provide_context=True,
        dag=dag,
    )

    sql_task = PostgresOperator(
        task_id='sql_task',
        sql="""
            select * from tmp.module_targets_tmp_laith_results order by the_date;
        """,
        postgres_conn_id='greenplum_conn',
        dag=dag
    )




    get_weeks1 >> pull_task >> for_loop_task >> sql_task




