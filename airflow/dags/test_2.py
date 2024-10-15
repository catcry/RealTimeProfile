from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import logging


default_args = {
    'owner': 'tahoora',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}

def pull_function(**kwargs):
    pulled_data = kwargs['ti'].xcom_pull(task_ids='get_weeks', key='return_value')
    t2dates = [date[0].strftime("%Y-%m-%d") for date in pulled_data]
    
    # Iterate through t2dates
    for t2date in t2dates:
        # Execute SQL query for each t2date
        task_id = f'get_weeks_loop_{t2date}'
        sql_query = """
            select dat from core.test_table;
        """.format(t2date=t2date)
        
        get_weeks_loop = PostgresOperator(
            task_id=task_id,
            sql=sql_query,
            postgres_conn_id='greenplum_conn',
            dag=dag,
        )

        # Execute the task
        result = get_weeks_loop.execute(context=kwargs)
        
        
        # Fetch the result
        tcrmdate = get_weeks_loop.output
        pulled_dataa = kwargs['ti'].xcom_pull(task_ids=task_id, key='return_value')
        logging.info("heeeeeeey")
        logging.info(pulled_dataa)
        logging.info(tcrmdate)

        # Store tcrmdate in xcom
        kwargs['ti'].xcom_push(key='tcrmdate', value=tcrmdate)

dag = DAG(
    dag_id='test_dag',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
)

# Define the task to fetch t2dates
get_weeks1 = PostgresOperator(
    task_id='get_weeks',
    sql="""
        SELECT data_date AS t2date 
        FROM core.partition_date_create_times
    """,
    postgres_conn_id='greenplum_conn',
    dag=dag
)

# Define PythonOperator to execute pull_function
pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=dag,
)

# Set up the task dependencies
get_weeks1 >> pull_task