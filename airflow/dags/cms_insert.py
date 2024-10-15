import gzip
import pandas as pd
import psycopg2
from io import StringIO
import glob
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Variable.get('perform_cleanup') 
default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}

db_params = {
    'dbname': Variable.get('cms_db_name'),
    'user': Variable.get('cms_db_user'),
    'password': Variable.get('cms_db_pass'),
    'host': Variable.get('cms_db_host'),
    'port': Variable.get('cms_db_port')
}


gz_files_pattern = Variable.get('cms_report_files_path')


def load_multiple_files_to_postgres():
    # List to hold DataFrames for each file
    df_list = []

    # Find all files matching the pattern
    gz_files = glob.glob(gz_files_pattern)
    
    for gz_file_path in gz_files:
        # Read the gz file
        with gzip.open(gz_file_path, 'rt') as gz_file:
            # Load the CSV data into a pandas DataFrame
            df = pd.read_csv(gz_file, sep=';', header=None, 
                             names=['triggertime', 'msisdn', 'action', 'logicid', 'content', 'paytype'])
            # Convert triggertime column to datetime format
            df['triggertime'] = pd.to_datetime(df['triggertime'], format='%Y%m%d%H%M%S')
            # Append the DataFrame to the list
            df_list.append(df)
    
    # Concatenate all DataFrames
    combined_df = pd.concat(df_list, ignore_index=True)

    # Convert DataFrame to list of tuples
    data = [tuple(row) for row in combined_df.values]

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create a placeholder string for the VALUES clause
    placeholders = ','.join(['%s'] * len(combined_df.columns))

    # Construct the full INSERT statement using executemany
    insert_query = '''
        INSERT INTO cms_schema.cms_table (triggertime, msisdn, action, logicid, content, paytype)
        VALUES ({})
        '''.format(placeholders)

    # Execute the INSERT statement with executemany
    cur.executemany(insert_query, data)

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()



with DAG(
    dag_id = 'cms_isnert_dag',
    default_args=default_args,
    description='test dag',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    check_split_result = PythonOperator(
        task_id='load_multiple_files_to_postgres',
        python_callable=load_multiple_files_to_postgres,
        provide_context=True,
        dag=dag
    )