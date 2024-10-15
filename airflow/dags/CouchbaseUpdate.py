from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException
from couchbase.auth import PasswordAuthenticator
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import couchbase.subdocument as SD

def update_json_in_couchbase():

    bucket_name = "rtpBucket"
    document_id = "p::237671060"
    field_name = "BI_Fields"
    field_value = {"name":"amir", "last":"Andalib", "age":"30"}


    try:
        # Connect to Couchbase
        #cluster = Cluster('couchbase://192.168.5.212:8091')
        #authenticator = PasswordAuthenticator('Administrator', 'Bonyan123')
        authenticator=PasswordAuthenticator(username='Administrator', password='Bonyan123', cert_path='/home/airflow/social_link/airflow/couchbase.crt')
        cluster = Cluster.connect('couchbase://192.168.5.212', ClusterOptions(authenticator=authenticator))
        
        
        bucket = cluster.bucket(bucket_name)
        collection = bucket.default_collection()

        # Update the JSON document in Couchbase using sub-document operations
        result = collection.mutate_in(document_id,[
        (SD.upsert(field_name, field_value))
        ])

        print("JSON document updated successfully")
    except CouchbaseException as e:
        print("Error:", e)




default_args = {
    'owner': 'bonyan',
    'start_date': datetime(2024, 4, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id = 'couchbase_update',
    default_args=default_args,
    description='couchbase update',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    c_update = PythonOperator(
        task_id='c_update',
        python_callable=update_json_in_couchbase,
        dag=dag
    )

    c_update