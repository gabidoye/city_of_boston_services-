#from aifc import Aifc_read
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dataengineering-bizzy")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "datalake-311-bronze")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'boston_service_request')

Airflow_Home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url_prefix = 'https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/f53ebccd-bc61-49f9-83db-625f209c95f5/download'
filename = 'tmppgq9965_.csv'
url_template = url_prefix + '/{filename}'
output_file_template= Airflow_Home +'/boston_{{ execution_date.strftime(\'%Y\') }}.csv'
csv_file = 'boston_{{ execution_date.strftime(\'%Y\') }}.csv'
clear_file = Airflow_Home +'/boston_*.csv'


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)



load_workflow= DAG(
    "boston_service_request_csv", 
    catchup=False,
    schedule_interval="0 0 1 * *",
    start_date= datetime(2021, 1, 1),
    #end_date = datetime(2020,12,31),
    max_active_runs= 1

)

with load_workflow:

    curl_csv_boston_job= BashOperator(
        task_id='download_csv_file_url',
        bash_command=f'curl -sSL {url_template} > {output_file_template}'
    )

    data_to_gcs_job = PythonOperator(
        task_id="csv_to_gcs_job",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{csv_file}",
            "local_file": f"{Airflow_Home}/{csv_file}",
        },
        
    )

    spark_submit_local = SparkSubmitOperator(
		application ='/home/airflow/datalake/project.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=load_workflow
		)


    cleanup_job= BashOperator(
        task_id='cleanup_csv',
        bash_command=f'rm {clear_file}'
    )




curl_csv_boston_job >> data_to_gcs_job >> spark_submit_local >> cleanup_job