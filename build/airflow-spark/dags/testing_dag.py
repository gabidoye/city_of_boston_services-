from aifc import Aifc_read
import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

Airflow_Home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url_prefix = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
url_template = url_prefix + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
output_file_template= Airflow_Home +'/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

testing_workflow= DAG(
    "testing_dag", 
    catchup=True,
    schedule_interval="0 6 2 * *",
    start_date= datetime(2019, 1, 1),
    end_date = datetime(2021,1,1),
    max_active_runs= 3

)

with testing_workflow:

    wget_job= BashOperator(
        task_id='wget',
        # bash_command=f'wget {url} -O {Airflow_Home}/output.csv'
        #bash_command=f'curl -sSL {url} >  {Airflow_Home}/output.csv'
        #bash_command='echo "{{ execution_date.strftime(\'%m\') }}"'
        bash_command=f'curl -sSL {url_template} > {output_file_template}'
    )
    
    ingest_job= BashOperator(
        task_id='ingest',
        bash_command=f'ls {Airflow_Home}'
    )


wget_job >> ingest_job