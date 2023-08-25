from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':0,
    'catchup':True,

}

# Define the variables
dbt_project_dir = '/opt/components/dbt'


# Define functions
def ab_partial_update_source(source_id:str, with_tz:bool,**context):
    url = 'http://host.docker.internal:8000/api/v1/sources/partial_update'
    if with_tz:
        body = {
            "sourceId":source_id,
            "connectionConfiguration":{
                "start_date":(context['data_interval_start'] - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end_date":(context['data_interval_end']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            }
        }
    else:
        body = {
            "sourceId":source_id,
            "connectionConfiguration":{
                "start_date":(context['data_interval_start'] - timedelta(days=7)).strftime('%Y-%m-%d'),
                "end_date":(context['data_interval_end']).strftime('%Y-%m-%d')
            }
        }
    result = requests.post(url,json=body,auth=('airbyte','password'))
    return result.json()



# Create the DAG instance
dag = DAG(
    'run_tiktok_ad_platform_sources',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
    concurrency=1,
    max_active_runs=1
)

# Define the tasks

change_titkok_source_dates = PythonOperator(
    task_id='change_tiktok_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'58471ac8-37c2-46b5-8a44-63493c9daf4b','with_tz':False},
    dag=dag
)


trigger_tiktok = AirbyteTriggerSyncOperator(
    task_id='trigger_tiktok',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '8bddbf3b-7ed4-4fda-95d3-0b69c366b053',
    asynchronous=False,
    # execution_timeout=timedelta(minutes=5),
    retries=0,
    timeout=18000,
    dag=dag
)


run_dbt_tiktok = DockerOperator(
    task_id='run_dbt_tiktok',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +int__tiktok__ad_insights --full-refresh"',
    retries=0,
    # retry_delay=timedelta(minutes=2),
    # execution_timeout=timedelta(minutes=2),
    dag=dag,
)

# Define task dependencies
change_titkok_source_dates >> trigger_tiktok >> run_dbt_tiktok