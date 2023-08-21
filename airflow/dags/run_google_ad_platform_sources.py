from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import requests

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,1,1),
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
                "start_date":context['data_interval_start'].replace(hour=7,minute=0,second=0).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end_date":context['data_interval_end'].replace(hour=7,minute=0,second=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }
    else:
        body = {
            "sourceId":source_id,
            "connectionConfiguration":{
                "start_date":context['data_interval_start'].replace(hour=0,minute=0,second=0).strftime('%Y-%m-%d'),
                "end_date":context['data_interval_end'].replace(hour=0,minute=0,second=0).strftime('%Y-%m-%d')
            }
        }
    result = requests.post(url,json=body,auth=('airbyte','password'))
    return result.json()



# Create the DAG instance
dag = DAG(
    'run_google_ad_platform_sources',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
    concurrency=1,
    max_active_runs=1
)

# Define the tasks


change_google_1698640435_source_dates = PythonOperator(
    task_id='change_google_1698640435_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'be095449-cd97-4225-a85e-10bbd808f861','with_tz':False},
    dag=dag
)

change_google_4607976191_source_dates = PythonOperator(
    task_id='change_google_4607976191_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'d2ba6144-9d66-4662-8ede-73e1d630fd83','with_tz':False},
    dag=dag
)

trigger_google_1698640435 = AirbyteTriggerSyncOperator(
    task_id='trigger_google_1698640435',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'cbd3404d-ee67-42ac-aa45-07688741d973',
    asynchronous=False,
    dag=dag
)

trigger_google_4607976191 = AirbyteTriggerSyncOperator(
    task_id='trigger_google_4607976191',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '8ca6a59f-6b7e-4cff-8efb-6cb465f5fd1f',
    asynchronous=False,
    dag=dag
)

run_dbt_google = DockerOperator(
    task_id='run_dbt_google',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +int__google__ad_insights"',
    retries=0,
    # retry_delay=timedelta(minutes=20),
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)


# Define task dependencies
change_google_1698640435_source_dates >> trigger_google_1698640435
change_google_4607976191_source_dates >> trigger_google_4607976191
[trigger_google_1698640435,trigger_google_4607976191] >> run_dbt_google