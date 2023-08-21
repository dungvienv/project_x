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
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':0,
    'catchup':False,

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
                "provider": {"start_date":context['data_interval_start'].strftime('%Y-%m-%dT%H:%M:%SZ')}
            }
        }
    else:
        body = {
            "sourceId":source_id,
            "connectionConfiguration":{
                "provider": {"start_date":context['data_interval_start'].strftime('%Y-%m-%d')}
            }
        }
    result = requests.post(url,json=body,auth=('airbyte','password'))
    return result.json()



# Create the DAG instance
dag = DAG(
    'run_appsflyer_inapps_retargeting_sources',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
    concurrency=1,
    max_active_runs=1
)

# Define the tasks

change_appsflyer_inapps_retargeting_source_dates = PythonOperator(
    task_id='change_appsflyer_inapps_retargeting_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'b1fc0391-8b8a-41c1-9e53-4784cb120749','with_tz':True},
    dag=dag
)


trigger_appsflyer_inapps_retargeting = AirbyteTriggerSyncOperator(
    task_id='trigger_appsflyer_inapps_retargeting',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'c681b68b-6064-4d11-96d6-c98227f3ac65',
    asynchronous=False,
    # execution_timeout=timedelta(minutes=5),
    retries=0,
    dag=dag
)


run_dbt_appsflyer_inapps_retargeting = DockerOperator(
    task_id='run_dbt_appsflyer_inapps_retargeting',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +int__appsflyer__inapps_retargeting"',
    retries=0,
    # retry_delay=timedelta(minutes=20),
    # execution_timeout=timedelta(minutes=2),
    dag=dag,
)


# Define task dependencies
change_appsflyer_inapps_retargeting_source_dates >> trigger_appsflyer_inapps_retargeting >> run_dbt_appsflyer_inapps_retargeting