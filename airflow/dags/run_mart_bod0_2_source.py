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
    'start_date': datetime(2023, 8, 15),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the variables
dbt_project_dir = '/opt/components/dbt'

# Define functions
def ab_partial_update_source(source_id:str, dict_changes:dict):
    url = 'http://localhost:8000/api/v1/sources/partial_update'
    body = {
        "sourceId":source_id,
        "connectionConfiguration":dict_changes
    }
    result = requests.post(url,json=body,auth=('airbyte','password'))
    return result.json()



# Create the DAG instance
dag = DAG(
    'run_mart_bod0_2_source',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
)

# Define the tasks
change_ab_sync_dates = PythonOperator(
    task_id='change_ab_sync_dates',
    python_callable=ab_partial_update_source,
    op_kwargs={
        'source_id':'1af79556-543c-4a20-9d8b-0009bd86c832',
        'dict_changes':{
            'start_date':'',
            'end_date':''
        }
    },
    dag=dag
)


trigger_appflyer_inapps = AirbyteTriggerSyncOperator(
    task_id='trigger_appflyer_inapps',
    airbyte_conn_id='airbyte_conn',
    connection_id='1af79556-543c-4a20-9d8b-0009bd86c832',
    asynchronous=False
)

run_dbt_stg_appsflyer_inapps = DockerOperator(
    task_id='run_dbt_debug',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    command='/bin/bash -c "dbt build -m "',
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag,
)

build_images = BashOperator(
    task_id='build_images',
    bash_command=f"""
        cd / && 
        cd {dbt_project_dir} && 
        ./build-dbt-platform.sh 
    """,
    dag=dag,
)

run_dbt_debug = DockerOperator(
    task_id='run_dbt_debug',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    command='/bin/bash -c "dbt debug"',
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag,
)

run_crm_mart_and_all_children = DockerOperator(
    task_id='run_crm_mart_and_all_children',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +mart__crm__rep_bod0_2_application"',
    retries=10,
    # retry_delay=timedelta(minutes=20),
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

# Define task dependencies
build_images >> run_dbt_debug >> check_crm_ddl_modified >> [check_source_freshness, run_crm_mart_and_all_children]
