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
    'run_facebook_ad_platform_sources',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
    concurrency=1,
    max_active_runs=1
)

# Define the tasks


change_facebook_1020643908514628_source_dates = PythonOperator(
    task_id='change_facebook_1020643908514628_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'2bc7bafc-1dcb-4be1-b49c-690ab36c2b6a','with_tz':True},
    dag=dag
)

change_facebook_1801919233484199_source_dates = PythonOperator(
    task_id='change_facebook_1801919233484199_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'628e9751-20e9-42ec-958e-2930efa23272','with_tz':True},
    dag=dag
)

change_facebook_1843032105782331_source_dates = PythonOperator(
    task_id='change_facebook_1843032105782331_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'a0b6e4db-8041-419c-bd9f-4c716ce9f724','with_tz':True},
    dag=dag
)

change_facebook_360061471554632_source_dates = PythonOperator(
    task_id='change_facebook_360061471554632_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'95dff50d-56f0-42a0-9100-6dced56deaff','with_tz':True},
    dag=dag
)

change_facebook_616509509961059_source_dates = PythonOperator(
    task_id='change_facebook_616509509961059_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'7761ed46-597f-4930-8fe6-625c9d7399f8','with_tz':True},
    dag=dag
)


trigger_facebook_1020643908514628 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1020643908514628',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '3832e186-0278-4432-af9e-9f52c2edb1d4',
    asynchronous=False,
    dag=dag
)

trigger_facebook_1801919233484199 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1801919233484199',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '11893a9f-1f71-47fa-8d8e-eedfe06d21c5',
    asynchronous=False,
    dag=dag
)

trigger_facebook_1843032105782331 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1843032105782331',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'd924ac41-dbc2-40ab-bb4d-a4b3245c0fc8',
    asynchronous=False,
    dag=dag
)

trigger_facebook_360061471554632 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_360061471554632',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '5f271a8f-e41c-416a-bdea-d71f933a9971',
    asynchronous=False,
    dag=dag
)

trigger_facebook_616509509961059 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_616509509961059',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '05075067-5c03-4375-a66e-56c951d1aaf4',
    asynchronous=False,
    dag=dag
)



run_dbt_facebook = DockerOperator(
    task_id='run_dbt_facebook',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +int__facebook__ad_insights"',
    retries=0,
    # retry_delay=timedelta(minutes=20),
    # execution_timeout=timedelta(minutes=2),
    dag=dag,
)


# Define task dependencies

change_facebook_1020643908514628_source_dates >> trigger_facebook_1020643908514628
change_facebook_1801919233484199_source_dates >> trigger_facebook_1801919233484199
change_facebook_1843032105782331_source_dates >> trigger_facebook_1843032105782331
change_facebook_360061471554632_source_dates >> trigger_facebook_360061471554632
change_facebook_616509509961059_source_dates >> trigger_facebook_616509509961059
[trigger_facebook_1020643908514628,trigger_facebook_1801919233484199,trigger_facebook_1843032105782331,trigger_facebook_360061471554632,trigger_facebook_616509509961059] >> run_dbt_facebook