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
    'start_date': datetime(2023,9,1,8,0,0),
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
    'run_facebook_ad_platform_sources',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval='* 1 * * *',  # Set the schedule interval
    concurrency=1,
    max_active_runs=1,
    catchup=False
)

# Define the tasks


change_facebook_1020643908514628_source_dates = PythonOperator(
    task_id='change_facebook_1020643908514628_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'893f8ece-1e45-4731-8b63-2f4f7a98571b','with_tz':True},
    dag=dag
)

change_facebook_1801919233484199_source_dates = PythonOperator(
    task_id='change_facebook_1801919233484199_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'818c8571-847f-405a-b320-2427f1e55e0a','with_tz':True},
    dag=dag
)

change_facebook_1843032105782331_source_dates = PythonOperator(
    task_id='change_facebook_1843032105782331_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'ca913a76-9001-48e1-8934-be1d0c0bce82','with_tz':True},
    dag=dag
)

change_facebook_360061471554632_source_dates = PythonOperator(
    task_id='change_facebook_360061471554632_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'3a2578a9-2162-4ba7-8457-a5e6bd66d070','with_tz':True},
    dag=dag
)

change_facebook_616509509961059_source_dates = PythonOperator(
    task_id='change_facebook_616509509961059_source_dates',
    python_callable=ab_partial_update_source,
    provide_context=True,
    op_kwargs={'source_id':'ab6270f8-b496-433c-86d9-828baab5a56e','with_tz':True},
    dag=dag
)


trigger_facebook_1020643908514628 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1020643908514628',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'c80ecc81-553d-4ad5-9b63-a83ec1feb19d',
    asynchronous=False,
    timeout=18000,
    dag=dag
)

trigger_facebook_1801919233484199 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1801919233484199',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = '247ae9b4-d8b4-4ddc-a01f-478b3101ab8a',
    asynchronous=False,
    timeout=18000,
    dag=dag
)

trigger_facebook_1843032105782331 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_1843032105782331',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'e3af74f3-526f-4be2-bb0a-b488ebc79ba8',
    asynchronous=False,
    timeout=18000,
    dag=dag
)

trigger_facebook_360061471554632 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_360061471554632',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'e9b24a72-cbc8-4d69-b9ef-bfb9b7ec2c13',
    asynchronous=False,
    timeout=18000,
    dag=dag
)

trigger_facebook_616509509961059 = AirbyteTriggerSyncOperator(
    task_id='trigger_facebook_616509509961059',
    airbyte_conn_id = 'airbyte_conn',
    connection_id = 'e6036090-ddf1-4be9-90ee-fa2f8382c4b0',
    asynchronous=False,
    timeout=18000,
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