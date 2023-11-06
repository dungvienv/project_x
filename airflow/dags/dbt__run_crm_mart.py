from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,9,1,8,0,0),
    'email_on_failure': False,
    'email_on_retry': False,

}
# Define the variables
dbt_project_dir = '/opt/components/dbt'

# Create the DAG instance
dag = DAG(
    'dbt__run_crm_mart',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=timedelta(hours=5),  # Set the schedule interval,
    catchup=False
)

# Define the tasks

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

check_crm_ddl_modified = DockerOperator(
    task_id='check_crm_ddl_modified',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    command='/bin/bash -c "dbt build -m test_check_ddl_recency"',
    dag=dag,
)

run_crm_mart_and_all_children = DockerOperator(
    task_id='run_crm_mart_and_all_children',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt build -m +mart__crm__rep_bod0_2_application"',
    retries=0,
    # retry_delay=timedelta(minutes=20),
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

# Define task dependencies
run_dbt_debug >> check_crm_ddl_modified >> run_crm_mart_and_all_children
