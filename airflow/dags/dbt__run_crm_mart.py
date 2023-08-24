from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
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
    schedule_interval=timedelta(days=1),  # Set the schedule interval
)

# Define the tasks
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

check_crm_ddl_modified = DockerOperator(
    task_id='check_crm_ddl_modified',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    command='/bin/bash -c "dbt build -m test_check_ddl_recency"',
    dag=dag,
)

check_source_freshness = DockerOperator(
    task_id='check_source_freshness',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    environment={'ORA_PYTHON_DRIVER_TYPE':'thin'},
    command='/bin/bash -c "dbt source freshness"',
    retries=0,
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
