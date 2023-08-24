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
    'refresh_dbt_image',
    default_args=default_args,
    description='An example DAG with DockerOperator',
    schedule_interval=None,  # Set the schedule interval
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

# Define task dependencies
build_images