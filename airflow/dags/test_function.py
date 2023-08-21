from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 25),
    'email_on_failure': False,
    'email_on_retry': False,

}
# Define the variables
dbt_project_dir = '/opt/components/dbt'

# Create the DAG instance
dag = DAG(
    'test_function',
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
    dag=dag,

)

run_dbt_gma = DockerOperator(
    task_id='run_dbt_gma',
    image='dbt-dbt',
    api_version='auto',
    auto_remove=True,
    command='/bin/bash -c "dbt build -m int__gma__first_login int__gma__install "',
    dag=dag,

)



# Define task dependencies
build_images >> run_dbt_debug >> run_dbt_gma
