from airflow import DAG
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
from airflow.utils.dates import days_ago
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.decorators import dag, task
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(hours=1)
}

# Define all environment variables
aws_date_param = '{{ macros.ds_add(data_interval_end | ds, -1) }}'
aws_prev_date_param = '{{ macros.ds_add(data_interval_end | ds, -2) }}'
appsflyer_dir = 'temp/appsflyer/inapps_retargeting/'
sync_cmd = f' rm -rf $AIRFLOW_HOME/{appsflyer_dir} &&\
            aws s3 sync \
            --region "eu-west-1" \
            --exclude "*" \
            --include "t=inapps_retargeting/dt={aws_date_param}/*.parquet" \
            --include "t=inapps_retargeting/dt={aws_prev_date_param}/*.parquet" \
            --delete \
            s3://af-ext-reports/b5ef-acc-vh9FVWSA-b5ef/mkt-gma/ $AIRFLOW_HOME/{appsflyer_dir}'

destination_table = 'CUSTOM_RAW_APPSFLYER_INAPPS_RETARGETING'
oracle_conn_id='oracle_conn'

# Define functions
def extract_and_insert(dir_path,destination_table_name,oracle_conn_id) -> None:
    oracle_hook = OracleHook(oracle_conn_id)
    oracle_conn = oracle_hook.get_conn()
    cursor = oracle_conn.cursor()
    file_paths = []

    for root, dirs, files in os.walk(dir_path, topdown=False):
        for file in files:
            if file.endswith(".parquet"):
                file_paths.append(os.path.join(root,file))

    for file in file_paths:
        print(f'Reading file: {file}')
        json_data = pq.read_table(file).to_pandas().to_json(orient='records', lines=True)
        rows = [(json_item,) for json_item in json_data.split('\n') if json_item]
        sql = f"insert into {destination_table_name}(json_data) values (:1)"
        start_pos = 0
        batch_size = 15000
        while start_pos < len(rows):
            data = rows[start_pos:start_pos + batch_size]
            start_pos += batch_size    
            cursor.executemany(sql, data)
            oracle_conn.commit()

    cursor.close()
    oracle_conn.close()

    



# Define the DAG
dag = DAG(
    'run_appsflyer_inapps_retargeting_sources',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,9,1,9,15,0),
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Define the operators
sync_appsflyer_inapps_retargeting = BashOperator(
    task_id='sync_appsflyer_inapps_retargeting',
    bash_command=sync_cmd,
    dag=dag,
)

oracle_create_table = OracleOperator(
    task_id='oracle_create_table',
    sql=f"""
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE {destination_table}';
            DBMS_OUTPUT.PUT_LINE('Table created successfully.');
        EXCEPTION
            WHEN OTHERS THEN
                IF (SQLCODE = -942) THEN  -- Table not existed error
                    EXECUTE IMMEDIATE 'CREATE TABLE {destination_table}(json_data NCLOB)';

                ELSE
                    DBMS_OUTPUT.PUT_LINE('Unknown error : '||SQLERRM);
                    RAISE;
                END IF;
        END;
        """,
    oracle_conn_id=oracle_conn_id,
    dag=dag
)
create_appsflyer_inapps_retargeting_dir = BashOperator(
    task_id='create_appsflyer_inapps_retargeting_dir',
    bash_command=f"""
        if [ ! -d "$AIRFLOW_HOME/{appsflyer_dir}" ]; then
        echo "Directory does not exist. Creating directory..."
        mkdir -p "$AIRFLOW_HOME/{appsflyer_dir}"
        echo "Directory created."
        else
        echo "Directory already exists."
        fi
    """,
    dag=dag,

)

oracle_insert_operator = PythonOperator(
    task_id = 'oracle_insert_operator',
    python_callable=extract_and_insert,
    op_kwargs={"dir_path":appsflyer_dir, "destination_table_name":destination_table, "oracle_conn_id":oracle_conn_id},
    dag=dag,
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


# Define the DAG dependencies
create_appsflyer_inapps_retargeting_dir >> sync_appsflyer_inapps_retargeting >> oracle_create_table >> oracle_insert_operator >> run_dbt_appsflyer_inapps_retargeting