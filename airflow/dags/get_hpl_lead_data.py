from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.operators.python import PythonOperator
from operators.gspread_operator import GoogleSheetUpdater
from operators.audience_retriever import AudienceRetrieveOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import shutil,os

destination_table = 'CUSTOM_RAW_HPL_LEAD'
oracle_conn_id='oracle_conn'
local_destination="./hpl_lead_data"




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

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


dag = DAG(
    'hpl_lead_workflow',
    default_args=default_args,
    description='Retrieve data from Hadoop HPL lead data, transform, and ingest into Oracle database',
    schedule_interval=timedelta(hours=5),
    start_date=days_ago(1),
)

# Define DAG tasks

refresh_data = BashOperator(
    task_id='refresh_data',
    bash_command='./scripts/hpl_refresh.sh',
    dag=dag
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

oracle_insert_operator = PythonOperator(
    task_id = 'oracle_insert_operator',
    python_callable=extract_and_insert,
    op_kwargs={"dir_path":local_destination, "destination_table_name":destination_table, "oracle_conn_id":oracle_conn_id},
    dag=dag,
)


# Define task dependencies
refresh_data >> oracle_create_table >> oracle_insert_operator
