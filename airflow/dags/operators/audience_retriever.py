from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.models import BaseOperator
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

class AudienceRetrieveOperator(BaseOperator):
    """
    Operator for retrieving ads audience
    """
    def __init__(self, oracle_conn_id, destination, audience_file_dir, **kwargs) -> None:
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.destination = destination
        self.audience_file_dir = audience_file_dir

    def execute(self,context):
        try:
            with open(self.audience_file_dir, 'r') as sql_file:
                sql_script = sql_file.read()
        except FileNotFoundError:
            raise FileNotFoundError("The audience file doesn't exist, please check again!")
        
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn = oracle_hook.get_conn()

        # Create a df from sql file
        sql_df = pd.read_sql(sql_script, con=conn)

        # Convert the pandas DataFrame to a PyArrow Table
        parquet_table = pa.Table.from_pandas(df=sql_df)
        
        # Write the PyArrow Table to a Parquet file
        pq.write_table(parquet_table,self.destination)