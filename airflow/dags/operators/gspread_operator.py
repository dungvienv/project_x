from airflow.models.baseoperator import BaseOperator
import gspread
import gspread_dataframe as gd
from pyarrow import parquet as pq
import pandas as pd
import string
import time

class GoogleSheetUpdater(BaseOperator):
    def __init__(self, source_file_dir, spreadsheet_id, worksheet_name, **kwargs):
        super().__init__(**kwargs)
        self.source_file_dir = source_file_dir
        self.spreadsheet_id  = spreadsheet_id
        self.worksheet_name = worksheet_name

    def execute(self, context):
        gc = gspread.service_account('dags/service_accounts/google_sheet_bot.json')
        sh = gc.open_by_key(self.spreadsheet_id)
        worksheet = sh.worksheet(self.worksheet_name)
        worksheet.clear()

        df = pd.read_parquet(self.source_file_dir)
        df_col_end_idx = len(df.columns)
        alphabet = list(string.ascii_uppercase)
        ws_formatter = {
            'numberFormat':{
                'type':'TEXT'
            }
        }
        worksheet.format(f'A:{alphabet[df_col_end_idx]}',ws_formatter)
        gd.set_with_dataframe(row=1,include_column_header=True,dataframe=df,worksheet=worksheet)
                
        print('Data exported to Google Sheets successfully!')


