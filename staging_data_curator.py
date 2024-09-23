import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os

class staging_data_curator:
    def __init__(self, source_file_path, target_file_path):
        self.source_file_path = source_file_path
        self.target_file_path = target_file_path

    '''
        Loads data from source file and returns as json-object
    '''
    def load_source_data(self):
        with open(self.source_file_path, "r") as src:
            #Read file
            src_data = src.read()

            #Create json
            src_data_json = json.loads(src_data)
            return src_data_json


    '''
        Loads dataframe to parquet file
    '''
    def load_df_to_parquet(self, df, full_load):
        if os.path.exists(self.target_file_path) and full_load:
            os.remove(self.target_file_path)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, self.target_file_path)

    
