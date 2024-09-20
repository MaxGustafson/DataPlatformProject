'''
    Extract data from staging JSON files, filter out uneccesary data and store in curated parquet files. 
    Data should be relevant and cleaned.

'''
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os

def main(full_load = True):
    file_path_trg = target_config['file_path_trg']

    if os.path.exists(file_path_trg) and full_load:
        os.remove(file_path_trg)

    file_path_source = source_config['file_path_json']
    src_data_json = load_source_data(file_path_source)
    df = load_json_to_df(src_data_json)
    load_df_to_parquet(df,file_path_trg)


'''
    Loads data from source file and returns as json-object
'''
def load_source_data(file_path_src):
    with open(file_path_src, "r") as src:
        #Read file
        src_data = src.read()

        #Create json
        src_data_json = json.loads(src_data)
        return src_data_json

'''
    Clean data and load to Pandas dataframe object
'''
def load_json_to_df(src_data_json):

    df_data = pd.json_normalize(src_data_json['data'])
    data_norm = {
        'Political_Entity' : df_data['key'].apply(lambda row: row[0]),
        'Year'  : df_data['key'].apply(lambda row: row[1][:4]),
        'Month_Numeric' : df_data['key'].apply(lambda row: row[1][5:7]),
        'Time_Orig'  : df_data['key'].apply(lambda row: row[1]),
        'Voting_Percent' : df_data['values'].apply(lambda row: row[0])
    }
    
    df_norm = pd.DataFrame(data_norm)
    df_norm.insert(3,'Month_Str', df_norm['Month_Numeric'].apply(lambda row : months_look_up[row]))
    df_norm.insert(0,'Political_Entity_Voting_Key',df_norm.apply(lambda row: row['Political_Entity'] + '#' + row['Time_Orig'], axis = 1),allow_duplicates=False)
    return df_norm

'''
    Loads dataframe to parquet file
'''
def load_df_to_parquet(df, file_path_trg):
    table = pa.Table.from_pandas(df)
    print(table)
    pq.write_table(table, file_path_trg)
    

source_config = {
   'file_path_json':'./data/staging/ME0201A.json'
}

target_config = {
  'file_path_trg':'./data/raw/ME0201A.parquet'
}

months_look_up = {
    '01':'January',
    '02':'February',
    '03':'March',
    '04':'April',
    '05':'May',
    '06':'June',
    '07':'July',
    '08':'August',
    '09':'September',
    '10':'October',
    '11':'November',
    '12':'December'
}

if __name__ == "__main__":
    main()     
