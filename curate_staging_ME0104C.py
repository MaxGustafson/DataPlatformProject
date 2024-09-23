'''
    Extract data from staging JSON files, filter out uneccesary data and store in curated parquet files. 
    Data should be relevant and cleaned.

'''
import pandas as pd
import json
import staging_data_curator as sdc
import config_raw as cr

def main(full_load = True):
    data_curator = sdc.staging_data_curator(cr.source_config_ME0104C['file_path_json'], cr.target_config_ME0104C['file_path_trg'])

    src_data_json = data_curator.load_source_data()
    df = load_json_to_df(src_data_json)
    data_curator.load_df_to_parquet(df,full_load)

'''
    Clean data and load to Pandas dataframe object
'''
def load_json_to_df(src_data_json):

    df_data = pd.json_normalize(src_data_json['data'])
    
    data_norm = {
        'Political_Entity' : df_data['key'].apply(lambda row: row[0]),
        'Year'  : df_data['key'].apply(lambda row: row[1]),
        'Number_Of_Mandates' : df_data['values'].apply(lambda row: row[0])
    }
    
    df_norm = pd.DataFrame(data_norm)     
    return df_norm

if __name__ == "__main__":
    main()     
