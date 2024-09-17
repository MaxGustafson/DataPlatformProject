import requests, json, pandas as pd, os
import pyarrow as pa
import pyarrow.parquet as pq

def main(nbr_of_users, full_load = True):
    #file_path_csv = './data/randomuser.csv'
    file_path_parquet = './data/randomuser.parquet'
    if os.path.exists(file_path_parquet) and full_load:
        os.remove(file_path_parquet)

    posts = get_posts(nbr_of_users)
    #write_to_csv(posts, file_path_csv)
    write_to_parquet(posts, file_path_parquet)
    

def write_to_parquet(posts, parquet_file_path = './data/randomuser.parquet'):
    df = pd.json_normalize(posts['results'])
    df =  df.astype({'location.postcode':str})
    print(df.loc[0])
    table = pa.Table.from_pandas(df)
    print(table)
    pq.write_table(table, parquet_file_path)


def write_to_csv(posts, file_path):
    df = pd.json_normalize(posts['results'])
    df.to_csv(file_path, mode='a', header=not os.path.exists(file_path))
   
    
def get_posts(nbr_of_users):
    #url = 'http://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0401/BE0401B/BefProgFoddaMedel11' #Put in YAML later
    url = 'https://randomuser.me/api?results=' + str(nbr_of_users)
    try:
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            posts = response.json()
            return posts
        else:
            print('Error:', response.status_code)
            return None

    except requests.exceptions.RequestException as e:
        print('Error', e)
        return None

if __name__ == "__main__":
    full_load = True
    nbr_of_users = 1000
    main(nbr_of_users, full_load)