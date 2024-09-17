import requests, json, pandas as pd, os
import pyarrow as pa
import pyarrow.parquet as pq

def main(nbr_of_users, full_load = True):
    file_path_json = './data/randomuser.json'
    if os.path.exists(file_path_json) and full_load:
        os.remove(file_path_json)

    posts = get_posts()
    write_to_json(posts)
    #write_to_parquet(posts, file_path_parquet)
    
def write_to_json(posts, json_file_path = './data/randomuser.json'):
    #Serializing json
    json_object = json.dumps(posts, indent=4)
    with open(json_file_path, "w") as outfile:
        outfile.write(json_object)

def write_to_parquet(posts, parquet_file_path = './data/randomuser.parquet'):
    df = pd.json_normalize(posts['results'])
    df =  df.astype({'location.postcode':str})
    print(df.loc[0])
    table = pa.Table.from_pandas(df)
    print(table)
    pq.write_table(table, parquet_file_path)
   
def get_posts():
    url = 'https://api.scb.se/OV0104/v1/doris/sv/ssd/START/ME/ME0201/ME0201A/Vid10'
    payload = {
            "query": [
                {
                "code": "ContentsCode",
                "selection": {
                    "filter": "item",
                    "values": [
                    "ME0201B1"
                    ]
                }
                }
            ],
            "response": {
                "format": "json"
            }
        }

    try:
        response = requests.post(url, json = payload)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            posts = response.json()
            return posts
        else:
            raise Exception('Error with API request : ', response.status_code)

    except requests.exceptions.RequestException as e:
        print('Error', e)
        return None

if __name__ == "__main__":
    full_load = False
    nbr_of_users = 1000
    main(nbr_of_users, full_load)