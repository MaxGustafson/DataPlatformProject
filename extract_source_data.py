import requests, json, pandas as pd, os

def main(full_load = True):
    file_path_json = target_config['file_path_json']
    
    if os.path.exists(file_path_json) and full_load:
        os.remove(file_path_json)

    posts = get_posts()
    write_to_json(posts, file_path_json)
    
def write_to_json(posts, json_file_path):
    json_object = json.dumps(posts, indent=4)
    with open(json_file_path, "w") as outfile:
        outfile.write(json_object)
   
def get_posts():
    try:
        response = requests.post(source_config['endpoint'], json = source_config['payload'])
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            posts = response.json()
            return posts
        else:
            raise Exception('Error with API request : ', response.status_code)

    except requests.exceptions.RequestException as e:
        print('Error', e)
        return None
 
source_config = {
    'endpoint': 'https://api.scb.se/OV0104/v1/doris/sv/ssd/START/ME/ME0201/ME0201A/Vid10',
    'payload' :  { 
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

}

target_config = {
  'file_path_json':'./data/staging/ME0201A.json'
}

if __name__ == "__main__":
    full_load = False
    main(full_load)     

