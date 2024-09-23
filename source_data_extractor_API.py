import requests, json, pandas as pd, os

class source_data_extractor_API:
    def __init__(self, endpoint, payload, target_file_path):
        self.endpoint = endpoint
        self.payload = payload
        self.target_file_path = target_file_path

    def get_posts(self):
        try:
            response = requests.post(self.endpoint, json = self.payload)
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                posts = response.json()
                return posts
            else:
                raise Exception('Error with API request : ', response.status_code)

        except requests.exceptions.RequestException as e:
            print('Error', e)
            return None
        
    def write_to_json(self, posts, full_load):
        if os.path.exists(self.target_file_path) and full_load:
            os.remove(self.target_file_path)

        json_object = json.dumps(posts, indent=4)
        with open(self.target_file_path, "w") as outfile:
            outfile.write(json_object)
