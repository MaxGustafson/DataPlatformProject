import requests, json, pandas as pd, os
import source_data_extractor_API as sde
import config_stage as cc

def main(full_load = True):
    #ME0201A
    data_extractor_ME0201A = sde.source_data_extractor_API(cc.source_config_ME0201A['endpoint'], cc.source_config_ME0201A['payload'], cc.target_config_ME0201A['file_path_json'])
    posts_ME0201A = data_extractor_ME0201A.get_posts()
    data_extractor_ME0201A.write_to_json(posts_ME0201A, full_load)

    #ME0104C
    data_extractor_ME0104C = sde.source_data_extractor_API(cc.source_config_ME0104C['endpoint'], cc.source_config_ME0104C['payload'], cc.target_config_ME0104C['file_path_json'])
    posts_ME0104C = data_extractor_ME0104C.get_posts()
    data_extractor_ME0104C.write_to_json(posts_ME0104C, full_load)

if __name__ == "__main__":
    full_load = True
    main(full_load)     

