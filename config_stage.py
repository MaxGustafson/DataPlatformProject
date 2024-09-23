source_config_ME0201A = {
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

target_config_ME0201A = {
  'file_path_json':'./data/staging/ME0201A.json'
}

source_config_ME0104C ={
  'endpoint' : 'https://api.scb.se/OV0104/v1/doris/sv/ssd/START/ME/ME0104/ME0104C/Riksdagsmandat',
  'payload'  :  {
                "query": [
                  {
                    "code": "Region",
                    "selection": {
                      "filter": "vs:RegionValkretsTot99",
                      "values": []
                    }
                  },
                  {
                    "code": "Parti",
                    "selection": {
                      "filter": "item",
                      "values": [
                        "M",
                        "C",
                        "FP",
                        "KD",
                        "MP",
                        "NYD",
                        "S",
                        "V",
                        "SD"
                      ]
                    }
                  }
                ],
                "response": {
                  "format": "json"
                }
              }
}

target_config_ME0104C = {
  'file_path_json':'./data/staging/ME0104C.json'
}


