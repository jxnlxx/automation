# deepcrawl.py

import requests
from deepcrawlcreds import dc_url, dc_key, dc_value # saved in env/Lib/site-packages

def get_token():
    response = requests.post(dc_url, auth=(dc_key, dc_value), verify=True) # auth with deepcrawl
    response = response.json()
    token = response['token'] # gets the token from the json response
    headers = {'X-Auth-Token':token} # create variable for adding into header of requests
    return headers