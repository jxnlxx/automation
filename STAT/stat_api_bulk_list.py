# %%
response
# %%
response
# %%
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 07:42:53 2020

@author: JLee35
"""
#%%

import datetime
import requests
import pandas as pd
from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

## start_time for timing the script
start_time = datetime.datetime.now().replace(microsecond=0)

# =============================================================================
# SETTINGS
# =============================================================================


export_url = stat_base_url + '/bulk/list?results=5000&format=json'

print('Requesting bulk list...')
response = requests.get(export_url)
response = response.json()
print('Data received!')
export = response.get('Response').get('Result')
df = pd.DataFrame()
print('Processing data...')
for item in export:
    row = item
    df = df.append(row, ignore_index=True)
print('Processing complete!')

print(r'Saving data in stat_api_bulk_list.csv')
df.to_csv(r'stat_api_bulk_list.csv', index=False)

print('\n'+'DURN!')

#stream_url= f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'