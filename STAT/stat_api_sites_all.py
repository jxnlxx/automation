# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 08:30:30 2020

@author: jon-lee@iprospect.com
"""
#%%
import os
import datetime
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse
from collections import defaultdict
from getstat import stat_subdomain, stat_key                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib


base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

sites_all_url = f'{base_url}/sites/all?&results=5000&format=json'
# url = f'{base_url}/sites/all?&format=json'

print('\n'+'Requesting data...')
response = requests.get(sites_all_url)
response = response.json()
print('\n'+'Data received!')
total_results = response.get('Response').get('totalresults')
print('\n'+'Processing..')
site_list = response.get('Response').get('Result')
site_list = pd.DataFrame(site_list)
#site_list = site_list.rename(columns={'Id':'SiteId'})
#site_list = site_list[['SiteId', 'Url', 'TotalKeywords', 'Tracking']]
#site_list = site_list[site_list['Tracking'].str.contains('^true')]

#for i in site_list.index:

print('\n'+'Exporting data...')

site_list.to_csv(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\stat_api_sites_all.csv', index = False)

print('\n'+'DURN!')
# %%
