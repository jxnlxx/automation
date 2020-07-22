# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 08:30:30 2020

@author: jon-lee@iprospect.com
"""

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

'''
DETAILS

This request returns all sites saved under your account.

The output contains general info about each site, but no keyword data.

Important information includes:
    'SiteId': a unique identifier for each site under 'Id' in response,
    'Tracking (TRUE or FALSE): whether the site currently has active keyword tracking,
    'RequestUrl': a path to append to 'base_url' for requesting keywords/list

JSON request URL:
    /sites/all?[start={start}][&results={results}&]format=json

JSON typical response:

{ 
   "Response":{ 
      "responsecode":"200",
      "totalresults":"100",
      "resultsreturned":"50",
      "nextpage":"/sites/all?start=50&format=json",
      "Result":[ 
         { 
            "Id":"1",
            "ProjectId":"13",
            "FolderId":"22",
            "FolderName":"Blog",
            "Title":"tourismvancouver.com",
            "Url":"tourismvancouver.com",
            "Synced":"N/A",
            "TotalKeywords":"63",
            "CreatedAt":"2011-01-25",
            "UpdatedAt":"2011-01-25",
            "RequestUrl":"/keywords/list?site_id=1&format=json"
         },
         { 
            "Id":"2",
            "ProjectId":"13",
            "FolderId":"N/A",
            "FolderName":"N/A",
            "Title":"perezhilton.com",
            "Url":"perezhilton.com",
            "Synced":"1",
            "TotalKeywords":"63",
            "CreatedAt":"2011-01-25",
            "UpdatedAt":"2011-01-25",
            "RequestUrl":"/keywords/list?site_id=2&format=json"
         },
         ...
      ]
   }
}


The output is a CSV file saved in this folder:
L:\Commercial\Operations\Technical SEO\Automation\Setup

current name of file is stat_api_sites_all.csv
'''

# =============================================================================
# SETTINGS
# =============================================================================

date = '2020-06-02'
from_date = '2020-05-01'
to_date = '2020-05-30'

year = 2020
month = 2

days_in_month = monthrange(year,month)[1]


#set a delay 
minute = 60
sleep_timer = minute*20

# replace keyword rank when not ranking with number
not_ranking = 120

#client_base = f'{client} Daily Ranks {month:02d}'


# =============================================================================
# SCRIPT
# =============================================================================

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

site_list.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_api_sites_all.csv', index = False)


print('\n'+'DURN!')