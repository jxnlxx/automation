# -*- coding: utf-8 -*-
"""
Created on Thu May 21 12:37:16 2020

@author: Jon Lee
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
from getstat import stat_subdomain, stat_key, stat_base_url                                  # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

'''
DETAILS

This request returns the archived SERP for the specified search engine 
and date for a specified keyword.

Requires a STAT 'keywordId' which can be obtained from the STAT API 
    (see stat_api_keywords_list.py for more info)

Correct at June 2020:

JSON request URL:
    /serps/show?keyword_id={keywordId}&engine={engine}&date={YYYY-MM-DD}&format=json

REQUEST PARAMETER       FORMAT          REQUIRED        NOTES
keyword_id              INTEGER         YES	
engine                  STRING          YES             ‘google’ or ‘bing’
date                    YYYY-MM-DD      YES	




All Information retrieved:
    'Rank' Google Rank
    'BaseRank' : Google Base Rank
    'ResultType': whether it is 'regular', 'shopping', 'people also ask', etc.
    'Url': Ranking URL
    'Protocol': http, https

The output is a CSV file saved in this folder:
L:\Commercial\Operations\Technical SEO\Automation\Setup

'''

# =============================================================================
# SETTINGS
# =============================================================================

date = '2019-06-01'


client = 'Holland and Barrett UK'
site_id = '8296'
keyword_id = 1083761


#set a delay 
minute = 60
sleep_timer = minute*20

# replace keyword rank when not ranking with number
not_ranking = 120


# =============================================================================
# SCRIPT
# =============================================================================

url = f'{stat_base_url}/serps/show?keyword_id={keyword_id}&engine=google&date={date}&format=json'
response = requests.get(url)
response = response.json()
kw_list = response.get('Response').get('Result')


#kw_df = pd.DataFrame(columns=['ResultType', 'Rank', 'BaseRank', 'Url', 'Protocol'])
for item in kw_list:
    temp = {
        'ResultType' : item.get('ResultTypes').get('ResultType'),
        'Rank' : item.get('Rank'),
        'BaseRank' : item.get('BaseRank'),
        'Url' : item.get('Url'),
        'Protocol' : item.get('Protocol')
        }
    kw_df = kw_df.append(temp, ignore_index=True)
    
kw_df.to_csv( r'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_api_serps_show.csv', index = False)

print( '\n'+'DURN!')