# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 12:51:54 2020

@author: JLee35
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
from getstat import stat_subdomain, stat_key                                    # saved locally in C:\Users\JLee35\AppData\Local\Programs\Python\Python37-32\Lib

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'


# =============================================================================
# SETTINGS
# =============================================================================

date = '2020-06-02'
from_date = '2020-05-01'
to_date = '2020-05-30'

year = 2020
month = 2

days_in_month = monthrange(year,month)[1]


client = 'Holland and Barrett UK'
site_id = '8296'
keyword_id = 1083761
#keyword_id = 12248158


#set a delay 
minute = 60
sleep_timer = minute*20

# replace keyword rank when not ranking with number
not_ranking = 120

client_base = f'{client} Daily Ranks {month:02d}'

# =============================================================================
# SCRIPT
# =============================================================================


url = f'{base_url}/rankings/list?keyword_id={keyword_id}&from_date={from_date}&to_date={to_date}&format=json'

response = requests.get(url)
response = response.json()
kw_df = pd.DataFrame.from_dict(response)
kw_df.to_csv( r'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_rankings_list_response_(basic).csv', index = False)

print('\n'+'DURN!')
