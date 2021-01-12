# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 14:19:47 2020

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
from getstat import stat_subdomain, stat_key                                    # saved locally in r'C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib'

'''
DETAILS

This request returns the SoV score for each competitor domain that appears
on the SERP for a specified site

The output is a CSV file saved in this folder:
r'L:\Commercial\Operations\Technical SEO\Automation\Setup'

'''

# time script
start_time = datetime.datetime.now().replace(microsecond=0)

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

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'
url = f'{base_url}/sites/sov?id={site_id}&from_date={from_date}&to_date={to_date}&results=5000&format=json'

response = requests.get(url)
response = response.json()

total_results = response.get('Response').get('totalresults')
sov_list = response.get('Response').get('ShareOfVoice')
#sov_list = pd.DataFrame.from_dict(sov_list)
#sov_list.to_csv( r'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Setup\stat_sov_sites_response_(basic).csv', index = False)

n= 1
print(n, url)
while True:
    try:
        url = base_url + response.get('Response').get('nextpage')       # if not, change the url to the 'next' page in the pagination
        response = requests.get( url )                                  # ping the new url to get the data
        response = response.json()                                      # convert the JSON response into something pythonic
        new_page = response.get('Response').get('ShareOfVoice')
        sov_list = sov_list + new_page                                  # append the data to the kw_list
        n += 1                                                          # add 1 to 'n'
        print( n, url )
    except TypeError:
        break
df = pd.DataFrame(columns=['Date', 'Domain', 'Share'])
n=0
for item in sov_list:
    sov_date = item.get('date')
    sov_site = item.get('Site')
    for site in sov_site:
        details = {
            'Date': sov_date,
            'Domain' : site.get('Domain'),
            'Share' : site.get('Share')
            }
        df= df.append(details, ignore_index = True)

df.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_api_sov_sites.csv', index = False)

end_time = datetime.datetime.now().replace(microsecond=0)

dc_time = mid_time - start_time
xl_time = end_time - mid_time
total_time = end_time - start_time


print(f'Time taken to get data from DeepCrawl: {dc_time}'+'\n'
      f'Time taken to generate Excel healthchecks: {xl_time}'+'\n'
      f'Total time taken: {total_time}')

print('\n'+'DURN!')

end_time = datetime.datetime.now().replace(microsecond=0)

total_time = end_time - start_time


print(f'Total time taken: {total_time}')