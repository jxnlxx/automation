# -*- coding: utf-8 -*-
"""
Created on Thu May 21 12:37:16 2020

@author: Jon Lee
"""

import os
import time
import json
import requests
import openpyxl
import pandas as pd
import datetime as dt
import xlsxwriter as xl

from uuid import uuid4
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

'''
DETAILS

This request returns all keywords saved under the specified site.

Requires a STAT 'siteId' which can be obtained from the STAT API 
    (see stat_api_sites_all.py for more info)
    
Correct at June 2020:

JSON request URL:
    /keywords/list?site_id={siteId}[&start={start}][&results={results}]&format=json

REQUEST PARAMETER       FORMAT          REQUIRED        NOTES
site_id                 INTEGER         YES	
start                   INTEGER         NO              Default is 0 (zero indexed)
results                 INTEGER         NO              Default is 100, max is 5000

JSON typical response:

{  
   "Response":{  
      "responsecode":"200",
      "resultsreturned":"2",
      "Result":[  
         {  
            "Id":"3008",
            "Keyword":"shirt",
            "KeywordMarket":"US-en",
            "KeywordLocation":"Boston",
            "KeywordDevice":"Smartphone",
            "CreatedAt":"2011-01-25"
         },
         {  
            "Id":"3009",
            "Keyword":"shoes",
            "KeywordMarket":"US-en",
            "KeywordLocation":"Boston",
            "CreatedAt":"2011-01-25"
         }
      ]
   }
}
The output is a CV file saved in this folder:
L:\Commercial\Operations\Technical SEO\Automation\Setup

'''

## start_time for timing the script
start_time = dt.datetime.now().replace(microsecond=0)
SARC = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================


site_id = ''
date = ''

# for whole month, using input above
year = 2019
month = 12
days = monthrange(year,month)[1]
cutoff = f'{year}-{month:02d}-{days:02d}'

# =============================================================================
# SCRIPT
# =============================================================================

# =============================================================================
# Google Sheets Auth
# =============================================================================

print('\n'+'Logging into Google Sheets')
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name( google_auth, scope )
client = gspread.authorize(creds)

# =============================================================================
# Load 'Tech SEO Projects List' workbook from Google Sheets
# =============================================================================

print('\n'+'Loading Tech SEO Projects List from Google Sheets...')

## Load Tech SEO Projects List from Google Sheets
gspread_id = '1H3qkPyGolEbq3pMpHYEWEb0kZkudy6mnbJT90L2kl1k'
sheet = client.open_by_key(gspread_id)

# =============================================================================
# Load 'STAT Client List' sheet from 'Tech SEO Projects List'
# =============================================================================

print('\n'+'Loading Client List from Google Sheets...')

stat_client_list_gsheet = 'STAT Client List'
client_list = sheet.worksheet(stat_client_list_gsheet)

## Load stat_automation into a DataFrame
data = client_list.get_all_values()
headers = data.pop(0)
client_list = pd.DataFrame(data, columns=headers)


print('Client List received!')

# =============================================================================
# Load full site list from STAT API
# =============================================================================

print('\n'+'Requesting Site List from STAT...')

sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
response = requests.get(sites_all_url)
SARC += 1
response = response.json()

total_results = response.get('Response').get('totalresults')

site_list = response.get('Response').get('Result')
print('Site List received!')
site_list = pd.DataFrame(site_list)
 
# =============================================================================
# Filter site_list so it shows only tracked sites with >0 keywords
# =============================================================================

print('\n'+'Filtering site list...')

## Remove False values from 'Tracking' column to leave only tracked sites
site_list = site_list[site_list['Tracking'].str.contains('^true')]

## Ensure 'TotalKeywords' column is set to 'int' (for next line)
site_list['TotalKeywords'] = site_list['TotalKeywords'].astype(int)

## Remove sites that have 0 keywords
site_list = site_list.drop(site_list[site_list['TotalKeywords'] == 0].index)
print('Removed untracked sites!')

site_list = site_list.reset_index(drop=True)
# =============================================================================
# Cross-reference client_list and site_list for new URLs
# If site not present already:
#    1) add to client_urls list for messaging later
#    2) add to client_list DataFrame
# =============================================================================

existing_clients = list(client_list['Url'])
new_clients = []
for i in site_list.index:
    site_url = site_list['Url'][i]
    if site_url not in existing_clients:
        new_site = {
            'CreatedAt': site_list['CreatedAt'][i],
            'SiteId' : site_list['Id'][i],
            'Title': site_list['Title'][i],
            'Url' : site_list['Url'][i]
            }
        new_clients.append(site_url)
        client_list = client_list.append(new_site, ignore_index=True)
    else:
        continue

# =============================================================================
# Save client_list to Google Sheets
# =============================================================================

#sheet.values_clear('{0}!A:AZ'.format(stat_client_list_gsheet))
#set_with_dataframe(sheet.worksheet(stat_client_list_gsheet), client_list, include_index=False)

# =============================================================================
# Group dataframe by 'Url' to get a list of 'SiteId's
# =============================================================================

site_list = site_list.reset_index(drop=True)
site_list = site_list[['CreatedAt', 'Id', 'Title', 'Url']]
site_list = site_list.groupby('Url', as_index=False).aggregate(lambda x: list(x))

s = 1 # for showing progress through total sites
total_sites = len(site_list)

for i in site_list.index:
    site_url = site_list['Url'][i]
    save_name = site_url.replace('/','_') # Remove slashes so file will save properly
    site_ids = site_list['Id'][i]
    print('\n'+f'Requesting data for {site_url} ({s} of {total_sites})')
    time.sleep(1)
    n = 1
    for site in site_ids:
        print(f'{n}. {site}')
        n += 1
        url = f'{stat_base_url}/keywords/list?site_id={site}&results=5000&format=json'
        response = requests.get(url)
        SARC +=1
        response = response.json()
        kw_list = response.get('Response').get('Result')                                # extracts 'result' which is a list of dictionaries - 1 dict for each keyword
        #print('\n'+f'Retrieving data for {total_kws} keywords...')
        print(n, url)
        p = 1 # for showing progress through pagination
        while True:                                                                     # Loop through pages to get all data
            try:
                nextpage = response.get('Response').get('nextpage')                     # get 'nextpage' path from response
                url = stat_base_url + nextpage                                          # create URL from 'base_url and 'nextpage'
                print(p, url)                                                           # print n + url to show script is not idle
                response = requests.get(url)                                            # ping the new url to get the data
                response = response.json()                                              # convert the JSON response into dict
                new_kws = response.get('Response').get('Result')                        # extract 'Result' from dict
                kw_list = kw_list + new_kws                                             # append the data to the kw_list
                p += 1                                                                  # add 1 to 'n'
            except TypeError:# TypeError:                                               # if 'nextpage' is blank, this will yield a TypeError when attempting to append
                break                                                                   # this will be the last page, so break the loop
    
    ## Process keywords for export    
    print('\n'+'Processing...')
    df = pd.DataFrame()
    k = 0 # for showing progress through site keywords
    total_kws = len(kw_list)
    for item in kw_list:
        if k % 1000 == 0 and k != 0:
            print(f'Completed {k} of {total_kws}')
        k += 1
        try:
            temp = pd.DataFrame({
                'keyword_id' : item.get('Id'),
                'keyword' : item.get('Keyword'),
                'device' : item.get('KeywordDevice'),
                'market' : item.get('KeywordMarket'),
                'regional_search_volume' : item.get('KeywordStats').get('RegionalSearchVolume'),               
                'Jan' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jan'),
                'Feb' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Feb'),
                'Mar' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Mar'),
                'Apr' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Apr'),
                'May' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('May'),
                'Jun' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jun'),
                'Jul' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jul'),
                'Aug' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Aug'),
                'Sep' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Sep'),
                'Oct' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Oct'),
                'Nov' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Nov'),
                'Dec' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Dec')
                }, index=[0])
            df = df.append(temp, ignore_index=True)
        except:
            continue
    print(f'Completed {k} of {total_kws}')
    print(f'Saving {save_name}.csv...')
    df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Keyword Lists\{save_name}_keywords_list.csv', index=False)
    mid_time = dt.datetime.now().replace(microsecond=0)
    mid_time = mid_time - start_time
    s += 1
    print('\n'+f'Finished processing {site_url}!'
          '\n'f'Completed {s} of {total_sites} sites'
          '\n'+f'Time elapsed: {mid_time}')


# =============================================================================
# END
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!'
      '\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {SARC}')
    