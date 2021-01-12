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
#request_counter = 0 # stat requests counter


# =============================================================================
# SCRIPT
# =============================================================================

# =============================================================================
# Load full site list from STAT API
# =============================================================================

print('\n'+'Requesting Site List from STAT...')

sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
response = requests.get(sites_all_url)
request_counter = 1
response = response.json()

total_results = response.get('Response').get('totalresults')

site_list = response.get('Response').get('Result')
print('Site List received!')
site_list = pd.DataFrame(site_list)

#save site list for reference
site_list.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Historical\stat_sites_all.csv', index=False)

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
# Group dataframe by 'Url' to get a list of 'SiteId's
# =============================================================================

site_list = site_list.reset_index(drop=True)
site_list = site_list[['CreatedAt', 'Id', 'Title', 'Url']]
site_list = site_list.groupby('Url', as_index=False).aggregate(lambda x : list(x))

for i in site_list.index:
    site_url = site_list['Url'][i]
    save_name = site_url.replace('/','_') # Remove slashes so file will save properly
    site_ids = site_list['Id'][i]
    print('\n'+f'Requesting data for {site_url}'
          '\n'+f'({i+1} of {len(site_list)})')
    n = 1 # for showing progress through site ids
    p = 1 # for showing progress through pagination
    for site_id in site_ids:
        print(f'{n}. {site_id}')
        n += 1
        keywords_list = f'/keywords/list?site_id={site_id}&results=5000&format=json'
        url = stat_base_url + keywords_list
        response = requests.get(url)
        request_counter += 1
        response = response.json()
        kw_list = response.get('Response').get('Result')                                # extracts 'result' which is a list of dictionaries - 1 dict for each keyword
        #print('\n'+f'Retrieving data for {total_kws} keywords...')
        print(p, keywords_list)
        p += 1 # for showing progress through pagination
        while True:                                                                     # Loop through pages to get all data
            try:
                nextpage = response.get('Response').get('nextpage')                     # get 'nextpage' path from response
                url = stat_base_url + nextpage                                          # create URL from 'base_url and 'nextpage'
                print(p, nextpage)                                                           # print n + url to show script is not idle
                response = requests.get(url)                                            # ping the new url to get the data
                response = response.json()                                              # convert the JSON response into dict
                new_kws = response.get('Response').get('Result')                        # extract 'Result' from dict
                kw_list = kw_list + new_kws                                             # append the data to the kw_list
                p += 1                                                                  # add 1 to 'n'
            except TypeError:# TypeError:                                               # if 'nextpage' is blank, this will yield a TypeError when attempting to append
                break                                                                   # this will be the last page, so break the loop

    # process keywords for export
    total_kws = len(kw_list)

    print(f'Processing {total_kws} keywords...')
    df = pd.DataFrame({
        'created_at' : [i['CreatedAt'] for i in kw_list],
        'keyword_id' : [i['Id'] for i in kw_list],
        'keyword' : [i['Keyword'] for i in kw_list],
        'device' : [i['KeywordDevice'] for i in kw_list],
        'market' : [i['KeywordMarket'] for i in kw_list],
        'regional_search_volume' : [i['KeywordStats']['TargetedSearchVolume'] for i in kw_list],
        'jan' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Jan'] for i in kw_list],
        'feb' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Feb'] for i in kw_list],
        'mar' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Mar'] for i in kw_list],
        'apr' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Apr'] for i in kw_list],
        'may' : [i['KeywordStats']['LocalSearchTrendsByMonth']['May'] for i in kw_list],
        'jun' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Jun'] for i in kw_list],
        'jul' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Jul'] for i in kw_list],
        'aug' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Aug'] for i in kw_list],
        'sep' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Sep'] for i in kw_list],
        'oct' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Oct'] for i in kw_list],
        'nov' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Nov'] for i in kw_list],
        'dec' : [i['KeywordStats']['LocalSearchTrendsByMonth']['Dec'] for i in kw_list]
        })


# =============================================================================
#     df = pd.DataFrame()
#     k = 0 # for showing progress through site keywords
#     total_kws = len(kw_list)
#
#     for item in kw_list:
#         if k % 1000 == 0 and k != 0:
#             print(f'Completed {k} of {total_kws}')
#         k += 1
#         try:
#             temp = pd.DataFrame({
#                 'created_at' : item.get('CreatedAt'),
#                 'keyword_id' : item.get('Id'),
#                 'keyword' : item.get('Keyword'),
#                 'device' : item.get('KeywordDevice'),
#                 'market' : item.get('KeywordMarket'),
#                 'regional_search_volume' : item.get('KeywordStats').get('RegionalSearchVolume'),
#                 'jan' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jan'),
#                 'feb' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Feb'),
#                 'mar' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Mar'),
#                 'apr' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Apr'),
#                 'may' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('May'),
#                 'jun' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jun'),
#                 'jul' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Jul'),
#                 'aug' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Aug'),
#                 'sep' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Sep'),
#                 'oct' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Oct'),
#                 'nov' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Nov'),
#                 'dec' : item.get('KeywordStats').get('LocalSearchTrendsByMonth').get('Dec')
#                 }, index=[0])
#             df = df.append(temp, ignore_index=True)
#         except:
#             continue
# =============================================================================

    print(f'Processing complete!')
    print(f'Saving {save_name}.csv...')
    df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Historical\Keywords List\{save_name}_keywords_list.csv', index=False)
    mid_time = dt.datetime.now().replace(microsecond=0)
    mid_time = mid_time - start_time
    print('\n'+f'Finished processing {site_url}!'
          '\n'f'Completed {i+1} of {len(site_list)} sites'
          '\n'+f'Time elapsed: {mid_time}')


# =============================================================================
# END
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!'
      '\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {request_counter}')
