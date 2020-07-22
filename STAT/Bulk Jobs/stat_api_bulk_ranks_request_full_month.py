# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 07:38:35 2020

First draft completed on 17/06/2020

@author: jon.lee@iprospect.com
"""

import os
import time
import json
import datetime
import requests
import openpyxl
import pandas as pd
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

from getstat import stat_subdomain, stat_key                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

'''
DETAILS

This script pings the STAT api to 1) request a report data for all sites on STAT.

The output contains general info about each site, but no keyword data.

Important information includes:
    'SiteId': a unique identifier for each site,
    'Tracking (TRUE or FALSE): whether the site currently has active keyword tracking,
    'RequestUrl': a path to append to 'base_url' for requesting keywords/list

The output is a CSV file saved in this folder:
L:\Commercial\Operations\Technical SEO\Automation\Setup

current name of file is stat_keywords_list_response_(Response_Result)_processed.csv
'''

## start_time for timing the script
start_time = datetime.datetime.now().replace(microsecond=0)
SRC = 0

# =============================================================================
# SETTINGS
# =============================================================================

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

site_id = ''
date = ''

sites_all_url = f'{base_url}/sites/all?&results=5000&format=json'
keywords_list_url = f'{base_url}/keywords/list?site_id={site_id}&results=5000&format=json'
bulk_ranks_url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'

# for whole month, using input above
year = 2020
month = 1
days = monthrange(year,month)[1]

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
# Load 'STAT Bulk Jobs (Daily)' sheet from 'Tech SEO Projects List'
# =============================================================================

print('\n'+'Loading Stat Bulk Jobs (Daily) from Google Sheets...')

stat_bulk_jobs_gsheet = 'STAT Bulk Jobs (Daily)'
bulk_jobs = sheet.worksheet(stat_bulk_jobs_gsheet)

## Load stat_automation into a DataFrame
data = bulk_jobs.get_all_values()
headers = data.pop(0)
bulk_jobs = pd.DataFrame(data, columns=headers)
#, converters={'Tags': lambda x: x[1:-1].split(',')})
bulk_jobs = bulk_jobs.set_index('Url')

print('STAT Bulk Jobs (Daily) Loaded!')

# =============================================================================
# Load full site list from STAT API
# =============================================================================

print('\n'+'Requesting Site List from STAT...')
response = requests.get(sites_all_url)
SRC += 1
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

## Get values for total no. of keywords and total no. of sites
total_kws = site_list['TotalKeywords'].sum()
total_sites = len(site_list)

# =============================================================================
# Request Bulk Ranks for sites in site_list on 'report_date' 
# =============================================================================

t = 1
all_jobs = site_list[['Url']]
all_jobs = all_jobs.set_index('Url')
while t <= days:
    date = f'{year}-{month:02d}-{t:02d}'
    print('\n'+f'Requesting bulk rank exports for {date}')

# for each site_id in site_list, call API and request reports for date

    n = 1 # For displaying n of 'total_sites' 

    job = pd.DataFrame(columns=['Url', f'{date}'])

    for i in site_list.index:
        # pull Id from client_list and create url 
        site_url = site_list['Url'][i]
        site_id = site_list['Id'][i]
        try:
            print(f'{n:03d} Requesting Ranks Report for {site_url} date: {date}')
            url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
        
            # request bulk ranks from url, adding job_id to list
            response = requests.get(url)
            SRC += 1
            response = response.json()
            job_id = response.get('Response').get('Result').get('Id')
            temp = {
                'Url': site_url,
                f'{date}' : job_id
                }
            job = job.append(temp, ignore_index=True)
            n += 1
        except:
            continue
    job = job.groupby('Url', as_index=True).aggregate(lambda x: list(x)) # as_index defaults to 'True'; it's there just to make this explicit when reverencing this code in future!
    all_jobs = pd.concat([all_jobs, job],axis=1,sort=False)
    print('\n'+f'Bulk rank requests for {date} complete!')
    n = 1 # reset counter
    t += 1 # add to days
    print('Sleeping for 5 secs')
    time.sleep(5)    


# =============================================================================
# Save Bulk Jobs (Monthly) to CSV
# =============================================================================

all_jobs.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Historical\{year}_{month}_bulk_ranks_all.csv')

end_time = datetime.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {SRC}')
