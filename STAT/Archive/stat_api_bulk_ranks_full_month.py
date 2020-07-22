# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 10:21:48 2020

@author: JLee35
"""
import os
import time
import datetime
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse
import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery                                 # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib
from getstat import stat_subdomain, stat_key                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib


# =============================================================================
# SETTINGS
# =============================================================================

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

site_id = ''
date = ''

sites_all_url = f'{base_url}/sites/all?&results=5000&format=json'
keywords_list_url = f'{base_url}/keywords/list?site_id={site_id}&results=5000&format=json'
bulk_ranks_url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'

year = 2020
month = 2
#days_in_month = monthrange(year,month)[1]
days_in_month = 1

#set a delay 
minute = 60

sleep_timer = minute*20

# replace keyword rank when not ranking with number
not_ranking = 120

client_base = '{client} Daily Ranks {month}'


# =============================================================================
# SCRIPT
# =============================================================================


# =============================================================================
# Log into Google Sheets
# =============================================================================

print('\n'+'Logging into Google Sheets')
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name( google_auth, scope )
client = gspread.authorize(creds)

# =============================================================================
# Load incremental_id to for primary key
# =============================================================================

print('\n'+'Loading Incremental ID from Google Sheets...')
gspread_incremental_id = '11o64o422YMR6-wkYDQgrrjN60nc0gvzks2aZ306Yt2c'
gsheet_name_incremental_id = 'Incremental ID'
incremental_id_sheet = client.open_by_key(gspread_incremental_id)
incremental_id_worksheet = incremental_id_sheet.worksheet(gsheet_name_incremental_id)

## Load the sheet into a DataFrame with index set at col 0 and header set as row 0
incremental_id = get_as_dataframe(incremental_id_worksheet, header=0, index_col=0)
## Remove NaN to save memory
incremental_id = incremental_id.dropna(how='all')
incremental_id = incremental_id.dropna(axis=1)

pk_id = int(incremental_id.loc['keyword_id', 'Id'])
print('\n'+f'Incremental ID is: {pk_id}')

# =============================================================================
# Load client list from google sheets
# =============================================================================

print('\n'+'Loading Client list from Google Sheets')

## Load tech_seo_client_list from Google Sheets
gspread_id = '1H3qkPyGolEbq3pMpHYEWEb0kZkudy6mnbJT90L2kl1k'
gsheet_name = 'Client List'
sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

## Load tech_seo_client_list data to dataframe
worksheet = worksheet.get_all_values()
headers = worksheet.pop(0)
client_list = pd.DataFrame(worksheet, columns=headers)
print('\n'+'Client list received!')

# =============================================================================
# Load full site list from STAT API
# =============================================================================

print('\n'+'Requesting site list from STAT...')
response = requests.get(sites_all_url)
response = response.json()

total_results = response.get('Response').get('totalresults')

site_list = response.get('Response').get('Result')
print('\n'+'Site list received!')
site_list = pd.DataFrame(site_list)
 
site_list = site_list.rename(columns={'Id':'SiteId'})

# =============================================================================
# Change data types in site_list
# =============================================================================



# =============================================================================
# Filter site_list so it shows only rows tracked keywords
# =============================================================================

print('\n'+'Filtering site list...')

## Remove False values from 'Tracking' column to leave only tracked sites
site_list = site_list[site_list['Tracking'].str.contains('^true')]

## Ensure 'TotalKeywords' column is set to 'int'
site_list['TotalKeywords'] = site_list['TotalKeywords'].astype(int)

## Remove sites that have 0 keywords
site_list = site_list.drop( site_list[site_list['TotalKeywords'] == 0].index )

total_kws = site_list['TotalKeywords'].sum()
total_sites = len(site_list)

# =============================================================================
# Request bulk ranks
# =============================================================================

jobs_df = pd.DataFrame(columns=['Date','JobId'])
x = 1
while x <= days_in_month:
    date = f'{year}-{month:02d}-{x:02d}' # the ':02d' changes 1 to 01, 2 to 02, etc.
    for i in site_list.index:
        site_id = site_list['SiteId'][i]
        client = site_list['Url'][i]
            # for each client, call API and request reports for date
        url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
        print(f'Requesting Ranks Report for {client}, date: {date}')
            
        # request bulk ranks from url (block above), adding 'date' and 'job_id' to jobs_df
        response = requests.get(url)
        response = response.json()
        job_id = response.get('Response').get('Result').get('Id')
        job_dict = {
            'Date' : date, 
            'JobId' : job_id
            }
        jobs_df = jobs_df.append( job_dict, ignore_index = True )
        x += 1


# =============================================================================
# Wait for reports to generate
# =============================================================================
print('\nWaiting for reports to run: sleeping for', sleep_timer/60, 'mins...'
      '\n\nStarting Timer:'
      '\n'+time.strftime('%M:%S', time.gmtime(0)))
seconds = 20
while seconds < sleep_timer:
    time.sleep(20)
    print(time.strftime('%M:%S', time.gmtime(seconds)))
    seconds += 20

print(time.strftime('%M:%S', time.gmtime(seconds)),
      '\nTimer Finished'
      '\nDownloading Reports')

# generate base report using first date in jobs_df. 'n' is for jobs_df 
jobs_df = pd.read_excel('jobs_df.xlsx')

n = 0
date = jobs_df.iloc[n]['Date']
job_id = jobs_df.iloc[n]['JobId']

# call server to get 'StreamUrl',   

response = requests.get(f'{base_url}/bulk/status?id={job_id}&format=json')
response = response.json()
export_url = response.get('Response').get('Result').get('StreamUrl')

# send request to 'StreamUrl' to get data, and extract keyword data
response = requests.get(export_url)
response = response.json()
export = response.get('Response').get('Project').get('Site').get('Keyword')
total_kws = len(export)

# dissect response and add to keyword_df. 'y' is for keyword count
keyword_df = pd.DataFrame()
y = 1
for item in export:
    print(f'Fetching {y} of {total_kws} -',item.get('Keyword'))
    try:
        base_rank = int(item.get('Ranking').get('Google').get('BaseRank'))
    except ValueError:
        base_rank = not_ranking
    try:
        regional_search_volume = int(item.get('KeywordStats').get('TargetedSearchVolume'))
    except ValueError:
        regional_search_volume = 0
    kw_dict = {
    'keyword' : item.get('Keyword'),
    'device' : item.get('KeywordDevice'),
    'regional_search_volume' : regional_search_volume,
    'keyword_tags' : item.get('KeywordTags'),
    f'{date} base_rank' : base_rank
    }     
    keyword_df = keyword_df.append( kw_dict, ignore_index = True )
    y += 1

# save dataframe to excel file
print(f'saving {client} ranks')
keyword_df = keyword_df[['keyword', 'device', 'regional_search_volume','keyword_tags', f'{date} base_rank']]


n += 1
# iterate through rest of jobs_df to get reports, and add to keyword_df
while n < days_in_month:
    date = jobs_df.iloc[n]['Date']
    job_id = jobs_df.iloc[n]['JobId']
    # send request to 'Status' url to get report url ('StreamUrl')
    response = requests.get(f'{base_url}/bulk/status?id={job_id}&format=json')
    response = response.json()
    export_url = response.get('Response').get('Result').get('StreamUrl')

    # send request to 'StreamUrl' to get data, and extract keyword data
    response = requests.get(export_url)
    response = response.json()
    export = response.get('Response').get('Project').get('Site').get('Keyword')
    total_kws = len(export)

    # dissect response and add to keyword_df2
    print(f'Collecting data for {date}')
    time.sleep(1)

    keyword_df2 = pd.DataFrame()
    y = 1
    for item in export:
        print(f'Fetching {y} of {total_kws} -',item.get('Keyword'))
        y += 1
        try:
            base_rank = int(item.get('Ranking').get('Google').get('BaseRank'))
        except (ValueError, TypeError):
            base_rank = not_ranking
        try:
            regional_search_volume = int(item.get('KeywordStats').get('TargetedSearchVolume'))
        except (ValueError, TypeError):
            regional_search_volume = 0
        kw_dict = {
        'keyword' : item.get('Keyword'),
        'device' : item.get('KeywordDevice'),
        f'{date} base_rank' : base_rank
        }     
        keyword_df2 = keyword_df2.append( kw_dict, ignore_index = True )
        
    # add keyword_df2 data to keyword_df for current date
    print(f'Merging {date} ranks')
    keyword_df2 = keyword_df2[['keyword', 'device', f'{date} base_rank']]
    keyword_df = keyword_df.merge(keyword_df2, how='left', on=['keyword','device'])
    n += 1

# set up excel file to put data in

# =============================================================================
# site_list = pd.read_excel
# 
# jobs_df = pd.DataFrame(columns=['Date','JobId'])
# 
# x = 1
# while x <= days_in_month:
#     date = f'{year}-{month:02d}-{x:02d}' # the ':02d' changes 1 to 01, 2 to 02, etc.
#     # for each client, call API and request reports for date
#     url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
#     print(f'Requesting Ranks Report for {client}, date: {date}')
#     
#     # request bulk ranks from url (block above), adding 'date' and 'job_id' to jobs_df
#     response = requests.get(url)
#     response = response.json()
#     job_id = response.get('Response').get('Result').get('Id')
#     job_dict = {
#         'Date' : date, 
#         'JobId' : job_id
#         }
#     jobs_df = jobs_df.append( job_dict, ignore_index = True )
#     x += 1
# 
# 
# # wait for reports to generate
# print('\nWaiting for reports to run: sleeping for', sleep_timer/60, 'mins...'
#       '\n\nStarting Timer:'
#       '\n'+time.strftime('%M:%S', time.gmtime(0)))
# seconds = 20
# while seconds < sleep_timer:
#     time.sleep(20)
#     print(time.strftime('%M:%S', time.gmtime(seconds)))
#     seconds += 20
# 
# print(time.strftime('%M:%S', time.gmtime(seconds)),
#       '\nTimer Finished'
#       '\nDownloading Reports')
# 
# # generate base report using first date in jobs_df. 'n' is for jobs_df 
# jobs_df = pd.read_excel('jobs_df.xlsx')
# 
# n = 0
# date = jobs_df.iloc[n]['Date']
# job_id = jobs_df.iloc[n]['JobId']
# 
# # call server to get 'StreamUrl',   
# 
# response = requests.get(f'{base_url}/bulk/status?id={job_id}&format=json')
# response = response.json()
# export_url = response.get('Response').get('Result').get('StreamUrl')
# 
# # send request to 'StreamUrl' to get data, and extract keyword data
# response = requests.get(export_url)
# response = response.json()
# export = response.get('Response').get('Project').get('Site').get('Keyword')
# total_kws = len(export)
# 
# # dissect response and add to keyword_df. 'y' is for keyword count
# keyword_df = pd.DataFrame()
# y = 1
# for item in export:
#     print(f'Fetching {y} of {total_kws} -',item.get('Keyword'))
#     try:
#         base_rank = int(item.get('Ranking').get('Google').get('BaseRank'))
#     except ValueError:
#         base_rank = not_ranking
#     try:
#         regional_search_volume = int(item.get('KeywordStats').get('TargetedSearchVolume'))
#     except ValueError:
#         regional_search_volume = 0
#     kw_dict = {
#     'keyword' : item.get('Keyword'),
#     'device' : item.get('KeywordDevice'),
#     'regional_search_volume' : regional_search_volume,
#     'keyword_tags' : item.get('KeywordTags'),
#     f'{date} base_rank' : base_rank
#     }     
#     keyword_df = keyword_df.append( kw_dict, ignore_index = True )
#     y += 1
# 
# # save dataframe to excel file
# print(f'saving {client} ranks')
# keyword_df = keyword_df[['keyword', 'device', 'regional_search_volume','keyword_tags', f'{date} base_rank']]
# 
# 
# n += 1
# # iterate through rest of jobs_df to get reports, and add to keyword_df
# while n < days_in_month:
#     date = jobs_df.iloc[n]['Date']
#     job_id = jobs_df.iloc[n]['JobId']
#     # send request to 'Status' url to get report url ('StreamUrl')
#     response = requests.get(f'{base_url}/bulk/status?id={job_id}&format=json')
#     response = response.json()
#     export_url = response.get('Response').get('Result').get('StreamUrl')
# 
#     # send request to 'StreamUrl' to get data, and extract keyword data
#     response = requests.get(export_url)
#     response = response.json()
#     export = response.get('Response').get('Project').get('Site').get('Keyword')
#     total_kws = len(export)
# 
#     # dissect response and add to keyword_df2
#     print(f'Collecting data for {date}')
#     time.sleep(1)
# 
#     keyword_df2 = pd.DataFrame()
#     y = 1
#     for item in export:
#         print(f'Fetching {y} of {total_kws} -',item.get('Keyword'))
#         y += 1
#         try:
#             base_rank = int(item.get('Ranking').get('Google').get('BaseRank'))
#         except (ValueError, TypeError):
#             base_rank = not_ranking
#         try:
#             regional_search_volume = int(item.get('KeywordStats').get('TargetedSearchVolume'))
#         except (ValueError, TypeError):
#             regional_search_volume = 0
#         kw_dict = {
#         'keyword' : item.get('Keyword'),
#         'device' : item.get('KeywordDevice'),
#         f'{date} base_rank' : base_rank
#         }     
#         keyword_df2 = keyword_df2.append( kw_dict, ignore_index = True )
#         
#     # add keyword_df2 data to keyword_df for current date
#     print(f'Merging {date} ranks')
#     keyword_df2 = keyword_df2[['keyword', 'device', f'{date} base_rank']]
#     keyword_df = keyword_df.merge(keyword_df2, how='left', on=['keyword','device'])
#     n += 1
# 
# =============================================================================
print(f'Exporting data...')
keyword_df.to_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_api_bulk_ranks_{month} - {client} Daily Ranks.xlsx', index=False )    

print('DURN!')

