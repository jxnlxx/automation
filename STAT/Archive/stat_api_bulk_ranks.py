# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 10:21:48 2020

@author: JLee35
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

# =============================================================================
# START TIMER
# =============================================================================

start_time = datetime.datetime.now().replace(microsecond=0)
SRC = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

site_id = ''
date = ''

sites_all_url = f'{base_url}/sites/all?&results=5000&format=json'
keywords_list_url = f'{base_url}/keywords/list?site_id={site_id}&results=5000&format=json'
bulk_ranks_url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'

# when not ranking, replace blanks with number
not_ranking = 120

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
SRC += 1 # stat requests counter
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
# Cross-reference client_list and site_list for new URLs
# If site not present already:
#    1) add to client_urls list for messaging later
#    2) add to client_list DataFrame
# =============================================================================

print('Checking for new sites...')
existing_clients = list(client_list['Url'])
new_clients = []
for i in site_list.index:
    site_url = site_list['Url'][i]
    new_site = {
        'SiteId' : site_list['Id'][i],
        'Title': site_list['Title'][i],
        'Url' : site_list['Url'][i],
        'CreatedAt' : site_list['CreatedAt'][i]
        }
    client_list = client_list.append(new_site, ignore_index=True)
    if site_url not in existing_clients:
        new_clients.append(site_url)
    else:
        continue

if len(new_clients) != 0:
    print('\n'+'New sites found!')
    print('\n'+'Saving New Sites...')
    sheet.values_clear('{0}!A:AZ'.format(stat_client_list_gsheet))
    set_with_dataframe(sheet.worksheet(stat_client_list_gsheet), client_list, include_index=False)    
    
    for item in new_clients:
        print(item)

# =============================================================================
# Request Bulk Ranks for sites in site_list on 'report_date' days ago
# =============================================================================

date = datetime.date.today() - datetime.timedelta(days=7)
date = date.strftime('%Y-%m-%d')

print('\n'+f'Requesting bulk rank exports for {date}')

# for each site_id in site_list, call API and request reports for date

n = 1 # For displaying n of 'total_sites' 

new_bulk_jobs = pd.DataFrame(columns=['Url', f'{date}'])

for i in site_list.index:
    # pull Id from client_list and create url 
    site_url = site_list['Url'][i]
    site_id = site_list['Id'][i]
    try:
        print(n,f'Requesting Ranks Report for {site_url} date: {date}')
        url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
    
        # request bulk ranks from url (block above), adding job_id to list
        response = requests.get(url)
        SRC += 1 # stat requests counter
        response = response.json()
        job_id = response.get('Response').get('Result').get('Id')
        temp = {
            'Url': site_url,
            f'{date}' : job_id
            }
        new_bulk_jobs = new_bulk_jobs.append(temp, ignore_index=True)
        n += 1
    except:
        continue
    
new_bulk_jobs = new_bulk_jobs.groupby('Url', as_index=True).aggregate(lambda x: list(x)) # as_index defaults to 'True'; it's there just to make this explicit when reverencing this code in future!

print('\n'+f'Bulk rank requests for {date} complete!')
n = 1 # reset counter

# =============================================================================
#  Concatenate bulk_jobs and new_bulk_jobs
# =============================================================================

# This construction adds new rows if they don't exist and appends a column
print('\n'+'Concatenating bulk_jobs with bulk_jobs_new')
bulk_jobs = pd.concat([bulk_jobs, new_bulk_jobs], axis=1)
print('Merging complete!')

# =============================================================================
# Save Bulk Ranks (Daily) to Gsheets
# =============================================================================

print('\n'+f'Saving new bulk_jobs to Google Sheets')

## Clear gsheet ready for adding new data
sheet.values_clear( '{0}!A:AZ'.format(stat_bulk_jobs_gsheet) )

## reset index so that index col has a label when saved
bulk_jobs = bulk_jobs.reset_index()

## Save new dataframe
set_with_dataframe(sheet.worksheet(stat_bulk_jobs_gsheet), bulk_jobs, include_index=False)
print('Saved!')

# set index as 'Url' again for rest of script
bulk_jobs = bulk_jobs.set_index('Url')



# =============================================================================
#  Filter bulk_jobs for just 'report_date + 1' column
# =============================================================================

date = datetime.date.today() - datetime.timedelta(days=report_date+1)
date = date.strftime('%Y-%m-%d')

## Extract column name 'date' from bulk_jobs and drop na

try:
    jobs_df = bulk_jobs.loc[:, [f'{date}']]
except KeyError:
    date2 =  (datetime.date.today() - datetime.timedelta(days=report_date+1)).strftime('%d/%m/%Y')
    jobs_df = bulk_jobs.loc[:, [f'{date2}']]

jobs_df = jobs_df.dropna()

# =============================================================================
# Iterate through jobs_df 'date' column and get keywords for each job_id
# =============================================================================

print('\n'+f'Fetching bulk rank exports for {date}')

for i in jobs_df.index:
    site_url = i
    site_url = site_url.replace('/','_') # Removes slashes so file will save properly
    print(f'Retrieving data for {site_url}...')
    site_ids = jobs_df[f'{date}'][i]
    site_ids = site_ids.replace("'","") # Replace ' with blank so json.loads() works
    site_ids = json.loads(site_ids) # converts str of site_ids into list
    for job_id in site_ids:
        url = f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'
        response = requests.get(url)
        SRC += 1 # stat requests counter
        response = response.json()
        export = response.get('Response').get('Project').get('Site').get('Keyword')
    
    # dissect response and add to dataframe

    google_base_rank_df = pd.DataFrame()
    google_rank_df = pd.DataFrame()
    google_ranking_url_df = pd.DataFrame()

    k = 0 # for showing progress through total_kws
    total_kws = len(export)
    for item in export:
        if k % 1000 == 0 and k != 0:
            print(f'Completed {k} of {total_results}')
        k += 1
        try:
            gbr_df = pd.DataFrame({
                'keyword_id' : item.get('Id'),
                'keyword' : item.get('Keyword'),
                'device' : item.get('KeywordDevice'),
#                'location' : item.get('KeywordLocation'),
                'market' : item.get('KeywordMarket'),
#                'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
                'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
#                'keyword_tags' : item.get('KeywordTags'),
#                'date' : item.get('Ranking').get('date'),
#                'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
#                'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
                f'{date}' : item.get('Ranking').get('Google').get('BaseRank'),
#                'ranking_url' : item.get('Ranking').get('Google').get('Url')
            }, index=[0])
            google_base_rank_df = google_base_rank_df.append(gbr_df, ignore_index=True)
        except TypeError:
            pass
        try:
            gr_df = pd.DataFrame({
                'keyword_id' : item.get('Id'),
                'keyword' : item.get('Keyword'),
                'device' : item.get('KeywordDevice'),
#                'location' : item.get('KeywordLocation'),
                'market' : item.get('KeywordMarket'),
#                'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
                'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
#                'keyword_tags' : item.get('KeywordTags'),
#                'date' : item.get('Ranking').get('date'),
#                'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
                f'{date}' : int(item.get('Ranking').get('Google').get('Rank')),
#                'google_base_rank' : item.get('Ranking').get('Google').get('BaseRank'),
#                'ranking_url' : item.get('Ranking').get('Google').get('Url')
            }, index=[0])
            google_rank_df = google_rank_df.append(gr_df, ignore_index=True)     
        except TypeError:
            pass
        try:
            ru_df = pd.DataFrame({
                'keyword_id' : item.get('Id'),
                'keyword' : item.get('Keyword'),
                'device' : item.get('KeywordDevice'),
#                'location' : item.get('KeywordLocation'),
                'market' : item.get('KeywordMarket'),
#                'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
                'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
#                'keyword_tags' : item.get('KeywordTags'),
#                'date' : item.get('Ranking').get('date'),
#                'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
#                'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
#                'google_base_rank' : item.get('Ranking').get('Google').get('BaseRank'),
                f'{date}' : item.get('Ranking').get('Google').get('Url')
            }, index=[0])
            google_ranking_url_df = ranking_url_df.append(gru_df, ignore_index=True)        
        except TypeError:
            pass

    print(f'{site_id} done!')
    break # for testing script

# =============================================================================
# Process df and concat with historical_ranks - 
# started: 17/06/20
# completed: 18-06-2020
#
# issues: finding the right join/merge/concat formula was really hard - none 
# of them did what i wanted, and they removed the indexes, pulled up empty 
# data, added columns i didnt want, came up with exceptions, etc.
#
# solution: convert keyword_id to a string, and do a left merge
# (caller = base_ranks) on it to add new date ranks to existing col. 
# Then, remove existing keywords and append any that remain.
# =============================================================================

    print('\n'+f'Processing {site_url}...')
    
    # replace 'N/A' with 120
    print(f'Replacing not ranking with {not_ranking}')
    df[f'{date}'] = df[f'{date}'].replace(to_replace=[None], value=not_ranking) 
    
    df[f'{date}'] = df[f'{date}'].replace(to_replace=['N/A'], value=not_ranking) 
    # convert keyword_id to str so merge works properly...
    df['keyword_id'] = df['keyword_id'].astype(str) 
    df2 = df[['keyword_id', f'{date}']]
    
    # load historical base ranks for client
    print('Loading historical data...')
    try:
        base_ranks = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\{site_url}_base_rank.csv')
        print('Historical data found!')    # convert keyword_id to str so merge works properly...
        base_ranks['keyword_id'] = base_ranks['keyword_id'].astype(str) 

    # add 'date's ranks for existing keywords (uses left table to include only historical data)
        print('Updating existing keywords...')
        base_ranks = pd.merge(base_ranks, df2, how='left', on='keyword_id')  
   
    # drop keywords from df that are already in base_ranks
    # append what remains as new rows (includes 'keyword', 'device', 'market', regional_search_volume' that's not in the df2 merge)
        print('Adding new keywords...')
        cond = df['keyword_id'].isin(base_ranks['keyword_id'])
        cond = df.drop(df[cond].index)
        try:
            base_ranks = base_ranks.append(cond, sort=False)
        except:
            pass
        print('\n'+f'Saving {site_url} base ranks...')

    except:
        print('No historical data found! Creating new tracker...')
        base_ranks = df
        print('\n'+f'Saving {site_url} base ranks...')

# =============================================================================
# Save updated base ranks
# =============================================================================
    
    base_ranks.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\{site_url}_base_rank.csv', index=False)
    
# =============================================================================
# New Section
# =============================================================================


# =============================================================================
# END TIMER
# =============================================================================

end_time = datetime.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {SRC}')



print('\nWaiting for reports to run: sleeping for 3 mins...'
      '\n\nStarting Timer'
      '\n'+time.strftime('%M:%S', time.gmtime(0)))
seconds = 20
while seconds < 180:
    time.sleep(20)
    print(time.strftime('%M:%S', time.gmtime(seconds)))
    seconds += 20

print(time.strftime('%M:%S', time.gmtime(seconds)),
      '\nTimer Finished'
      '\nDownloading Reports')

# for each client, call server to get 'StreamUrl',   

stream_url= f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'

for i in client_list.index:
    client = client_list['Client'][i]
    job_id = client_list['JobId'][i]

    # send request to 'Status' url to get report url ('StreamUrl')
    response = requests.get(f'{base_url}/bulk/status?id={job_id}&format=json')
    response = response.json()
    export_url = response.get('Response').get('Result').get('StreamUrl')

    # send request to 'StreamUrl' to get data, and extract keyword data
    response = requests.get(export_url)
    response = response.json()
    export = response.get('Response').get('Project').get('Site').get('Keyword')
    total_kws = len(export))

    # dissect response and add to dataframe
    keyword_df = pd.DataFrame()
    n = 1
    for item in export:
        print(f'({n} of {total_kws} )',item.get('Keyword'))
        n += 1
        kw_dict = {
        'keyword' : item.get('Keyword'),
        'device' : item.get('KeywordDevice'),
        'location' : item.get('KeywordLocation'),
        'market' : item.get('KeywordMarket'),
        'global_search_volume' : int(item.get('KeywordStats').get('GlobalSearchVolume')),
        'regional_search_volume' : int(item.get('KeywordStats').get('TargetedSearchVolume')),
        'keyword_tags' : item.get('KeywordTags'),
        'date' : item.get('Ranking').get('date'),
        'rank_type' : item.get('Ranking').get('type'),
        'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
        'google_base_rank' : int(item.get('Ranking').get('Google').get('BaseRank')),
        'ranking_url' : item.get('Ranking').get('Google').get('Url')
        }     
        keyword_df = keyword_df.append( kw_dict, ignore_index = True )

    # save dataframe to excel file
    print(f'saving {client} ranks')
    keyword_df.to_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\stat_api_bulk_ranks.csv', index = False)   

print('\n'+'DURN!')
end_time = datetime.datetime.now().replace(microsecond=0)
total_time = end_time - start_time
print('\n'+f'Time taken: {total_time}')
