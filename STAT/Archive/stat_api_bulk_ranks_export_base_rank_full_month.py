# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 16:21:30 2020

@author: JLee35
"""

import os
import time
import json
import datetime as dt
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


print('\n'+'Welcome to the STAT API Bulk Ranks Request script!'
      '\n'+'This script requests daily ranks for all clients in'
      '\n'+'a single, specified month.'
      '\n'+''
      '\n'+'Please answer the following questions:')

# =============================================================================
# SET MONTH AND YEAR (NUMERIC)
# =============================================================================

davids_law1 = int(dt.datetime.today().strftime('%Y')) - 2
current_year = int(dt.datetime.today().strftime('%Y'))
  
print('\n'+'Which year would you like to retrieve exports for?')
while True:
    try:
        year = int(input(f'Please enter a value between {davids_law1} & {current_year} >>> '))
        if (year < davids_law1) or (year > current_year):
            raise ValueError
        else:
            break
    except ValueError:
        print('Invalid response.')    

if year == davids_law1:
    davids_law2 = int(dt.datetime.today().strftime('%m'))
    davids_law3 = 12
elif year == current_year:
    davids_law2 = 1
    davids_law3 = int(dt.datetime.today().strftime('%m'))
else:
    davids_law2 = 1
    davids_law3 = 12

print('\n'+'Which month would you like to retrieve exports for?')
while True:
    try:
        month = int(input(f'Please enter a value between {davids_law2}-{davids_law3} >>> '))
        if month < davids_law2 or month > davids_law3:
            raise ValueError
        try:
            jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv')
            print('\n'+f'Loading {year}_{month:02d}_bulk_ranks_all.csv...')
            time.sleep(2)
            break
        except FileNotFoundError:
            print('\n'+'Reports have not been requested for this month yet.'
                  '\n'+'Please run stat_api_bulk_ranks_request_full_month.py'
                  '\n'+'for the month, or select a different month.')
            continue

    except ValueError:
        print('Invalid response.')
    except FileNotFoundError:
        print('Reports have not been requested for this month yet'
              '\n'+'Please run stat_api_bulk_ranks_request_full_month.py,'
              '\n'+'or select a different month.')

days = monthrange(year,month)[1]
cutoff = f'{year}-{month:02d}-{days:02d}'

# =============================================================================
# START
# =============================================================================

start_time = dt.datetime.now().replace(microsecond=0)
SRC = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

not_ranking = 120#

# site_id = 

# =============================================================================
# SCRIPT
# =============================================================================
date
# =============================================================================
# Load bulk_ranks_all CSV for month
# =============================================================================

jobs_all = jobs_all.set_index('Url') 

## filter dataframe to exclude jobs already done
try:
    jobs_head = jobs_all[~jobs_all['base_rank_status'].isin(['Done'])]  # the tilde makes this 'is not in 'done''
    jobs_head = jobs_head.drop(columns=['base_rank_status','rank_status','ranking_url_status'])
#    jobs_head = jobs_head.head(10)

## if new month chop off head(10), then add status columns to jobs_all
except KeyError:
    jobs_head = jobs_all
    jobs_all = jobs_all.assign(base_rank_status='',rank_status='',ranking_url_status='').astype(str)

print('Done!')
 
s = 0
#rows, cols = jobs_head.shape # for iterating through rows later as cannot 
new_sites = []

for index, row in jobs_head.iterrows():
    site_url = index
    print('\n'+f'Checking for existing data for {site_url}...')
    site_url = site_url.replace('/','_') # Remove slashes so file will load properly

# =============================================================================
# Try loading the site's existing keyword ranks. If it doesn't exist,
# try loading a file containing just the keywords (see stat_api_keywords_list.py).
# If this doesn't exist, skip the client and add to 'new sites' list
# 'new_sites' is called at the end of the script as a reminder
# =============================================================================
    
    try: # try loading existing storage for client. 
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Exports\Google Base Rank\{site_url}_google_base_rank.csv')
        df['keyword_id'] = df['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
        print(f'{site_url} historical data found!')
    except FileNotFoundError:
        try:
            # If it doesn't exist, try loading the client's kw_list
            df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Keyword Lists\{site_url}_kw_list.csv')
            df['keyword_id'] = df['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
            print(f'No historical data found...'
                  '\n'+f'Loading {site_url}_kw_list.csv instead...')
        except FileNotFoundError:
            new_sites.append(site_url)
            print(f'{site_url} setup required; a reminder has been set!')
            continue

    site_url = index # Reset site_url after changing slashes to load dataframes above

    print('\n'+f'Beginning {site_url}...')

    for i in range(len(row)):
        date = row.index[i]
        site_ids = row.values[i]
        if pd.isnull(site_ids): # checks if site_ids is empty - if it is, try next cell
            continue
        else:
            site_ids = site_ids.replace("'","") # Replace ' with blank so json.loads() works
            site_ids = json.loads(site_ids) # converts str of site_ids into list
            kw_list = [] # For adding keywords into for URLs with multiple sites
            for job_id in site_ids:
                print(f'Requesting {site_url} data for {date}...')
                response = requests.get(f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}')
                SRC += 1 # stat requests counter
                print('Data received!')
                response = response.json()
                new_kws = response.get('Response').get('Project').get('Site').get('Keyword')
                kw_list = kw_list + new_kws
        total_results = len(kw_list)
        print(f'Processing {total_results} keywords...')
        df2 = pd.DataFrame()
        k = 0 # for showing progress through site keywords

# =============================================================================
# Iterate through kw_list to append the day's data onto 'df'        
# =============================================================================

        for item in kw_list: 
            if k % 1000 == 0 and k != 0:
                print(f'Completed {k} of {total_results}')
            k += 1            
            try:
                temp = pd.DataFrame({
                    'keyword_id' : item.get('Id'),
                    'keyword' : item.get('Keyword'),
                    'device' : item.get('KeywordDevice'),
#                            'location' : item.get('KeywordLocation'),
                    'market' : item.get('KeywordMarket'),
#                                 'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
                    'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
#                            'keyword_tags' : item.get('KeywordTags'),
#                            'date' : item.get('Ranking').get('date'),
#                            'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
#                            'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
                    f'{date}' : item.get('Ranking').get('Google').get('BaseRank'),
#                            'ranking_url' : item.get('Ranking').get('Google').get('Url')
                    }, index=[0])
                df2 = df2.append(temp, ignore_index=True)
            except:
                continue
        
        print(f'Completed {k} of {total_results}')
        print(f'Processing complete!')
        time.sleep(1)
        print(f'Replacing not ranking with {not_ranking}...')
        df2[f'{date}'] = df2[f'{date}'].replace(to_replace=[None], value=not_ranking) 
        df2[f'{date}'] = df2[f'{date}'].replace(to_replace=['N/A'], value='') 
        print('Done!')
            
        print('Updating existing keywords...')
        df2['keyword_id'] = df2['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
        
        # add 'date's ranks for existing keywords (uses left table to include only historical data)
        df3 = df2[['keyword_id', f'{date}']]
        df = pd.merge(df, df3, how='left', on='keyword_id')  
        print('Done!')
        print('Checking for new keywords...')
        # drop keywords from df2 that are already in df
        # append what remains as new rows (includes 'keyword', 'device', 'market', regional_search_volume' that's not in the df2 merge)
        cond = df2['keyword_id'].isin(df['keyword_id'])
        cond = df2.drop(df2[cond].index)
        try:
            df = df.append(cond, sort=False)
            print('Done!')
        except:
            print('No new keywords found!')
            pass

# =============================================================================
# Once all days of the month have been done for the site, save to CSV
# =============================================================================

    print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv...')
    jobs_all.at[f'{site_url}','base_rank_status'] = 'Done' # add 'Done to 'Status' col for URL
    jobs_all = jobs_all.reset_index()  #reset index so it has a label when saved 
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv', index=False)        
    jobs_all = jobs_all.set_index('Url')
    site_url = site_url.replace('/','_') # Remove slashes so file will save properly
    df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Exports\Google Base Rank\{site_url}_google_base_rank.csv', index=False)
    print('\n'+f'{site_url} complete!')        

#    break # end after 1 site for testing

# =============================================================================
# END TIMER
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!')
print('\n'+f'{len(new_sites)} new sites found!')
if len(new_sites) != 0:
    for site in new_sites:
        print(site)
    print('Please run stat_api_keywords_list.py to update')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {SRC}')