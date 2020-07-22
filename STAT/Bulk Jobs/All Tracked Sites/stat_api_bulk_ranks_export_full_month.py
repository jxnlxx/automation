# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 16:21:30 2020

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

# =============================================================================
# START
# =============================================================================

start_time = datetime.datetime.now().replace(microsecond=0)
SRC = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================

base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

site_id = ''
job_id = ''
date = ''


sites_all_url = f'{base_url}/sites/all?&results=5000&format=json'
keywords_list_url = f'{base_url}/keywords/list?site_id={site_id}&results=5000&format=json'
bulk_ranks_url = f'{base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
stream_url =  f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'

# SET MONTH AND YEAR (NUMERIC)
# =============================================================================
# 
# # =============================================================================
# # month = range(13)
# # print('\n'+'Welcome to the STAT API Bulk Ranks Export Script!'
# #       '\n'+'In order to use this script, reports need to have been reqested')
# #       '\n'+'using the STAT API Bulk Ranks Request script.')
# #       '\n'+'')
# #       '\n'+'')
# #       '\n'+'')
# # =============================================================================
# 
# ## if anyone wants to know, david is my muse
# davids_law1 = int(dt.datetime.today().strftime('%Y')) - 2
# current_year = int(dt.datetime.today().strftime('%Y'))
#   
# print('\n'+'Which year would you like to run reports for?')
# while True:
#     try:
#         year = int(input(f'Please enter a value between {davids_law1} & {current_year} >>> '))
#         if (year < davids_law1) or (year > current_year):
#             raise ValueError
#         else:
#             break
#     except ValueError:
#         print('Invalid response.')    
# 
# if year == davids_law1:
#     davids_law2 = int(dt.datetime.today().strftime('%m'))
# elif year == current_year:
#     davids_law3 = int(dt.datetime.today().strftime('%m'))
# else:
#     davids_law2 = 1
#     davids_law3 = 12
# 
# 
# 
# print('\n'+'Which month would you like to run reports for?')
# while True:
#     try:
#         month = int(input(f'Please enter a number from {davids_law2}-{davids_law3} >>> '))
#         if month < davids_law2 or month > davids_law3:
#             raise ValueError
#         else:
#             try:
#                 jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv')
#             except FileNotFoundError:
#                 print(f'No data has been requested for {month} in {year}, please try again...')    
#             break
#     except ValueError:
#         print('Invalid response.')
#         
# davids_law4 = 'comprised of is not an incorrect use of language'
# davids_opus = [davids_law1,davids_law2, davids_law3,davids_law4]
# month = month:02d
# =============================================================================

start_day = 1
year = 2018
month = 7
days = monthrange(year,month)[1]

not_ranking = 120

# =============================================================================
# SCRIPT
# =============================================================================

# =============================================================================
# Load bulk_ranks_all CSV for month
# =============================================================================

print('\n'+f'Loading {year}_{month:02d}_bulk_ranks_all.csv')
jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv')
jobs_all = jobs_all.set_index('Url') 

## filter dataframe to exclude jobs already done
try:
    jobs_head = jobs_all[~jobs_all['Status'].isin(['Done'])]  # the tilde makes this 'is not in 'done''
    jobs_head = jobs_head.head(10)
## if new month chop off head(10), then add ['Status'] column to jobs_all
except KeyError:
    jobs_head = jobs_all.head(10)
    jobs_all['Status'] = ''
print('Done!')
 
cols, rows = jobs_head.shape
s = 0
new_sites = []
for index, row in jobs_head.iterrows():
    site_url = index
    site_url = site_url.replace('/','_') # Remove slashes so file will save properly
    print('\n'+f'Checking for existing data for {site_url}...')
    
# =============================================================================
# Try loading the site's existing keyword ranks. If it doesn't exist,
# try loading a file containing just the keywords (see stat_api_keywords_list.py).
# If this doesn't exist, skip the client and add to 'new sites' list
# 'new_sites' is called at the end of the script as a reminder
# =============================================================================

    try: # try loading existing storage for client. If doesn not exist, create blank DataFrame
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Exports\{site_url}_base_ranks.csv')
        df['keyword_id'] = df['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
        print(f'{site_url} historical data found!')
    except:
        try:
            df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Keyword Lists\{site_url}_kw_list.csv')
            df['keyword_id'] = df['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
            print(f'No historical data found...'
                  '\n'+'Loading {site_url}_kw_list.csv instead...')
        except:
            print(f'{site_url} setup required; a reminder has been set!')
            new_sites.append(site_url)
            continue
    site_url = index # Reset site_url after changing slashes to load dataframes above
    print('\n'+f'Beginning {site_url}...')
    for i in range (rows):
        date = row.index[i]
        site_ids = row.values[i]
        site_ids = site_ids.replace("'","") # Replace ' with blank so json.loads() works
        site_ids = json.loads(site_ids) # converts str of site_ids into list
        kw_list = [] # For adding keywords into for URLs with multiple sites
        for job_id in site_ids:
            print(f'Requesting data for {date}...')
            response = requests.get(f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}')
            SRC += 1 # stat requests counter
            print('Data received!')
            response = response.json()
            new_kws = response.get('Response').get('Project').get('Site').get('Keyword')
            kw_list = kw_list + new_kws
        print('Processing keywords...')
        df2 = pd.DataFrame()
        k = 0 # for showing progress through site keywords
        total_results = len(kw_list)

# =============================================================================
# Iterate through kw_list to pull out data into a dataframe        
# =============================================================================

        for item in kw_list: 
            if k % 1000 == 0:
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
        
        print(f'Replacing not ranking with {not_ranking}')
        df2[f'{date}'] = df2[f'{date}'].replace(to_replace=[None], value=not_ranking) 
        df2[f'{date}'] = df2[f'{date}'].replace(to_replace=['N/A'], value='') 
        print('Done!')
            
        print('Updating existing keywords...')
        df2['keyword_id'] = df2['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
        
        # add 'date's ranks for existing keywords (uses left table to include only historical data)
        df3 = df2[['keyword_id', f'{date}']]
        df = pd.merge(df, df3, how='left', on='keyword_id')  
   
        print('Checking for new keywords...')
        # drop keywords from df2 that are already in df
        # append what remains as new rows (includes 'keyword', 'device', 'market', regional_search_volume' that's not in the df2 merge)
        cond = df2['keyword_id'].isin(df['keyword_id'])
        cond = df2.drop(df2[cond].index)
        try:
            df = df.append(cond, sort=False)
            print('New keywords added!')
        except:
            print('No new keywords found!')
            continue
    df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Exports\{site_url}_base_ranks.csv', index=False)
    print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv...')
    jobs_all.at[f'{site_url}','Status'] = 'Done' # add 'Done to 'status' col for URL
    jobs_all = jobs_all.reset_index()  #reset index so it has a label when saved 
    site_url = site_url.replace('/','_') # Remove slashes so file will save properly
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv', index=False)        
    print('\n'+f'{site_url} complete!')        

# =============================================================================
# 
#     
#     df = pd.merge(df, df3, how='left', on='keyword_id')  else:
#             print('\n'+f'Exporting {site_url}_base_ranks.csv...')        
#             print('\n'+f'Finished {date}')
#         print(f'Replacing not ranking with {not_ranking}...')
#         df2[f'{date}'] = df2[f'{date}'].replace(to_replace=[None], value=not_ranking) 
#         df2[f'{date}'] = df2[f'{date}'].replace(to_replace=['N/A'], value='') # 'N/A' is used when 'date' is before keyword tracking began
#         print('Done!')
#         print('Updating existing keywords...')
#         df2['keyword_id'] = df['keyword_id'].astype(str) #changes keyword_id to str so merge works later
#         df3 = df2[['keyword_id', f'{date}']]
#         print('\n'+f'Exporting {site_url}_base_ranks.csv...')                print('\n'+f'Requesting data for {date}...')
#                 df2 = pd.DataFrame()
#                 for job_id in site_ids:
#                     url = f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'
#                     response = requests.get(url)
#                     SRC += 1 # stat requests counter
#                     print('Data Received!')
#                     response = response.json()
#                     export = response.get('Response').get('Project').get('Site').get('Keyword')
#                     n = 1
#                     total_kws = len(export)
#                     # dissect response and add to dataframe
#                     print('Processing...')
#                     time.sleep(1)
#                     for item in export:
#                         print(f'({n} of {total_kws})',item.get('Keyword'))
#                         n += 1
#                         temp = pd.DataFrame({
#                             'keyword_id' : item.get('Id'),
#                             'keyword' : item.get('Keyword'),
#                             'device' : item.get('KeywordDevice'),
# #                            'location' : item.get('KeywordLocation'),
#                             'market' : item.get('KeywordMarket'),
# #                            'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
#                             'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
# #                            'keyword_tags' : item.get('KeywordTags'),
# #                            'date' : item.get('Ranking').get('date'),
# #                            'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
# #                            'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
#                             f'{date}' : item.get('Ranking').get('Google').get('BaseRank'),
# #                            'ranking_url' : item.get('Ranking').get('Google').get('Url')
#                             }, index=[0])
#                         df2 = df2.append(temp, ignore_index=True)
#                     print('\n'+f'Finished {date}')
#             
#        
#             # drop keywords from df2 that are already in df
#             # append what remains as new rows (includes 'keyword', 'device', 'market', regional_search_volume' that's not in the df2 merge)
#             print('Checking for new keywords...')
#             cond = df2['keyword_id'].isin(df['keyword_id'])
#             cond = df2.drop(df2[cond].index)
#             try:
#                 df = df.append(cond, sort=False)
#                 print('New keywords added!')
#             except:
#                 print('No new keywords found!')
#                 continue
#         else:
#             print('\n'+f'Requesting data for {date}...')
#             kw_list = []
#             for job_id in site_ids:
#                 url = f'https://iprospectman.getstat.com/bulk_reports/stream_report/{job_id}?key={stat_key}'
#                 response = requests.get(url)
#                 SRC += 1 # stat requests counter
#                 print('Data Received!')
#                 response = response.json()
#                 export = response.get('Response').get('Project').get('Site').get('Keyword')
#                 n = 1
#                 total_kws = len(export)
#                 # dissect response and add to dataframe
#                 print('Processing...')
#                 time.sleep(1)
#             total_kws = len(kw_list)
#             df2 = pd.DataFrame()
#             for item in export:
#                 print(f'({n} of {total_kws})',item.get('Keyword'))
#                 n += 1
#                 temp = pd.DataFrame({
#                     'keyword_id' : item.get('Id'),
#                     'keyword' : item.get('Keyword'),
#                     'device' : item.get('KeywordDevice'),
# #                        'location' : item.get('KeywordLocation'),
#                     'market' : item.get('KeywordMarket'),
# #                        'global_search_volume' : item.get('KeywordStats').get('GlobalSearchVolume'),
#                     'regional_search_volume' : item.get('KeywordStats').get('TargetedSearchVolume'),
# #                        'keyword_tags' : item.get('KeywordTags'),
# #                        'date' : item.get('Ranking').get('date'),
# #                        'rank_type' : item.get('Ranking').get('type'), #default is 'highest'
# #                        'google_rank' : int(item.get('Ranking').get('Google').get('Rank')),
#                     f'{date}' : item.get('Ranking').get('Google').get('BaseRank'),
# #                        'ranking_url' : item.get('Ranking').get('Google').get('Url')
#                     }, index=[0])
#                 df2 = df2.append(temp, ignore_index=True)
#                 print('\n'+f'Finished {date}')
#             print(f'Replacing not ranking with {not_ranking}')
#             df2[f'{date}'] = df2[f'{date}'].replace(to_replace=[None], value=not_ranking) 
#             df2[f'{date}'] = df2[f'{date}'].replace(to_replace=['N/A'], value='') 
#             print('Done!')
#                 
#             print('Updating existing keywords...')
#             df2['keyword_id'] = df['keyword_id'].astype(str) # convert keyword_id to str so merge works properly...
#             
#             # add 'date's ranks for existing keywords (uses left table to include only historical data)
#             df3 = df2[['keyword_id', f'{date}']]
#             df = pd.merge(df, df3, how='left', on='keyword_id')  
#        
#             print('Checking for new keywords...')
#             # drop keywords from df2 that are already in df
#             # append what remains as new rows (includes 'keyword', 'device', 'market', regional_search_volume' that's not in the df2 merge)
#             cond = df2['keyword_id'].isin(df['keyword_id'])
#             cond = df2.drop(df2[cond].index)
#             try:
#                 df = df.append(cond, sort=False)
#                 print('New keywords added!')
#             except:
#                 print('No new keywords found!')
#                 continue
#     print('\n'+f'Exporting {site_url}_base_ranks.csv...')
#     df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Exports\{site_url}_base_ranks.csv', index=False)
#     print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv...')
#     jobs_all.at[f'{site_url}','Status'] = 'Done' # add 'Done to 'status' col for URL
#     jobs_all = jobs_all.reset_index()  #reset index so it has a label when saved 
#     site_url = site_url.replace('/','_') # Remove slashes so file will save properly
#     jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Setup\Client Ranks\Historical\Jobs\{year}_{month:02d}_bulk_ranks_all.csv', index=False)        
#     jobs_all = jobs_all.set_index('Url') 
#     print('\n'+f'{site_url} complete!')
# 
# =============================================================================
# =============================================================================
# END TIMER
# =============================================================================

end_time = datetime.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!')
print('\n'+f'{len(new_sites)} new sites found!')
for site in new_sites:
    print(site)
print('Please run stat_api_keywords_list.py to update')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {SRC}')