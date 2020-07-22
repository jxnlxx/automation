# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 10:21:48 2020

@author: JLee35
"""

import requests
import pandas as pd
import time
import datetime

#####


app_subdomain = 'iprospectman'
api_key = '6a3cbf9fc980e2695714c5022312bd9ba509d6aa'
base_url = f'https://{app_subdomain}.getstat.com/api/v2/{api_key}'

# =============================================================================
# SETTINGS
# =============================================================================

year = '2020'
month = '02'
days_in_month = 29

client = 'Holland and Barrett UK'
site_id = '8296'

minute = 60
sleep_timer = minute*20

not_ranking = 120

client_base = '{client} Daily Ranks {month}'

# =============================================================================
# SCRIPT
# =============================================================================

# set up excel file to put data in
jobs_df = pd.DataFrame(columns=['Date','JobId'])

x = 1
while x <= days_in_month:
    if x < 10:
        date = f'{year}-{month}-0{x}'
    else:
        date = f'{year}-{month}-{x}'

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


# wait for reports to generate
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
#keyword_df.to_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Daily Ranks\{month} - {client} Daily Ranks.xlsx', index=False )   



# =============================================================================
# # =============================================================================
# # for starting script
# keyword_df = pd.read_excel( fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Daily Ranks\{month} - {client} Daily Ranks.xlsx' )
# jobs_df = pd.read_excel('jobs_df.xlsx')
# 
# n = 3
# # =============================================================================
# =============================================================================


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

print(f'Exporting file to L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Daily Ranks\{month} - {client} Daily Ranks.xlsx')
keyword_df.to_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Daily Ranks\{month} - {client} Daily Ranks.xlsx', index=False )    

print('DURN!')

