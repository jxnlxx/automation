# stat_ivy_export_all.py
#

#%%

import os
import json
import time
import requests
import pandas as pd
import datetime as dt
import xlsxwriter as xl
from calendar import monthrange
from getstat import stat_subdomain, stat_key, stat_base_url

# dates

date = dt.datetime.now()
month = int(date.strftime('%m'))-1
year = int(date.strftime('%Y'))
days = monthrange(year,month)[1]
start_date = f'{year}-{month:02d}-01'
cutoff = f'{year}-{month:02d}-{days:02d}'

# not ranking

not_ranking = 120

# script

jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Requests\{year}_{month:02d}_ivy_requests.csv')

jobs_all = jobs_all.set_index('Url')

# filter dataframe to exclude jobs already done
try:
    jobs_head = jobs_all[~jobs_all['Status'].isin(['Done'])]  # the tilde makes this 'is not in 'done''
    jobs_head = jobs_head.drop(columns=['Status'])
# if 'Status' column doesn't exist,
except KeyError:
    jobs_head = jobs_all
    jobs_all = jobs_all.assign(Status='').astype(str)

#jobs_head = jobs_head.head(2) #for testing 2 sites for full month

print('Done!')
s = 1
total_sites = len(jobs_head)
new_sites = []

# try making directory if it doesnt exist
try:
    os.mkdir(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Exports\{year}_{month:02d}_ivy_restaurants')
except FileExistsError:
    pass

for index, row in jobs_head.iterrows(): # this construction allows us to go across rows rather than down cols (like with 'for i in jobs_head.index')
    ranks_month_df = pd.DataFrame()
    site_url = index
    save_name = site_url.replace('/','_') # Remove slashes so file will save/load properly
    save_name = save_name.replace('-','_') # Remove slashes so file will save/load properly
    save_name = save_name.replace('.','_') # Remove slashes so file will save/load properly

# Continue with iteration through bulk_ranks df to get the keyword data

    print('\n'+f'Beginning {site_url}...'
          '\n'+f'{s} of {total_sites}')

    for i in range(len(row)):
        date = row.index[i]
        site_ids = [row.values[i]] #TODO - remove []
        if pd.isnull(site_ids): # checks if site_ids is empty - if it is, start loop again (i.e., try next cell)
            continue
        else:
            #site_ids = site_ids.replace("'","") # Replace ' with blank so json.loads() works
            #site_ids = json.loads(site_ids) # converts str of site_ids into list
            kw_list = [] # For adding keywords into for URLs with multiple sites
            j = 1
            print(f'Requesting {site_url} data for {date}...'
                  '\n'+f'(Site {s} of {total_sites})')
            for job_id in site_ids:
                stream_url = f'/bulk_reports/stream_report/{job_id}'
                print(j, stream_url)
                response = requests.get('https://iprospectman.getstat.com'+stream_url+f'?key={stat_key}')
                #request_counter += 1 # stat requests counter
                response = response.json()
                new_kws = response.get('Response').get('Project').get('Site').get('Keyword')
                kw_list = kw_list + new_kws
                j += 1
                time.sleep(1)
            print('Data received!')

# Create DataFrame from kw_list using pd.json_normalize

        total_kws = len(kw_list)
        print(f'Processing {total_kws} keywords...')

        ranks_day_df = pd.json_normalize(kw_list)
        ranks_month_df = ranks_month_df.append(ranks_day_df)

# Update jobs_all['Status'] with 'Done' & save file to create a save state

    print(f'Saving {year}_{month:02d}_{save_name}.csv')
    ranks_month_df.columns = ranks_month_df.columns.str.split('.').str[-1]
    ranks_month_df = ranks_month_df.rename(columns={'Id':'KeywordId', 'date':'Date', 'type':'Type', ;'TargetedSearchVolume':'RegionalSearchVolume'})
    ranks_month_df = ranks_month_df.assign(Site=f'{site_url}').astype(str)
    ranks_month_df = ranks_month_df[['KeywordId','Keyword','Site','KeywordMarket','KeywordLocation','KeywordDevice','KeywordTags','CreatedAt','TargetedSearchVolume','Rank','BaseRank','Url']]
    ranks_month_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Exports\{year}_{month:02d}_ivy_restaurants\{year}_{month:02d}_{save_name}.csv', index=False)

    print('\n'+f'Updating {year}_{month:02d}_ivy_requests.csv...')

    # Update jobs_all and save
    jobs_all.at[f'{site_url}','Status'] = 'Done'
    jobs_all = jobs_all.reset_index()  # reset index so it has a label when saved (problems using file in future otherwise...)
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Requests\{year}_{month:02d}_ivy_requests.csv', index=False)
    jobs_all = jobs_all.set_index('Url') # set index so next iteration works
    s += 1
    print('\n'+f'Done!')
    break

print ('DURN!')
# %%


jobs_all = jobs_all.drop(columns=['Status'])
jobs_all = jobs_all.drop(columns=['Unnamed: 0'])
