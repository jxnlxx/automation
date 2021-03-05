# stat_ivy_export_all.py
#

#%%

import os
import json
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
import xlsxwriter as xl
from calendar import monthrange
from getstat import stat_subdomain, stat_key, stat_base_url

# dates

date = dt.date.today().replace(day = 1) - dt.timedelta(days = 1)
year = int(date.strftime('%Y'))
month = int(date.strftime('%m'))
days = int(date.strftime('%d'))
start_date = f'{year}-{month:02d}-01'
cutoff = f'{year}-{month:02d}-{days:02d}'

# # Custom Date range - just enter last date in month as 'date'
# date = '2020-06-30'
# date = dt.datetime.strptime(date, '%Y-%m-%d')
# year = int(date.strftime('%Y'))
# month = int(date.strftime('%m'))
# days = int(date.strftime('%d'))
# start_date = f'{year}-{month:02d}-01'
# cutoff = f'{year}-{month:02d}-{days:02d}'


# file locations

ivy_path = fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\The Ivy Restaurants'
teams_path = fr'C:\Users\JLee35\dentsu\IVY Restaurants - Documents\General\STAT Rank Reports'

#%% check final job is 'completed

jobs_all = pd.read_csv(os.path.join(ivy_path, fr'Requests\{year}_{month:02d}_ivy_requests.csv'))

final_job = jobs_all.iloc[-1,-1]
final_job = final_job.replace("'","") # remove ' so json.loads() works
final_job = json.loads(final_job) # converts str of site_ids into list
response = requests.get(f'{stat_base_url}/bulk/status?id={final_job[-1]}&format=json')
response = response.json()
status = response.get('Response').get('Result').get('Status')
if status != 'Completed':
    time = dt.datetime.now().strftime('%H:%M')
    print(f'{time} - Final job status: {status}')
    print('Current time:', dt.datetime.now().strftime('%H:%M'))
else:
    print(f'{time} - Final job status: {status}')
    print('Continue with next cell')

#%% retrieve ranks and export data

print(f'Loading {year}_{month:02d}_ivy_requests.csv')
jobs_all = pd.read_csv(os.path.join(ivy_path, fr'Requests\{year}_{month:02d}_ivy_requests.csv'))
jobs_all = jobs_all.set_index('Title')

# filter dataframe to exclude jobs already done
try:
    jobs_head = jobs_all[~jobs_all['Status'].isin(['Done'])]  # the tilde makes this 'is not in 'done''
    jobs_head = jobs_head.drop(columns=['Status'])
# if 'Status' column doesn't exist,
except KeyError:
    jobs_head = jobs_all.copy()
    jobs_all['Status'] = ''

#jobs_head = jobs_head.head(2) #for testing 2 sites for full month

print('Done!')
s = 1
total_sites = len(jobs_head)
new_sites = []

# try making directory if it doesnt exist
try:
    os.mkdir(os.path.join(ivy_path, fr'Exports\{year}_{month:02d}_ivy_restaurants'))
except FileExistsError:
    pass

try:
    os.mkdir(os.path.join(ivy_path, fr'Exports\{year}_{month:02d}_ivy_restaurants\Data'))
except FileExistsError:
    pass

for index, row in jobs_head.iterrows(): # this construction allows us to go across rows rather than down cols (like with 'for i in jobs_head.index')
    ranks_month_df = pd.DataFrame()
    site_name = index
    save_name = index

    # Continue with iteration through bulk_ranks df to get the keyword data

    print('\n'+f'Beginning {site_name}...'
          '\n'+f'{s} of {total_sites}')

    for i in range(len(row)):
        date = row.index[i]
        site_ids = row.values[i]
        if pd.isnull(site_ids): # checks if site_ids is empty - if it is, start loop again (i.e., try next cell)
            continue
        else:
            site_ids = site_ids.replace("'","") # remove ' so json.loads() works
            site_ids = json.loads(site_ids) # converts str of site_ids into list
            kw_list = [] # List for adding keywords when site has multiple IDs
            j = 1
            print(f'Requesting {site_name} data for {date}...'
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

    print(f'All data for {site_name} received!')

    #break

    # remove all cols but 'Keyword','KeywordDevice','Ranking.Google.BaseRank'
    site_ranks = ranks_month_df[['Keyword','KeywordDevice','Ranking.Google.BaseRank']]

    # rename cols
    site_ranks.columns = site_ranks.columns.str.replace('Ranking.Google.','')
    site_ranks.columns = site_ranks.columns.str.replace('KeywordDevice','Device')

    # drop cols with 'N/A' (these arent being tracked) & reset index
    site_ranks = site_ranks[site_ranks['BaseRank'] != 'N/A'].reset_index(drop=True)

    # replace None and 'N/A' with np.nan
    site_ranks[['BaseRank']] = site_ranks[['BaseRank']].replace(to_replace=[None], value=np.nan)
    site_ranks[['BaseRank']] = site_ranks[['BaseRank']].astype(float)

    # average out base rank by keyword and device
    site_ranks = site_ranks.groupby(['Keyword','Device'], as_index=False).mean().round(0)

    #replace blanks with 120
    site_ranks[['BaseRank']] = site_ranks[['BaseRank']].replace(to_replace=[np.nan], value=120)

    # insert new column with restaurant name
    site_ranks.insert(0,'Restaurant',site_name)

    # save file
    site_ranks.to_csv(ivy_path + fr'\Exports\{year}_{month:02d}_ivy_restaurants\Data\{year}_{month:02d}_{save_name}.csv', index=False)

    print('\n'+f'Updating {year}_{month:02d}_ivy_requests.csv...')

    # Update jobs_all and save
    jobs_all.at[f'{site_name}','Status'] = 'Done'
    jobs_all = jobs_all.reset_index()  # reset index so it has a label when saved (problems using file in future otherwise...)
    jobs_all.to_csv(ivy_path + fr'\Requests\{year}_{month:02d}_ivy_requests.csv', index=False)
    jobs_all = jobs_all.set_index('Title') # set index so next iteration works
    s += 1
    print('\n'+f'Finished {site_name}!')

    #break

# create single file with all site ranks

ivy_all = pd.DataFrame(columns=['Restaurant','Keyword','Device','BaseRank'])

for filename in os.listdir(os.path.join(ivy_path, fr'Exports\{year}_{month:02d}_ivy_restaurants\Data')):
    if filename.endswith('.csv'):
        print(f'Getting {filename}')
        df = pd.read_csv(os.path.join(ivy_path, fr'Exports\{year}_{month:02d}_ivy_restaurants\Data\{filename}'))
        ivy_all = ivy_all.append(df)
        print(f'{filename} appended!')
    else:
        continue

print('Saving...')
ivy_all.to_csv(os.path.join(teams_path, fr'{year}_{month:02d}_ivy_all.csv'), index=False)
ivy_all.to_csv(os.path.join(ivy_path, fr'Exports\{year}_{month:02d}_ivy_restaurants\{year}_{month:02d}_ivy_all.csv'), index=False)


print('\nDURN!')

#%%

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                           #
#        the stuff below is not required for the job,       #
#        but can be used in the case there's an error       #
#                   in its execution                        #
#                                                           #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


##%

jobs_all = jobs_all.drop(columns=['Status'])
jobs_all = jobs_all.reset_index()
jobs_all.to_csv(os.path.join(ivy_path, fr'Requests\{year}_{month:02d}_ivy_requests.csv'), index=False)

jobs_all = jobs_all.drop(columns=['Unnamed: 0'])
#%%

not_ranking = 120
ranks_month_df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Exports\{year}_{month:02d}_ivy_restaurants\{year}_{month:02d}_{save_name}.csv')
ranks_month_df[['Rank','BaseRank']] = ranks_month_df[['Rank','BaseRank']].replace(to_replace=[None], value=not_ranking)
new_df = ranks_month_df.groupby('Keyword', 'Site', as_index=False).mean()
new_df = ranks_month_df[['Keyword','Site','BaseRank']]

grouped = new_df.groupby(['Keyword']).mean()
ranks_month_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Exports\{year}_{month:02d}_ivy_restaurants\{year}_{month:02d}_{save_name}2.csv', index=False)


#%%

ranks_day_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\The Ivy Restaurants\Exports\tester.csv', index=False)
jobs_all.to_csv

#%%

jobs_all = jobs_all.drop(columns=['index'])
# %%

# %%
