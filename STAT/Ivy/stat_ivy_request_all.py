# stat_ivy_request_all.py
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
# TODO adjust dates etc...

date = dt.date.today().replace(day = 1) - dt.timedelta(days = 1)
year = int(date.strftime('%Y'))
month = int(date.strftime('%m'))
days = int(date.strftime('%d'))
start_date = f'{year}-{month:02d}-01'
cutoff = f'{year}-{month:02d}-{days:02d}'

# # Custom Date range - just enter last date in month as 'date'
# date = '2020-11-30'
# date = dt.datetime.strptime(date, '%Y-%m-%d')
# year = int(date.strftime('%Y'))
# month = int(date.strftime('%m'))
# days = int(date.strftime('%d'))
# start_date = f'{year}-{month:02d}-01'
# cutoff = f'{year}-{month:02d}-{days:02d}'

# file locations

ivy_path = fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\The Ivy Restaurants'

#%% get ivy sites list

url = f'{stat_base_url}/sites/list?project_id=475&format=json'
response = requests.get(url)
response = response.json()
response = response.get('Response').get('Result')

ivy_sites = pd.DataFrame(response)
ivy_sites = ivy_sites[ivy_sites['FolderId'].isin(['2751'])] # drop sites that arent in the Ivy Restaurants folder
ivy_sites = ivy_sites[~ivy_sites['TotalKeywords'].isin(['0'])] # drop sites that have 0 keywords
ivy_sites = ivy_sites.reset_index(drop=True) # reset index

#ivy_sites.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\stat_ivy_sites_list.csv', index = False)
all_jobs = pd.DataFrame(ivy_sites['Title'])

# request reports for all days of the month for each site in ivy_sites
for date in pd.date_range(start=start_date, end=cutoff):
    date = date.strftime('%Y-%m-%d')
    print('\n'+f'Requesting bulk rank exports for {date}')

    # for each site_id in site_list, call API and request reports for date
    n = 1 # counter for 'total_sites'
    jobs = pd.DataFrame(columns=['Title', f'{date}'])
    for i in ivy_sites.index:
        if ivy_sites['CreatedAt'][i] <= date:
            # pull Id from client_list and create url
            site_name = ivy_sites['Title'][i]
            site_id = ivy_sites['Id'][i]
            try:
                print(f'{n:03d} Requesting {date} rank report for {site_name}')
                url = f'{stat_base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'

                # request bulk ranks from url, adding job_id to list
                response = requests.get(url)
                response = response.json()
                job_id = response.get('Response').get('Result').get('Id')
                temp = {'Title': site_name, f'{date}' : job_id}
                jobs = jobs.append(temp, ignore_index=True)
                n += 1
                time.sleep(0.5)
            except:
                continue
        else:
            continue
    jobs = jobs.groupby('Title', as_index=True).aggregate(lambda x : list(x)) # as_index defaults to 'True', it's there just for future reference!
    all_jobs = all_jobs.merge(jobs,how='left', on='Title')
    print('\n'+f'Bulk rank requests for {date} complete!')
    n = 1 # reset counter for 'total_sites'
    print('Done!')
    time.sleep(1)

# save job_ids to csv

all_jobs.to_csv(ivy_path + fr'\Requests\{year}_{month:02d}_ivy_requests.csv', index = False)

ivy_title = ivy_sites[['Title','Url']]
all_jobs_plus = all_jobs.merge(ivy_title, on='Title', how='left')

all_jobs_plus = all_jobs_plus.set_index('Title')
all_jobs_plus = all_jobs_plus.reset_index()
all_jobs_plus = all_jobs_plus.drop(columns=['Url'])
all_jobs_plus.to_csv(ivy_path + fr'\Requests\{year}_{month:02d}_ivy_requests.csv', index = False)


print('DURN!')
print('Time Finished:',dt.datetime.now().strftime('%H:%M'))
# %%
