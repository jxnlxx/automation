# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 09:08:57 2020

@author: JLee35
"""

import sys
import time
import json
import sqlite3
import requests
import pandas as pd
import datetime as dt
from calendar import monthrange
from pandas.io.json import json_normalize
from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib



print('\n'+'Welcome to the STAT API Bulk Ranks Request script!'
      '\n'+'This script requests daily ranks for all clients in'
      '\n'+'a single, specified month.'
      '\n'+''
      '\n'+'Please answer the following questions:')

# =============================================================================
# SET MONTH AND YEAR (NUMERIC)
# =============================================================================

davids_law1 = int(dt.datetime.today().strftime('%Y')) - 2
davids_law2 = int(dt.datetime.today().strftime('%Y'))

print('\n'+'Which year would you like to retrieve exports for?')
while True:
    try:
        year = int(input(f'Please enter a value between {davids_law1} & {davids_law2} >>> '))
        if (year < davids_law1) or (year > davids_law2):
            raise ValueError
        else:
            break
    except ValueError:
        print('Invalid response.')

if year == davids_law1:
    davids_law3 = int(dt.datetime.today().strftime('%m')) -1
    davids_law4 = 12
elif year == davids_law2:
    davids_law3 = 1
    davids_law4 = int(dt.datetime.today().strftime('%m'))
else:
    davids_law3 = 1
    davids_law4 = 12

print('\n'+'Which month would you like to retrieve exports for?')
while True:
    month = input(f'Please enter a value between {davids_law3}-{davids_law4}'
                      '\n'+'or type \'end\' to terminate. >>> ')
    if month == 'end':
        print('Terminating script...')
        sys.exit(1)
    else:
        try:
            month = int(month)
            if month < davids_law3 or month > davids_law4:
                raise ValueError

            try:
                jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\Requests\{year}_{month:02d}_bulk_ranks_all.csv')
                print('\n'+f'Checking {year}_{month:02d}_bulk_ranks_all.csv...')
                time.sleep(1)
                try: # remove any rows with 'Done' in 'Status' column
                    temp = jobs_all[~jobs_all['Status'].isin(['Done'])]  # the tilde makes this 'is NOT in 'done''
                    if len(temp) == 0: # if length of the product is 0 then all reports will have been exported already
                        raise AttributeError
                    else: # check if the last requested job_id has finished running
                        days = monthrange(year,month)[1]
                        cutoff = f'{year}-{month:02d}-{days:02d}' # generate date for last day of month
                        job_id = len(jobs_all)-1 # generates a number corresponding to the index of the last row
                        job_id = jobs_all.at[job_id, cutoff] # gets the value of the bottom cell of the column labeled with 'cutoff' date 
                        job_id = job_id.replace("'","") # Replace ' with blank so json.loads() works
                        job_id = json.loads(job_id) # converts str of site_ids into list
                        job_id = job_id[-1] # selects last item in list (which will be the first if there's only 1 item...)
                        response = requests.get(f'{stat_base_url}/bulk/status?id={job_id}&format=json')
                        response = response.json()
                        status = response.get('Response').get('Result').get('Status')
                        if status == 'Completed':
                            print('Exports are good to go, continuing with script!')
                            break
                        else:
                            raise SyntaxError
                except KeyError:
                    print('Exports are good to go, continuing with script!')
                    break
            except FileNotFoundError:
                print('\n'+'Reports have not been requested for this month yet.'
                      '\n'+'Please run stat_api_bulk_ranks_request_full_month.py'
                      '\n'+'for the month, or select a different month.')

        except ValueError:
            print('Invalid response. Please try again.')
        except FileNotFoundError:
            print('\n'+'Reports have not been requested for this month yet'
                  '\n'+'Please run stat_api_bulk_ranks_request_full_month.py,'
                  '\n'+'or select a different month.')
        except AttributeError:
            print('\n'+'This month\'s reports have already been exported for all'
                  '\n'+'sites. Please enter a different value.')
        except SyntaxError:
            print('\n'+'Reports for this month are not at \'Completed\'.'
                  '\n'+f'Status: \'{status}\'. Please enter a different value.')

# =============================================================================
# START
# =============================================================================

start_time = dt.datetime.now().replace(microsecond=0)
request_counter = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================

not_ranking = 120

## for messing around with errors:
# jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\Requests\{year}_{month:02d}_bulk_ranks_all.csv')
# jobs_all = jobs_all.drop(columns=['Status'])
# jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\Requests\{year}_{month:02d}_bulk_ranks_all.csv', index=False)

# =============================================================================
# SCRIPT
# =============================================================================

# =============================================================================
# Load bulk_ranks_all CSV for month
# =============================================================================

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

for index, row in jobs_head.iterrows(): # this construction allows us to go across rows rather than down, like with 'for i in jobs_head.index'
    ranks_month_df = pd.DataFrame()
    site_url = index
    save_name = site_url.replace('/','_') # Remove slashes so file will save/load properly
    save_name = save_name.replace('-','_') # Remove slashes so file will save/load properly
    save_name = save_name.replace('.','_') # Remove slashes so file will save/load properly
    print('\n'+f'Checking for existing data for {site_url}...')

# =============================================================================
# Load templates
# =============================================================================




# =============================================================================
# Continue with iteration through bulk_ranks df to get the keyword data
# =============================================================================

    print('\n'+f'Beginning {site_url}...'
          '\n'+f'{s} of {total_sites}')

    for i in range(len(row)):
        date = row.index[i]
        site_ids = row.values[i]
        if pd.isnull(site_ids): # checks if site_ids is empty - if it is, start loop again (i.e., try next cell)
            continue
        else:
            site_ids = site_ids.replace("'","") # Replace ' with blank so json.loads() works
            site_ids = json.loads(site_ids) # converts str of site_ids into list
            kw_list = [] # For adding keywords into for URLs with multiple sites
            j = 1
            print(f'Requesting {site_url} data for {date}...'
                  '\n'+f'(Site {s} of {total_sites})')
            for job_id in site_ids:
                stream_url = f'/bulk_reports/stream_report/{job_id}'
                print(j, stream_url)
                response = requests.get('https://iprospectman.getstat.com'+stream_url+f'?key={stat_key}')
                request_counter += 1 # stat requests counter
                response = response.json()
                new_kws = response.get('Response').get('Project').get('Site').get('Keyword')
                kw_list = kw_list + new_kws
                j += 1
                time.sleep(1)
            print('Data received!')

# =============================================================================
# Create DataFrame from kw_list using json_normalize
# =============================================================================

        total_kws = len(kw_list)
        print(f'Processing {total_kws} keywords...')

        ranks_day_df = json_normalize(kw_list)

        # adjust column label names
        ranks_day_df.columns = ranks_day_df.columns.str.replace('Google.','Google')
        ranks_day_df.columns = ranks_day_df.columns.str.split('.').str[-1]
        ranks_day_df = ranks_day_df.rename(columns={'Id':'KeywordId', 'date':'Date', 'type':'Type'})

        # create date_id from ranks_day_df[['Date', 'Id']] and insert at index [0]
        date_id = ranks_day_df[['Date', 'KeywordId']].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        ranks_day_df.insert(0, 'Id', date_id)

        try:
            ranks_day_df = ranks_day_df.drop(columns=['LocalSearchTrendsByMonth'])
        except KeyError:
            pass

        print('Processing complete!')

        #ranks_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\test.csv', index=False)


# =============================================================================

        print(f'Replacing not ranking with {not_ranking}...') # configured at the start under SETTINGS

        # replace 'None' with not_ranking in ['GoogleRank','GoogleBaseRank']
        ranks_day_df[['GoogleRank','GoogleBaseRank']] = ranks_day_df[['GoogleRank','GoogleBaseRank']].replace(to_replace=[None], value=not_ranking)

        print('Done!')

        ranks_month_df = ranks_month_df.append(ranks_day_df)
#        ranks_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\test_edit.csv', index=False)

#    these two breaks can be used together to test 1st day of month for 1 site without saving
#        break # for testing 1st day of month (this doesn't stop it saving)
#    break # for testing 1 client in list WITHOUT SAVING

# =============================================================================
# append full month to database
# =============================================================================

    print('Saving {} to database'.format(site_url))

    conn = sqlite3.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\stat_ranks_flat.db')

    with conn:

        c = conn.cursor()

        ##TODO: figure out a better way of doing this to avoid sql injection
        c.execute(
            'CREATE TABLE IF NOT EXISTS ' + save_name +  '''
            Id TEXT PRIMARY KEY,
            KeywordId INTEGER,
            Keyword TEXT,
            KeywordMarket TEXT,
            KeywordLocation TEXT,
            KeywordDevice TEXT,
            KeywordTranslation TEXT,
            KeywordCategories TEXT,
            KeywordTags TEXT,
            CreatedAt DATETIME,
            AdvertiserCompetition REAL,
            GlobalSearchVolume INTEGER,
            TargetedSearchVolume INTEGER,
            Apr INTEGER,
            Mar INTEGER,
            Feb INTEGER,
            Jan INTEGER,
            Dec INTEGER,
            Nov INTEGER,
            Oct INTEGER,
            Sep INTEGER,
            Aug INTEGER,
            Jul INTEGER,
            Jun INTEGER,
            May INTEGER,
            CPC REAL,
            Date DATETIME,
            Type TEXT,
            GoogleRank INTEGER,
            GoogleBaseRank INTEGER,
            GoogleUrl TEXT
            )'''
        )

        ranks_month_df.to_sql('{}'.format(save_name), conn, if_exists='append', schema='online', index=False)

        print('Done!')

#    break # for testing one client in script with saving BEFORE updating bulk_ranks_all

# =============================================================================
# Update jobs_all['Status'] with 'Done' & save file to create a save state
# =============================================================================

    print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv...')

    # Update jobs_all and save
    jobs_all.at[f'{site_url}','Status'] = 'Done'
    jobs_all = jobs_all.reset_index()  # reset index so it has a label when saved (problems using file in future otherwise...)
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\Requests\{year}_{month:02d}_bulk_ranks_all.csv', index=False)
    jobs_all = jobs_all.set_index('Url') # set index so next iteration works
    s += 1

    print('\n'+f'Done!')

#    break # for testing one client in script with saving AFTER updating bulk_ranks_all

# =============================================================================
# END TIMER
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
start_time = start_time.strftime('%H:%M:%S')
end_time = end_time.strftime('%H:%M:%S')

print('\n'+f'{date} DURN!')

print('\n'+f'{len(new_sites)} new sites found!')
if len(new_sites) != 0:
    print('The following sites are not currently set up:')
    for site in new_sites:
        print(site)
    print('Please run stat_api_keywords_list.py to correct this.')
print('\n'+f'Start:', start_time.strftime('%H:%M'),
      '\n'+f'End:', end_time.strftime('%H:%M'),
      '\n'+f'Time elapsed: {time_elapsed}',
      '\n'+f'Requests made: {request_counter}')