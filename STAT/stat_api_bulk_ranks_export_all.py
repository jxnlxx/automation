# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 09:08:57 2020

@author: JLee35
"""

import sys
import time
import json
import requests
import pandas as pd
import datetime as dt

from calendar import monthrange

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
    davids_law3 = int(dt.datetime.today().strftime('%m'))
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
                jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Requests\{year}_{month:02d}_bulk_ranks_all.csv')
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

# =============================================================================
# jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Requests\{year}_{month:02d}_bulk_ranks_all.csv')
# jobs_all = jobs_all.drop(columns=['Status'])
# jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Requests\{year}_{month:02d}_bulk_ranks_all.csv', index=False)
# =============================================================================

# =============================================================================
# SCRIPT
# =============================================================================

#%%
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
    site_url = index
    save_name = site_url.replace('/','_') # Remove slashes so file will save/load properly
    save_name = site_url.replace('-','_') # Remove slashes so file will save/load properly   
    print('\n'+f'Checking for existing data for {site_url}...')

#%%
# =============================================================================
# Load templates
# =============================================================================

    try:
        # try loading client's historical data with dtype=object. no calculations are needed and helps with memory usage (i think)
        google_rank_df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Rank\{save_name}_google_rank.csv', dtype=object)
        print(f'{site_url}_google_base_rank.csv found!')

        google_base_rank_df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Base Rank\{save_name}_google_base_rank.csv', dtype=object)
        print(f'{site_url}_google_rank.csv found!')

        google_ranking_url_df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Ranking URL\{save_name}_google_ranking_url.csv', dtype=object)
        print(f'{site_url}_google_ranking_url.csv found!')

    except FileNotFoundError:
        try:
            # If it doesn't exist, try loading the client's keywords_list (see stat_api_keywords_list.py).
            google_rank_df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Keywords List\{save_name}_keywords_list.csv', dtype=object)
            google_rank_df = google_rank_df[['keyword_id', 'keyword', 'device', 'market', 'regional_search_volume']] # filter the kw list to remove any unnecessary columns
            google_base_rank_df = google_rank_df # this just duplicates the filtered keywords_list dataframe 
            google_ranking_url_df = google_rank_df # as above
            print(f'No historical data found; loaded {save_name}_keywords_list.csv instead.')
        
        except FileNotFoundError:
            # if there's no kw_list, add to new_sites list to be printed at end of script
            new_sites.append(site_url)
            print(f'{site_url} setup required; a reminder has been set!')
            s += 1
            continue

# %%
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
            print('Data received!')

# =============================================================================
# Create DataFrame from kw_list using list comprehension
# =============================================================================

        total_kws = len(kw_list)
        print(f'Processing {total_kws} keywords...')

        ranks_df = pd.DataFrame({
            'date' : [i['Ranking']['date'] for i in kw_list],
            'url' : site_url,
            'keyword_id' : [i['Id'] for i in kw_list],
            'keyword' : [i['Keyword'] for i in kw_list],
            'device' : [i['KeywordDevice'] for i in kw_list],
            'market' : [i['KeywordMarket'] for i in kw_list],
            'regional_search_volume' : [i['KeywordStats']['TargetedSearchVolume'] for i in kw_list],
            'google_rank' : [i['Ranking']['Google']['Rank'] for i in kw_list],
            'google_base_rank' : [i['Ranking']['Google']['BaseRank'] for i in kw_list],
            'google_ranking_url' : [i['Ranking']['Google']['Url'] for i in kw_list],
            'keyword_tags': [i['KeywordTags'] for i in kw_list],
            })
        
        print('Processing complete!')
        
# =============================================================================

        print(f'Replacing not ranking with {not_ranking}...') # configured at the start under SETTINGS

        # replace 'None' with not_ranking in ['google_rank','google_base_rank']
        ranks_df[['google_rank','google_base_rank']] = ranks_df[['google_rank','google_base_rank']].replace(to_replace=[None], value=not_ranking)

        # replace 'None' with '' in ['google_ranking_url']
        ranks_df[['google_ranking_url']] = ranks_df[['google_ranking_url']].replace(to_replace=[None], value='')

        # replace 'N/A' with '' in ['google_rank','google_base_rank','google_ranking_url'] (N/A keywords were not being tracked on 'date')
        ranks_df[['google_rank','google_base_rank','google_ranking_url']] = ranks_df[['google_rank','google_base_rank','google_ranking_url']].replace(to_replace=['N/A'], value='')
        
        print('Done!')

# =============================================================================

        print('Formatting results...')  

        # convert 'keyword_id' col into dtype 'str' so merge works
        ranks_df.iloc[:, :5] = ranks_df.iloc[:, :5].astype(str) 

        # split out ranks_df into different DataFrames such that each contain
        # only one of google_rank, google_base_rank, and google_ranking_url
        gr_df = ranks_df.drop(columns=['google_base_rank','google_ranking_url'])
        gbr_df = ranks_df.drop(columns=['google_rank','google_ranking_url'])
        gru_df = ranks_df.drop(columns=['google_rank','google_base_rank'])

        # rename columns with 'date'         
        gr_df = gr_df.rename(columns={'google_rank' : f'{date}'})
        gbr_df = gbr_df.rename(columns={'google_base_rank' : f'{date}'})
        gru_df = gru_df.rename(columns={'google_ranking_url' : f'{date}'})
    
        print('Done!')

# =============================================================================
         
        print('Updating existing keywords...')
        
        # create new DataFrame containing only 'keyword_id' and 'date'
        # this prevents duplication of 'keyword', 'device', 'market', regional_search_volume'
        gr_df2 = gr_df[['keyword_id', f'{date}']]
        gbr_df2 = gbr_df[['keyword_id', f'{date}']]
        gru_df2 = gru_df[['keyword_id', f'{date}']]

        # add new data to historical data (uses left table to include only historical data)
        google_rank_df = pd.merge(google_rank_df, gr_df2, how='left', on='keyword_id')  
        google_base_rank_df = pd.merge(google_base_rank_df, gbr_df2, how='left', on='keyword_id')  
        google_ranking_url_df = pd.merge(google_ranking_url_df, gru_df2, how='left', on='keyword_id')  
        print('Done!')
        
# =============================================================================        

        print('Checking for new keywords...')

        # drop keywords from ['gr_df', 'gbr_df', 'gru_df'] that are already in ['google_rank_df', 'google_base_rank_df', 'google_ranking_url_df']
        # append what remains as new rows (includes ['keyword', 'device', 'market', regional_search_volume'] that weren't in the above 'df2' merge)
        cond = gr_df['keyword_id'].isin(google_rank_df['keyword_id'])
        cond = gr_df.drop(gr_df[cond].index)
        google_rank_df = google_rank_df.append(cond, sort=False)

        cond = gbr_df['keyword_id'].isin(google_base_rank_df['keyword_id'])
        cond = gbr_df.drop(gbr_df[cond].index)
        google_base_rank_df = google_base_rank_df.append(cond, sort=False)
        
        cond = gru_df['keyword_id'].isin(google_ranking_url_df['keyword_id'])
        cond = gru_df.drop(gru_df[cond].index)
        google_ranking_url_df = google_ranking_url_df.append(cond, sort=False)

        print(len(cond),'new keywords found!')

#    these two breaks can be used together to test 1st day of month for 1 site without saving 
#        break # for testing 1st day of month (this doesn't stop it saving)
#    break # for testing 1 client in list WITHOUT SAVING

# =============================================================================
# When all days of month have been done for site, save to CSVs
# =============================================================================

    print('\n'+f'Updating {site_url} CSV files...')

    print(f'Saving {save_name}_google_rank.csv')
    google_rank_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Rank\{save_name}_google_rank.csv', index=False)
    print('Done!')

    print(f'Saving {save_name}_google_base_rank.csv')
    google_base_rank_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Base Rank\{save_name}_google_base_rank.csv', index=False)
    print('Done!')

    print(f'Saving {save_name}_google_ranking_url.csv')
    google_ranking_url_df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Ranking URL\{save_name}_google_ranking_url.csv', index=False)
    print('Done!')
    
    print(f'{site_url} complete! ({s} of {total_sites})')

    s += 1

#    break # for testing one client in script with saving BEFORE updating bulk_ranks_all

# =============================================================================
# Update jobs_all['Status'] with 'Done' & save file to create a save state
# =============================================================================

    print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv...')

    # Update jobs_all and save
    jobs_all.at[f'{site_url}','Status'] = 'Done' 
    jobs_all = jobs_all.reset_index()  # reset index so it has a label when saved (problems using file in future otherwise...)
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Requests\{year}_{month:02d}_bulk_ranks_all.csv', index=False)        
    jobs_all = jobs_all.set_index('Url') # set index so next iteration works

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