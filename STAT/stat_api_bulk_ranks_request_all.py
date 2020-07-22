# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 07:38:35 2020

First draft completed on 17/06/2020

@author: jon.lee@iprospect.com
"""

import os
import time
import json
import requests
import openpyxl
import pandas as pd
import datetime as dt
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

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

# =============================================================================
# SET MONTH AND YEAR (NUMERIC)
# =============================================================================

'''
This part of the script is a request to input a year and a month.
I've included a few systems that restrict the input to prevent requesting
reports for further back than the STAT API alows, to avoid reports from
being generated more than once, and to stop data being overwritten.

I've also included a sort of save state later in the script and, in this 
part, there's a construction that will allow us to pick up from roughly
where we left off if the script breaks.

# =============================================================================
# Rejecting input for further back than STAT allows, or dates in the future
# =============================================================================

davids_law and its various iterations calculate a relative date range based
on the current month that allow us to go back no more than 2 years. Input
will be rejected if outside this range, and you'll be requested to enter
another value.

# =============================================================================
# Check if report has been requested
# =============================================================================

If the input is within the date range, the script checks to see if reports
have been requested. This is achieved by trying to load a CSV file that is 
created by this script, based on its name. If the file does not exist, then
we can assume that the script has not been executed for this month. The 
script then breaks the input request cycle and starts requesting reports
for all clients from the start of the month input.

# =============================================================================
# Check report to see what the last date is
# =============================================================================

Once the script has loaded the CSV into a DataFrame, it loads the label of 
the last column (start_date). It then tries to convert start_date to a 
datetime datatype, and adds 1 to it to make it the next day. It then checks
if this date is greater than the last date in the input month ('cutoff').

If start_date > cutoff, reject input. 

This prevents the script for requesting more reports if the last day of the
requested month has already run.

else, start requesting reports from start_date + 1 day

This picks up from the day after the last saved date, so it works as a sort
of save state.

2 date formats have been included. This is because if you open a CSV file in
excel and save it, it changes the date format.

saved format: YYYY-MM-DD
excel format: DD/MM/YYYY

The idea here is that it will recognise either due to a nested 'try:' with 
appropriate breaks and exceptions.

# =============================================================================
# Catch exception if last column is not a date
# =============================================================================

When you run the stat_api_bulk_ranks_export_all.py script, it adds a column 
called 'Status', which is used as another sort of save state for that script.

Here, if last column name of the loaded file is 'status', it will be of the 
wrong dtype to convert into a date. If this is the case, the code raises
an exception and you'll be requested to enter another value.

'''

# =============================================================================
# Introduction
# =============================================================================

print('\n'+'Welcome to the STAT API Bulk Ranks Request script!'
      '\n'+'This script requests daily ranks for all clients in'
      '\n'+'a single, specified month.'
      '\n'+''
      '\n'+'Please answer the following questions:')


davids_law1 = int(dt.datetime.today().strftime('%Y')) - 2
davids_law2 = int(dt.datetime.today().strftime('%Y'))
  
print('\n'+'Which year would you like to run reports for?')
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
    davids_law3 = int(dt.datetime.today().strftime('%m'))+1
    davids_law4 = 12
elif year == davids_law2:
    davids_law3 = 1
    davids_law4 = int(dt.datetime.today().strftime('%m'))
else:
    davids_law3 = 1
    davids_law4 = 12


print('\n'+'Which month would you like to run reports for?')
while True:
    try: # check if input is within davids_law
        month = int(input(f'Please enter a value between {davids_law3}-{davids_law4} >>> '))
        if month < davids_law3 or month > davids_law4: #if not, raise ValueError
            raise ValueError
        else: # else calculate the last day in the month and create a date with it
            days = monthrange(year,month)[1]
            cutoff = f'{year}-{month:02d}-{days:02d}'
            try: # then, check if a JobId file has already been created. if not, raise AttributeError
                jobs_all = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Historical\Requests\{year}_{month:02d}_bulk_ranks_all.csv')
                start_date = jobs_all.iloc[:,-1].name
                try: # after loading the last column's name, check if it can be converted into a date, and add 1 to it (i.e., is not 'Status' and is being processed)
                    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d') 
                    start_date = start_date + dt.timedelta(days=1)
                    start_date = start_date.strftime('%Y-%m-%d')
                    if start_date > cutoff: # if the date created is greater than the last day of the month created earlier, reject 
                        raise AttributeError
                    else:
                        break
                except ValueError: 
                    try: # after loading the last column's name, check if it can be converted into a date, and add 1 to it (i.e., is not 'Status' and is being processed)
                        start_date = dt.datetime.strptime(start_date, '%d/%m/%Y') 
                        start_date = start_date + dt.timedelta(days=1)
                        start_date = start_date.strftime('%Y-%m-%d')
                        if start_date > cutoff: # if the date created is greater than the last day of the month created earlier, reject                 except:
                            raise AttributeError
                        else:
                            break
                    except ValueError:
                        raise AttributeError
            except FileNotFoundError:
                start_date = f'{year}-{month:02d}-01'
                break
    except ValueError:
        print('Invalid response.')
    except AttributeError:
        print('\n'+'Reports have already been requested for this month.'
              '\n'+'Please choose a different date.')

# =============================================================================
# START
# =============================================================================

# start a timer to show how long the script takes (see bottom for end_time)
start_time = dt.datetime.now().replace(microsecond=0)
request_counter = 0 # stat api request counter SRC

# =============================================================================
# SCRIPT
# =============================================================================

# =============================================================================
# Load full site list from STAT API
# =============================================================================

print('\n'+'Requesting Site List from STAT...')

sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
response = requests.get(sites_all_url)
request_counter += 1
response = response.json()

total_results = response.get('Response').get('totalresults')

site_list = response.get('Response').get('Result')
print('Site List received!')
site_list = pd.DataFrame(site_list)
 
# Filter site_list so it shows only tracked sites with >0 keywords
print('\n'+'Filtering site list...')

## Remove False values from 'Tracking' column to leave only tracked sites
site_list = site_list[site_list['Tracking'].str.contains('^true')]

## Ensure 'TotalKeywords' column is set to 'int' (for next line)
site_list['TotalKeywords'] = site_list['TotalKeywords'].astype(int)

## Remove sites that have 0 keywords
site_list = site_list.drop(site_list[site_list['TotalKeywords'] == 0].index)
print('Removed untracked sites!')

site_list = site_list.reset_index(drop=True)

# Drop rows from site_list that were 'CreatedAt' after end of current month
for i in site_list.index:
    if site_list['CreatedAt'][i] >= cutoff:
        site_list = site_list.drop(i)
        
# reset index
site_list = site_list.reset_index(drop=True)

## Get values for total no. of keywords and total no. of sites
total_kws = site_list['TotalKeywords'].sum()

# =============================================================================
# Request Bulk Ranks for sites in site_list on 'report_date' 
# =============================================================================

if start_date == f'{year}-{month:02d}-01':
    jobs_all = site_list[['Url']]
    jobs_all = jobs_all.set_index('Url')
else:
    pass

for date in pd.date_range(start=start_date, end=cutoff):
    date = date.strftime('%Y-%m-%d')   
    print('\n'+f'Requesting bulk rank exports for {date}')

    # for each site_id in site_list, call API and request reports for date
    n = 1 # counter for 'total_sites' 
    job = pd.DataFrame(columns=['Url', f'{date}'])
    for i in site_list.index:
        if site_list['CreatedAt'][i] <= date:
            # pull Id from client_list and create url 
            site_url = site_list['Url'][i]
            site_id = site_list['Id'][i]
            try:
                print(f'{n:03d} Requesting {date} rank report for {site_url}')
                url = f'{stat_base_url}/bulk/ranks?date={date}&site_id={site_id}&engines=google&format=json'
            
                # request bulk ranks from url, adding job_id to list
                response = requests.get(url)
                request_counter += 1
                response = response.json()
                job_id = response.get('Response').get('Result').get('Id')
                temp = {
                    'Url': site_url,
                    f'{date}' : job_id
                    }
                job = job.append(temp, ignore_index=True)
                n += 1
            except:
                continue
        else:
            continue
    job = job.groupby('Url', as_index=True).aggregate(lambda x : list(x)) # as_index defaults to 'True', it's there just for future reference!
    print('\n'+f'Bulk rank requests for {date} complete!')
    n = 1 # reset counter for 'total_sites'
    print('\n'+f'Updating {year}_{month:02d}_bulk_ranks_all.csv')
    jobs_all = jobs_all.reset_index() # issue with concat removing index name, hence this and next line...
    jobs_all = jobs_all.rename(columns={'index':'Url'}) # renames new index to 'Url'
    jobs_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Historical\Requests\{year}_{month:02d}_bulk_ranks_all.csv',index=False)
    jobs_all = jobs_all.set_index('Url')
    print('Done!')
    time.sleep(1)
    
# =============================================================================
# END
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time
print('\n'+'DURN!'
      '\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {request_counter}')
