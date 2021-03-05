# -*- coding: utf-8 -*-
"""
@author: Jon Lee - jon.lee@iprospect.com

This is a script that allows you to call the google search console API and 
request data. The date range is defined at the top of the script

"""
import re
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

import httplib2
from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from collections import defaultdict
from dateutil import relativedelta
import argparse
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from urllib.parse import urlparse

# =============================================================================
# Introduction
# =============================================================================

print('\n'+'Welcome to the Google Search Console Site Data Export script!'
      '\n'+'This script requests daily clicks, impressions, ctr and position'
      '\n'+'for all clients in a single, specified month.'
      '\n'+''
      '\n'+'Please answer the following questions:')


davids_law1 = int(dt.datetime.today().strftime('%Y')) - 1
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
        print('Invalid response. Please try again.')

if year == davids_law1:
    davids_law3 = int(dt.datetime.today().strftime('%m'))-3
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
        else: # else create a start_date and an end_date for the month
            days = monthrange(year,month)[1]
            start_date = f'{year}-{month:02d}-01'
            end_date = f'{year}-{month:02d}-{days:02d}'
            break
    except ValueError:
        print('Invalid response. Please try again.')

# =============================================================================
# START
# =============================================================================

start_time = dt.datetime.now().replace(microsecond=0)
request_counter = 0 # stat requests counter


# =============================================================================
# API SECRETS
# =============================================================================

CLIENT_SECRETS_PATH = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\iprospectseonorth\OAuth2\credentials.json'
WEBMASTER_CREDS_DAT = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\iprospectseonorth\OAuth2\webmaster_credentials.dat'
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

# =============================================================================
# AUTHENTICATE
# =============================================================================

# Create a parser to be able to open browser for Authorization
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])
flags = parser.parse_args([])

flow = client.flow_from_clientsecrets(
    CLIENT_SECRETS_PATH, scope = SCOPES,
    message = tools.message_if_missing(CLIENT_SECRETS_PATH))

# Prepare credentials and authorize HTTP
# If they exist, get them from the storage object
# credentials will get written back to a file.
storage = file.Storage(WEBMASTER_CREDS_DAT)
credentials = storage.get()

# If authenticated credentials don't exist, open Browser to authenticate
if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
http = credentials.authorize(http=httplib2.Http())
service = build('webmasters', 'v3', http=http)

# =============================================================================
# REQUEST SITE LIST FROM SEARCH CONSOLE
# =============================================================================

site_list = service.sites().list().execute()
site_list = site_list.get('siteEntry')
site_list = pd.DataFrame.from_records(site_list)
site_list = site_list[~site_list['permissionLevel'].isin(['siteUnverifiedUser'])] # 'isin' part is a list, so just add any more conditions
site_list = site_list.reset_index(drop=True)
total_sites = len(site_list)

#site_list = site_list.head(2) # limits number of sites for testing

# Iterate through site_list and get data for each client
for i in site_list.index:
    site_url = site_list['siteUrl'][i]
    site_name = site_url.replace('/','_') # Remove slashes so file will save/load properly
    site_name = site_name.replace(':','_') # Remove slashes so file will save/load properly
    print(f'Getting data for {site_url}'
          '\n'+'(Site {i+1} of {total_sites})')
    try:
        historical_clicks = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Clicks\{site_name}_sc_clicks.csv')
        historical_impressions = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Impressions\{site_name}_sc_impressions.csv')
        historical_ctr = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\CTR\{site_name}_sc_ctr.csv')
        historical_position = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Position\{site_name}_sc_position.csv')
        try:
            last_date = historical_clicks.iloc[:,-1].name
            if last_date >= end_date:
                print(f'{site_url} data already obtained for this month. Moving onto next site!')
                continue
        except AttributeError:
            pass
    except FileNotFoundError:
        historical_clicks = ''
        historical_impressions = ''
        historical_ctr = ''
        historical_position = ''
        pass

    for date in pd.date_range(start=start_date, end=end_date):
        date = date.strftime('%Y-%m-%d')
        try:
            last_date = historical_clicks.iloc[:,-1].name
            if last_date >= date:
                print(f'{site_url} data already obtained for this day. Moving onto next day!')
                continue
            else:
                print(f'Requesting {site_url} data for {date}...'
                      '\n'+'(Site {i+1} of {total_sites})')
                pass
        except AttributeError:
            pass

        # populate the request with details
        # available 'dimensions' are - 'date', 'country', 'device', 'page', 'query', 'searchAppearance'
        max_row = 25000
        start_row = 0
        status = ''
        request = {
              'startDate': date,
              'endDate': date,
              'dimensions': ['date', 'page', 'query','country', 'device'], # uneditable to enforce a nice clean dataframe at the end!
              'rowLimit': 25000, # valid range is 1 - 25000; default is 1000
              'startRow': 0 # '0' starts it at the beginning
               }


       # response = service.searchanalytics().query(siteUrl=client_url, body=request).execute()
        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        request_counter += 1
        try:
            total_kws = len(response['rows'])
        except KeyError:
            print(f'No data received for {site_url} on {date}')
            time.sleep(1)
            break
        try:
            sc_dict = defaultdict(list)
            print('Processing results...')
            #Process the response
            for row in response['rows']:
                sc_dict['date'].append(row['keys'][0] or 0)
                sc_dict['page'].append(row['keys'][1] or 0)
                sc_dict['query'].append(row['keys'][2] or 0)
                sc_dict['country'].append(row['keys'][3] or 0)
                sc_dict['device'].append(row['keys'][4] or 0)
                sc_dict['clicks'].append(row['clicks'] or 0)
                sc_dict['ctr'].append(row['ctr'] or 0)
                sc_dict['impressions'].append(row['impressions'] or 0)
                sc_dict['position'].append(row['position'] or 0)
            print('successful at %i' % max_row)

        except KeyError:
            print('error occurred at %i' % max_row)
            continue
        #Add response to dataframe response

        df = pd.DataFrame(data = sc_dict)
        print('Filtering results...')
        df['clicks'] = df['clicks'].astype('int')
        df['ctr'] = df['ctr']*100
        df['impressions'] = df['impressions'].astype('int')
        df['position'] = df['position'].round(2)
        df = df.sort_values('impressions', ascending = False)
        df = df.head(2500)

        ## create separate DataFrames such that each one contains only one of
        ## clicks, impressions, ctr, and position
        clicks_df = df.drop(columns=['date', 'impressions', 'ctr', 'position'])
        impressions_df = df.drop(columns=['date', 'clicks', 'ctr', 'position'])
        ctr_df = df.drop(columns=['date', 'clicks', 'impressions', 'position'])
        position_df = df.drop(columns=['date', 'clicks', 'impressions', 'ctr'])

        ## change name of columns to 'date'
        clicks_df = clicks_df.rename(columns={'clicks' : f'{date}'})
        impressions_df = impressions_df.rename(columns={'impressions' : f'{date}'})
        ctr_df = ctr_df.rename(columns={'ctr' : f'{date}'})
        position_df = position_df.rename(columns={'position' : f'{date}'})
        print('Done!')
        ## merge new data with old data
        print('Merging data...')
        try:
            historical_clicks = pd.merge(historical_clicks, clicks_df, copy=False, how='outer', on=['page','query','country','device'])#, right_on=['page','query','country','device'])
            historical_impressions = pd.merge(historical_impressions, impressions_df, copy=False, how='outer', on=['page','query','country','device'])#, right_on=['page','query','country','device'])
            historical_ctr = pd.merge(historical_ctr, ctr_df, copy=False, how='outer', on=['page','query','country','device'])#, right_on=['page','query','country','device'])
            historical_position = pd.merge(historical_position, position_df, copy=False, how='outer', on=['page','query','country','device'])#, right_on=['page','query','country','device'])
            print('Done!')
        except TypeError:
            historical_clicks = clicks_df
            historical_impressions = impressions_df
            historical_ctr = ctr_df
            historical_position = position_df
            print('No historical data; new entry created...')
    print('Exporting data...')
    try:
        historical_clicks.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Clicks\{site_name}_sc_clicks.csv', index=False)
        historical_impressions.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Impressions\{site_name}_sc_impressions.csv', index=False)
        historical_ctr.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\CTR\{site_name}_sc_ctr.csv', index=False)
        historical_position.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Google APIs\Search Console\Data\Position\{site_name}_sc_position.csv', index=False)
    except AttributeError:
        print('No data to export; moving onto next client...')
    print('Done!')

 #   break

# =============================================================================
# END TIMER
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time

print('\n'+'DURN!')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {request_counter}')

