# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 09:29:47 2020

@author: JLee35
"""


import os
import time
import datetime
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
from xlsxwriter.utility import xl_range



# time script
start_time = datetime.datetime.now().replace(microsecond=0)

# =============================================================================
# create dates
# =============================================================================

report_month = datetime.datetime.now()
year = report_month.strftime( '%Y' )
month = report_month.strftime( '%m %b')
report_month = report_month.strftime( '%b-%y' )

# =============================================================================
# log in to deepcrawl
# =============================================================================

url = 'https://api.deepcrawl.com/sessions'
key = '1598'
value = 'FPXwkQUsX-qqqnhB_HLbHzO9brrycphhHKQwBwHOneNTP5Ur-5Dx_k2y_E57__4gWSGj810c'

# POST request to api.deepcrawl.com/sessions with the API key and value
response = requests.post( url, auth=( key, value ), verify=True )

# convert JSON string into dict
response_json = response.json()

# assign the 'token' in the dict to token variable
token = response_json[ 'token' ]

# create variable for adding into header of requests
headers = { 'X-Auth-Token' : token }

# print 'token'
print(token)


'''
# =============================================================================
# update healthcheck templates
# =============================================================================
'''
# load healthcheck list
hc_list = pd.read_excel( r'L:\Commercial\Operations\Technical SEO\Technical Healthchecks\Healthcheck List.xlsx', sheet_name = 'Healthcheck List' )

#iterate through list to grab data, ping server to get data, then update reports
for i in hc_list.index:
    n = 0
    client = hc_list['Client'][i]
    project_id = hc_list['Project ID'][i]
    if project_id == '':
        print( f'{client} does not have a project ID in the Healthchek List. Please rectify.')
        continue
    else:
        print( f'\nInitialising {client}' )
        pass

    brand = hc_list[ 'Brand' ][i]
    url = f'https://api.deepcrawl.com/accounts/117/projects/{project_id}'
    response = requests.get( url, headers=headers )

# handle errors if project does not exists on DeepCrawl

    if response.status_code != 200:
        print( '{client} Project does not exist on DeepCrawl - please check Project ID on Healthcheck List' )
        continue
    else:
        response_json = response.json()


# if no last crawl, skip client
    site_primary = response_json.get( 'site_primary' )
    last_crawl = response_json.get( '_crawls_last_href' )
    if last_crawl == None:
        print( f'{client} has no _crawls_last_href.')
        continue

# begin building dataframe for crawl

    print( f'Getting Data for {site_primary}')
    df = pd.DataFrame()                                                                 # create a blank DataFrame for the project
    url = 'https://api.deepcrawl.com' + last_crawl + '/reports?page=1&per_page=200'     # create a URL from the href 
    crawl = requests.get( url, headers=headers )                                        # ping the URL to get the data

    # check status code and retry if not 200
    while True:
        if crawl.status_code != 200:
            print(f'\n{crawl} \nRetrying...\n')
            time.sleep(2)
            crawl = requests.get( url, headers=headers )                                    # ping the URL again to get the data
        else:
            print('Success!\n')
            break

    crawl_json = crawl.json()                                                           # convert the JSON response into something pythonic
    df = df.append( crawl_json, ignore_index = True )                                   # fill the DataFrame with data from the response
    n += 1                                                                              # add 1 to 'n'
    print( n, url )                                                                     # print 'n, url' so the script shows it's not idle
    while url != 'https://api.deepcrawl.com' + crawl.links[ 'last' ][ 'url' ]:          # check if the URL is the same as the last page in the response header
        url = 'https://api.deepcrawl.com' + crawl.links[ 'next' ][ 'url']               # if not, change the url to the 'next' page in the pagination
        crawl = requests.get( url, headers=headers )                                    # ping the new url to get the data
        crawl_json = crawl.json()                                                       # convert the JSON response into something pythonic
        df = df.append( crawl_json, ignore_index = True )                               # append the data to the DataFrame
        n += 1                                                                          # add 1 to 'n'
        print( n, url )                                                                 # print 'n, url' so the script shows it's not idle

# remove duplicates from 'report_template' column

    df = df.drop_duplicates( subset = 'report_template'  )                              # de-dupe the sreadsheet based on the 'report template' column

# filter columns for 'report_template' and 'basic_total'

    df = df[[ 'report_template', 'basic_total' ]]                                       # remove all columns from sheet except 'report_template' and 'basic_total' columns

# open template

# see if client's template exists, and if not, open blank template

    try:
        df_data = pd.read_excel( fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\{client} - Tech Healthcheck Template.xlsx', sheet_name = 'Data' )
        df_healthcheck = pd.read_excel( fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\{client} - Tech Healthcheck Template.xlsx', sheet_name = 'Healthcheck' )

    except FileNotFoundError:
        df_data = pd.read_excel( fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Blank Tech Healthcheck Template.xlsx', sheet_name = 'Data', read_only=True )

# update 'data' tab

    df_data = df_data.merge( df, on= 'report_template', how='left' )
    df_data = df_data.rename( columns={ 'basic_total' : report_month })
    df_data = df_data.fillna( 0 )

    print( '\n- Data Tab Updated' )


# update 'healthcheck' tab

    sumif_data = df_data[[ 'Issue', report_month ]]
    sumif_data = sumif_data.groupby( 'Issue' )[report_month].sum()
    df_healthcheck = df_healthcheck.merge( sumif_data , on='Issue', how='left' )

    print( '- Healthcheck Tab Updated' )

# save template

    writer = pd.ExcelWriter( fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\{client} - Tech Healthcheck Template.xlsx', engine='xlsxwriter' )
    df_data.to_excel(writer, sheet_name = 'Data', index=False )
    df_healthcheck.to_excel(writer, sheet_name = 'Healthcheck', index=False )
    # set zoom at 85
    data_ws = writer.sheets[ 'Data' ]
    healthcheck_ws = writer.sheets[ 'Healthcheck' ]
    data_ws.set_zoom(85)
    healthcheck_ws.set_zoom(85)
    writer.save()

    print( f'\n{client} Template Saved'
            '\n'
            '\n*   *   *   *   *   *   *   *   *   *' )

print( '\n-----------------------------'
       '\nHealthcheck Templates Updated'
       '\n-----------------------------' )




