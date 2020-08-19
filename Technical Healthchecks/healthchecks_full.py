# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 12:16:23 2020

@author: Jon Lee
"""

import os
import time
import shutil
import datetime as dt
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
from xlsxwriter.utility import xl_range
from deepcrawl import dc_key, dc_value, dc_url                                        # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib
import gspread
import gspread_formatting
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

# time script
start_time = dt.datetime.now().replace(microsecond=0)

# =============================================================================
# create dates
# =============================================================================

report_month = start_time.replace(day=1)
backup_month = report_month - dt.timedelta(days=1)
report_month = report_month.strftime('%b-%y')
backup_month = backup_month.strftime('%Y-%m')

# =============================================================================
# backup healthcheck templayes if no backup exists
# =============================================================================

n = 1
if os.path.exists(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\Backup\{backup_month}'):
    pass
else:
    print('Creating backup of data...')
    os.mkdir(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\Backup\{backup_month}')
    src_dir = fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data'
    dst_dir = fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\Backup\{backup_month}'
    time.sleep(1)
    for filename in os.listdir(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data'):
        if filename.endswith(".xlsx"):
            if filename.startswith('~$'): # skips hidden temp files
                continue
            else:
                src_file = os.path.join(src_dir, filename)
                dst_file = os.path.join(dst_dir, filename)
                print(n, filename)
                shutil.copy(src_file, dst_file)
                n += 1
        else:
            pass
    print('Done!')

# =============================================================================
# load google sheets tech seo projects list
# =============================================================================

# load healthcheck list

gspread_id = '1H3qkPyGolEbq3pMpHYEWEb0kZkudy6mnbJT90L2kl1k'
gsheet_name = 'Tech Healthchecks'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

# load data to dataframe from gsheet
print('Retrieving client list from Google Sheets...')
hc_list = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True)# usecols=range(0, NUMBERCOLS))

# loading gsheets automatically loads 25 cols x 1k rows, so we trim it:
hc_list = hc_list.dropna(axis=1, how='all')
hc_list = hc_list.dropna(axis=0, how='all')
print('Done!')

# =============================================================================
# log in to deepcrawl
# =============================================================================

# POST request to api.deepcrawl.com/sessions with the API key and value
print('Logging into DeepCrawl...')
response = requests.post(dc_url, auth=(dc_key, dc_value), verify=True)
response = response.json()
token = response['token'] # gets the token from the json response
headers = {'X-Auth-Token':token} # create variable for adding into header of requests
print('Done!')

# =============================================================================
# update healthcheck templates
# =============================================================================
skipped_clients = list()
#iterate through list to grab data, ping server to get data, then update reports
for i in hc_list.index:
    n = 0
    client = hc_list['Client'][i]
    project_id = int(hc_list['deepcrawl_project_id'][i])
    if project_id == '':
        print(f'{client} does not have a project ID in the Healthchek List. Please rectify.')
        continue
    else:
        print(f'\nInitialising {client}')
        pass

    brand = hc_list['Brand'][i]
    url = f'https://api.deepcrawl.com/accounts/117/projects/{project_id}'
    response = requests.get(url, headers=headers)

    # handle errors if project does not exists on DeepCrawl
    if response.status_code != 200:
        print('{client} Project does not exist on DeepCrawl - please check Project ID on Healthcheck List')
        continue
    else:
        response_json = response.json()

    # if no last crawl, skip client
    site_primary = response_json.get('site_primary')
    last_crawl = response_json.get('_crawls_last_href')
    if last_crawl == None:
        print(f'{client} has no _crawls_last_href.')
        continue

    # begin building dataframe for crawl
    print(f'Getting Data for {site_primary}')
    df = pd.DataFrame()                                                                 # create a blank DataFrame for the project
    url = 'https://api.deepcrawl.com' + last_crawl + '/reports?page=1&per_page=200'     # create a URL from the href 
    crawl = requests.get(url, headers=headers)                                          # ping the URL to get the data

    # check status code and retry if not 200
    while True:
        if crawl.status_code != 200:
            print(f'\n{crawl} \nRetrying...\n')
            time.sleep(2)
            crawl = requests.get(url, headers=headers)                                    # ping the URL again to get the data
        else:
            print('Success!\n')
            break

    crawl_json = crawl.json() # convert the JSON response into something pythonic
    df = df.append(crawl_json, ignore_index = True) # fill the DataFrame with data from the response
    n += 1 # add 1 to 'n'
    print(n, url) # print 'n, url' so the script shows it's not idle
    while url != 'https://api.deepcrawl.com' + crawl.links['last']['url']: # check if the URL is the same as the last page in the response header
        url = 'https://api.deepcrawl.com' + crawl.links['next']['url'] # if not, change the url to the 'next' page in the pagination
        crawl = requests.get(url, headers=headers) # ping the new url to get the data
        crawl_json = crawl.json() # convert the JSON response into something pythonic
        df = df.append(crawl_json, ignore_index = True) # append the data to the DataFrame
        n += 1 # add 1 to 'n'
        print(n, url) # print 'n, url' so the script shows it's not idle

    # try removing duplicates from 'report_template' col, filter for 'report_template' and 'basic_total' cols
    try:
        df = df.drop_duplicates(subset = 'report_template')
        df = df[['report_template', 'basic_total']]
    # if 'KeyError', add client name to list for mesaging at end, then skip client
    except KeyError:
        skipped_clients = skipped_clients + [f'{client}']
        continue

    # see if client's template exists, and if not, open blank template

    try:
        df_data = pd.read_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\{client} - Tech Healthcheck Template.xlsx', sheet_name='Data')
        df_healthcheck = pd.read_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\{client} - Tech Healthcheck Template.xlsx', sheet_name='Healthcheck')

    except FileNotFoundError:
        df_data = pd.read_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Blank Tech Healthcheck Template.xlsx', sheet_name='Data', read_only=True)
        df_healthcheck = pd.read_excel(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Blank Tech Healthcheck Template.xlsx', sheet_name='Healthcheck', read_only=True)

    # update 'data' tab

    df_data = df_data.merge(df, on= 'report_template', how='left')
    df_data = df_data.rename(columns={'basic_total':report_month})
    df_data = df_data.fillna(0)

    print('\n- Data Tab Updated')


    # update 'healthcheck' tab

    sumif_data = df_data[['Issue', report_month]]
    sumif_data = sumif_data.groupby('Issue')[report_month].sum()
    df_healthcheck = df_healthcheck.merge(sumif_data , on='Issue', how='left')

    print('- Healthcheck Tab Updated')

    # save template

    writer = pd.ExcelWriter(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\{client} - Tech Healthcheck Template.xlsx', engine='xlsxwriter')
    df_data.to_excel(writer, sheet_name = 'Data', index=False)
    df_healthcheck.to_excel(writer, sheet_name = 'Healthcheck', index=False)
    # set zoom at 85
    data_ws = writer.sheets['Data']
    healthcheck_ws = writer.sheets['Healthcheck']
    data_ws.set_zoom(85)
    healthcheck_ws.set_zoom(85)
    writer.save()

    print(f'\n{client} Template Saved'
           '\n'+'*   *   *   *   *   *   *   *   *   *')

print('\n'+'Healthcheck Templates Updated!')

# time script
mid_time = dt.datetime.now().replace(microsecond=0)

# iterate through rows of healthcheck list and produce one healthcheck per client
for i in hc_list.index:
    client = hc_list['Client'][i]
    project_id = hc_list['deepcrawl_project_id'][i]
    brand = hc_list['Brand'][i]

    # load workbook as openpyxl and convert to dataframe
    # (gets around an issue where pandas wouldn't load the workbook properly)
    print(f'\nOpening {client} Healthcheck')
    try:
        df = openpyxl.load_workbook(filename=fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Crawl Data\{client} - Tech Healthcheck Template.xlsx', data_only=True)
        df = df['Healthcheck']
        df = pd.DataFrame(df.values)
        df_rows, df_cols = df.shape
        xl_rows = df_rows + 4
        print(f'Starting to Write {client} Healthcheck')
    except FileNotFoundError:
        print(f'{client} Healthcheck Template not found')
        continue
    df_rows, df_cols = df.shape
    xl_rows = df_rows+4
    df = df.fillna('')

    # create blank workbook with xlsxwriter

    wb = xl.Workbook(fr'L:\Commercial\Operations\Technical SEO\Technical Healthchecks\01. Healthchecks\{client} - Tech Healthcheck.xlsx')
    ws = wb.add_worksheet('Healthcheck')

    #  CELL FORMATTING

    # heading formatting (conditional on brand client is with)

    if brand == 'Carat':
        brand_colour = '#00beff' # Carat
        brand_spark = '#4ee6C6' # Carat

    if brand == 'Vizeum':
        brand_colour = '#fac600' # Vizeum
        brand_spark = '#f6861f' # Vizeum

    if brand not in ['Carat', 'Vizeum']:
        brand_colour = '#8dc63f' # iPro
        brand_spark = '#066bb1' # iPro

    text = '#636363' # Generic

    # table Formatting
    client_name = wb.add_format({'bold':True, 'indent':0, 'font_color':text, 'font_size':20, 'align':'center', 'valign':'vcenter', 'rotation':0})
    l_head = wb.add_format({'bold':False, 'indent':0, 'bg_color':brand_colour, 'font_color':brand_colour, 'font_size':12, 'align':'center', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':0})
    c_head = wb.add_format({'bold':False, 'indent':0, 'bg_color':brand_colour, 'font_color':brand_colour, 'font_size':12, 'align':'center', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':0, 'right':0})
    r_head = wb.add_format({'bold':False, 'indent':0, 'bg_color':brand_colour, 'font_color':'#ffffff', 'font_size':12, 'align':'center', 'valign':'vcenter', 'rotation':90, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':1, 'num_format':'mmm-yy'})

    sub1 = wb.add_format({'bold':True, 'indent':1, 'font_color':text, 'font_size':18, 'align':'left', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':0, 'right':0})
    sub2 = wb.add_format({'bold':True, 'indent':1, 'font_color':'#ffffff', 'font_size':18, 'align':'left', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':0, 'right':0})
    issue = wb.add_format({'bold':True, 'indent':1, 'font_color':text, 'font_size':12, 'align':'left', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':1})
    detail = wb.add_format({'bold':False, 'indent':1, 'font_color':text, 'font_size':12, 'align':'left', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':1})
    spark = wb.add_format({'bold':False, 'indent':1, 'font_color':'#ffffff', 'font_size':12, 'align':'left', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':1})
    data = wb.add_format({'bold':False, 'indent':0, 'font_color':text, 'font_size':12, 'align':'center', 'valign':'vcenter', 'rotation':0, 'border_color':text, 'top':1, 'bottom':1, 'left':1, 'right':1})

    # row groups for adding formatting

    xl_head = [5]
    xl_sub = [6, 14, 21, 28, 37, 42, 46]
    xl_data = [7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 33, 34, 35, 36, 38, 39, 40, 41, 43, 44, 45, 47, 48, 49, 50, 51, 52, 53]

    # format + fill table header

    ws.set_row(2, 30)
    ws.write(2, 2, f'{client} Healthcheck', client_name)
    #print('1')

    # format + fill table data
    x = 5
    n = 0 #for df.loc[n]
    while x <= xl_rows:
        #print('2')
        if x in xl_head:
            #print('3')
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row (x, 60)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], l_head)
                    y += 1
                if y == 2:
                    ws.write(x, y, df_list[y-1], c_head)
                    y += 1
                if y == 3:
                    ws.write(x, y, '', c_head)
                    y += 1
                if y in range (4, df_cols):
                    ws.write(x, y, df_list[y-1], r_head)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], r_head)
                    y += 1
                    x += 1
                    n += 1
        if x in xl_sub:
            #print('4')
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row(x, 60)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], sub1)
                    y += 1
                if y in range(2, df_cols):
                    ws.write(x, y, df_list[y-1], sub2)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], sub2)
                    y += 1
                    x += 1
                    n += 1
        if x in xl_data:
        #print('5')
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row(x, 30)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], issue)
                    y += 1
                if y == 2:
                    ws.write(x, y, df_list[y-1], detail)
                    y += 1
                if y == 3:
                    ws.write(x, y, '', spark)
                    y += 1
                if y in range (4, df_cols):
                    ws.write(x, y, df_list[y-1], data)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], data)
                    y += 1
                    x += 1
                    n += 1

    # Add sparklines

    x, y = 7, 3
    while x <= xl_rows:
        sparkline = xl_range(x, 4, x, df_cols)
        ws.add_sparkline(x, y, {'range': sparkline, 'series_color': brand_spark, 'markers':True})
        x += 1
    ws.set_tab_color(brand_colour)

    # insert brand logo
    if brand == 'Carat':
        ws.insert_image('B2', fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Branding\Logos\Carat Logo.png', {'y_offset':5, 'x_scale':0.13, 'y_scale': 0.13})

    if brand == 'Vizeum':
        ws.insert_image('B1', fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Branding\Logos\Vizeum Logo.png', {'x_offset':20, 'y_offset':10, 'x_scale':0.4, 'y_scale': 0.4})

    if brand not in ['Carat', 'Vizeum']:
        ws.insert_image('B2', fr'L:\Commercial\Operations\Technical SEO\Automation\Data\Technical Healthchecks\Branding\Logos\iPro Logo.png', {'x_scale':0.22, 'y_scale': 0.22})

    # ADDITIONAL FORMATTING

    # column widths
    ws.set_column(0, 0, 5)
    ws.set_column(1, 1, 45)
    ws.set_column(2, 2, 90)
    ws.set_column(3, 3, 60)

    ws.freeze_panes(6, 2)   # Freeze first 6 rows and first 2 columns
    ws.set_zoom(70) # Set zoom to 70%
    ws.hide_gridlines(2) # Hide Gridlines
    #ws.hide_row_col_headers() # hide headers - not great for unhiding hiddent tabs

    if df_cols > 9:
        ws.set_column(4, df_cols-6, 0) # hide columns so only 6 months of data is visible

    wb.close()
    print(f'{client} Healthcheck Done'+'\n\n'
            '*   *   *   *   *   *   *   *   *   *')

print('\n'+'DURN!')

print('\n'+'Checking for skipped Clients:')
time.sleep(1)

if len(skipped_clients) > 0:
    for c in skipped_clients:
        print(c)
else:
    print('\n'+'All clients in Healthcheck List sheet were completed successfully!')

# =============================================================================
# END
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
dc_time = mid_time - start_time
xl_time = end_time - mid_time
total_time = end_time - start_time

print(f'Time taken to get data from DeepCrawl: {dc_time}'+'\n'
      f'Time taken to generate Excel healthchecks: {xl_time}'+'\n'
      f'Total time taken: {total_time}')

