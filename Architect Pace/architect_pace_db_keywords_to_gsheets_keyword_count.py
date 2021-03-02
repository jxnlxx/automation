#%% architect_pace_db_keywords_to_gsheets_keyword_count.py

import time
import sqlite3
import requests
import numpy as np
import pandas as pd
import datetime as dt

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key, stat_base_url                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

# definitions

def scrub(table_name):
    return table_name.lower().replace(' ', '_').replace('(','').replace(')','').replace(',','')

# get client list

gspread_id = '1ckZh9TSaSp1Ucu2HKIA7TELPdd24oylFDXVEx6dtdSw'
gsheet_name = 'Client List'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

# load data to dataframe from gsheet
print('Retrieving client list from Google Sheets...')
client_list = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True)# usecols=range(0, NUMBERCOLS))

# loading gsheets automatically loads 25 cols x 1k rows, so we trim it:
client_list = client_list.loc[:,~client_list.columns.str.contains('unnamed', case=False)] # remove columns containing 'unnamed' in label
client_list = client_list.dropna(axis=0, how='all') # remove rows that are empty
client_list['STAT ID'] = client_list['STAT ID'].astype(int) # ensure that STAT ID is not a float
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str (for some reason, 'int' on its own didn't work...)

print('Done!')

#%%

client = 'Jacamo'
client_list = client_list[client_list['Client Name'] == client].reset_index(drop=True)


#%%


for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    gspread_id = client_list['GSheet ID'][i]
    counts_df = pd.DataFrame(columns=['Category', 'Count'])

    date = cur.execute(f'SELECT Date FROM ranks_{save_name} ORDER BY Date ASC LIMIT 1')
    date = str(cur.fetchone()[0])

    sql = f'''
        SELECT keywords.Keyword, categories.Category
        FROM ranks_{save_name}
        JOIN keywords ON ranks_{save_name}.KeywordId = keywords.Id
        JOIN devices ON keywords.DeviceId = devices.Id
        JOIN categories ON keywords.CategoryId = categories.Id
        JOIN ctr_{save_name} ON ranks_{save_name}.GoogleBaseRank = ctr_{save_name}.Position
        WHERE Date = :date;
        '''

        #AND Device = :device
    params = {'date':date}#, 'device': 'smartphone'}
    df = pd.read_sql(sql, params=params, con=con)

    categories = df['Category'].to_list()
    categories = [i.strip() for i in categories]
    categories = list(dict.fromkeys(categories))

    count = pd.DataFrame({'Category': 'All Keywords', 'Count': df.shape[0]}, index=[0])
    counts_df = counts_df.append(count)

    for i in categories:
        count = df[df['Category']==i].shape[0]
        count = pd.DataFrame({'Category': i, 'Count': count}, index=[0])
        counts_df = counts_df.append(count)

    counts_df = counts_df.reset_index(drop=True)

    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(f'{client_name} - Keyword Counts')
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=f'{client_name} - Keyword Counts', rows=1, cols=1)

    set_with_dataframe(worksheet, counts_df)



# %%
