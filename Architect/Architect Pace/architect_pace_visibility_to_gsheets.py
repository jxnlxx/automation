#%% architect_pace_visibility_to_gsheets.py

import os
import time
import json
import sqlite3
import requests
import openpyxl
import numpy as np
import pandas as pd
import datetime as dt
import xlsxwriter as xl

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery


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

client = 'JD Williams'
client_list = client_list[client_list['Client Name'] == client].reset_index(drop=True)

#%% add to gsheets

for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    gspread_id = client_list['GSheet ID'][i]

# insert code to add CTR model to gsheets

    sql = f'''
    SELECT visibility_{save_name}.Date, devices.Device as Device, criteria.Criterion as Criteria, categories.Category as Category, visibility_{save_name}.Score
    FROM visibility_{save_name}
    JOIN devices ON visibility_{save_name}.DeviceId = devices.Id
    JOIN criteria ON visibility_{save_name}.CriterionId = criteria.Id
    JOIN categories ON visibility_{save_name}.CategoryId = categories.Id
    WHERE date(Date) > date('now', '-1 year');'''

    df = pd.read_sql(sql, con=con)
    df_rows, df_cols = df.shape

    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(f'{client_name} - Visibility')
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=f'{client_name} - Visibility', rows=1, cols=1)

    set_with_dataframe(worksheet, df)


#%%
