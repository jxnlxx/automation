#%% architect_pace_db_setup
import os
import time
import json
import sqlite3
import datetime
import requests
import openpyxl
import numpy as np
import pandas as pd
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

# load queries list

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
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str

print('Done!')
#%%
client = 'Simply Be'
client_list = client_list[client_list['Client Name'] == client].reset_index(drop=True)

#%%

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')

cur = con.cursor()

cur.execute(''' CREATE TABLE IF NOT EXISTS keywords(
Id INTEGER PRIMARY KEY,
StatId INTEGER NOT NULL,
Keyword TEXT NOT NULL,
DeviceId INTEGER NOT NULL,
CategoryId INTEGER NOT NULL,
TargetedSearchVolume INTEGER NOT NULL,
FOREIGN KEY (DeviceId) REFERENCES devices (Id),
FOREIGN KEY (CategoryId) REFERENCES categories (Id),
UNIQUE (StatID)
);''')

cur.execute('''CREATE TABLE IF NOT EXISTS devices(
    Id INTEGER PRIMARY KEY,
    Device TEXT NOT NULL,
    UNIQUE (Device)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS categories(
    Id INTEGER PRIMARY KEY,
    Category TEXT NOT NULL,
    UNIQUE (Category)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS criteria(
    Id INTEGER PRIMARY KEY,
    Criterion TEXT NOT NULL,
    UNIQUE (Criterion)
    );''')

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    cur.execute(f'''CREATE TABLE IF NOT EXISTS ranks_{save_name}(
        KeywordId INTEGER NOT NULL,
        Date TEXT NOT NULL,
        GoogleRank INTEGER NOT NULL,
        GoogleBaseRank INTEGER NOT NULL,
        FOREIGN KEY (KeywordId) REFERENCES keywords (Id),
        UNIQUE (KeywordId, Date)
        );''')

    cur.execute(f'''CREATE TABLE IF NOT EXISTS requests_{save_name}(
        Date TEXT NOT NULL,
        JobId INTEGER NOT NULL,
        Status INTEGER NOT NULL,
        UNIQUE (Date)
        );''')

    cur.execute(f'''CREATE TABLE IF NOT EXISTS visibility_{save_name}(
        Date TEXT NOT NULL,
        DeviceId INTEGER NOT NULL,
        CriterionId INTEGER NOT NULL,
        CategoryId INTEGER NOT NULL,
        Score INTEGER NOT NULL,
        FOREIGN KEY (DeviceId) REFERENCES keywords (Id),
        FOREIGN KEY (CriterionId) REFERENCES keywords (Id),
        FOREIGN KEY (CategoryId) REFERENCES keywords (Id),
        UNIQUE (Date, DeviceId, CriterionId, CategoryId)

        );''')

con.commit()
con.close()

#%%


#%%

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    save_name = client_name.lower().replace(' ','_')
    kw_list = pd.read_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\{save_name}_keywords_list2.csv')

    for i in kw_list.index:
        StatId = int(kw_list['Id'][i])
        Keyword = kw_list['Keyword'][i]
        KeywordDevice = kw_list['KeywordDevice'][i]
        Category = kw_list['Category'][i]
        TargetedSearchVolume = int(kw_list['KeywordStats.RegionalSearchVolume'][i])
        #Date = all_kws['KeywordRanking.date'][i]
        #GoogleRank = all_kws['KeywordRanking.Google.Rank'][i]
        #BaseRank = all_kws['KeywordRanking.Google.BaseRank'][i]
        print(f'Adding {Keyword}...')

#   Devices
        cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
        try:
            DeviceId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO devices (Device) VALUES (?)', (KeywordDevice,))
            cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
            DeviceId = int(cur.fetchone()[0])

#   Categories
        cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (Category,))
        try:
            CategoryId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO categories (Category) VALUES (?)', (Category,))
            cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (Category,))
            CategoryId  = int(cur.fetchone()[0])

#   Keywords
        cur.execute('SELECT Id FROM keywords WHERE StatId = ? LIMIT 1', (StatId,))
        try:
            KeywordId = int(cur.execute.fetchone()[0])
        except:
            cur.execute(
            '''INSERT OR IGNORE INTO keywords
            (StatId, Keyword, DeviceId, CategoryId, TargetedSearchVolume)
            VALUES (?, ?, ?, ?, ?)''',
            (StatId, Keyword, DeviceId, CategoryId, TargetedSearchVolume)
            )
        con.commit()

print('DURN!')

#%%

con.close()

# %%
