#%% architect_pace_add_ctr_from_csv.py

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

#%% filter client_list

print('Filtering client_list')
client = 'Simply Be'
client_list = client_list[client_list['Client Name'] == client].reset_index(drop=True)
print('Done!')

#%% create ctr table in db

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    print (f'Adding {save_name} ranks...')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS ctr_{save_name}(
        Position INTEGER NOT NULL,
        CTR REAL NOT NULL,
        UNIQUE (Position)
        );''')

con.commit()
con.close()

#%% import keywords from csv

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    save_name = client_name.lower().replace(' ','_')
    ctr = pd.read_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Setup\{save_name}_ctr.csv')
    print(f'Adding ranks...')
    for j in ctr.index:
        try:
            Position = int(ctr['Rank'][j])
        except KeyError:
            Position = int(ctr['Position'][j])
        CTR = float(ctr['CTR'][j])
        print(Position)
        cur.execute(f'INSERT OR IGNORE INTO ctr_{save_name}(Position, CTR) VALUES (?, ?)', (Position, CTR,))

con.commit()
con.close()

print('DURN!')

# %%
