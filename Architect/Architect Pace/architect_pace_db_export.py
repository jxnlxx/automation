#%% architect_pace_export.py

import os
import time
import json
import sqlite3
import datetime as dt
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

client_name = 'Jacamo'
client_list = client_list[client_list['Client Name'] == client_name]

#%%

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
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

con.commit()
con.close()

#%% retrieve reports

incomplete = []

for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    print(f'Checking {client_name}\'s final job status...')

    requests_df = pd.read_sql_query(f'SELECT * FROM requests_{save_name} WHERE status = 0', con)
    try:
        final_job = requests_df['JobId'].iloc[-1]
    except IndexError:
        continue
    response = requests.get(f'{stat_base_url}/bulk/status?id={final_job}&format=json')
    response = response.json()
    status = response.get('Response').get('Result').get('Status')
    if status != 'Completed':
        time = dt.datetime.now().strftime('%H:%M')
        print(f'{time} - Final job status: {status}')
        incomplete += [client_name]
        continue
    else:
        print(f'{time} - Final job status: {status}')
        print('Continue with next cell')
        pass

    print (f'Adding {save_name} ranks...')

    all_kws = pd.read_sql_query(f'SELECT * FROM keywords', con)
    all_kws = all_kws['StatId'].to_list()

    for j in requests_df.index:
        con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
        cur = con.cursor()

        job_id = int(requests_df['JobId'][j])
        date = requests_df['Date'][j]
        print(f'\nBeginning job {j+1:03d} of {len(requests_df)}')
        print(f'\nFetching {client_name} data for {date}')

        stream_url = f'/bulk_reports/stream_report/{job_id}'
        response = requests.get('https://iprospectman.getstat.com'+stream_url+f'?key={stat_key}')
        response = response.json()
        new_kws = response.get('Response').get('Project').get('Site').get('Keyword')

        print('Data received!')
        new_kws = pd.json_normalize(new_kws)

        print('Filtering data...')
        new_kws = new_kws[['Id','Ranking.date','Ranking.Google.Rank','Ranking.Google.BaseRank']]

        new_kws['Id'] = new_kws['Id'].astype(int) # changes 'Id' to int so filter works - StatId is stored in the db as int
        new_kws = new_kws[new_kws['Id'].isin(all_kws)]

        new_kws = new_kws.replace('N/A', int('120'))
        new_kws = new_kws.fillna(int('120'))

        new_kws = new_kws.reset_index(drop=True)

        print(f'\nAdding keywords...')

#   add keywords to db
        for k in new_kws.index:
            if (k+1) % 500 == 0:
                print(f'Completed {k+1} of {len(new_kws)}')
            StatId = str(new_kws['Id'][k])
            Date = str(new_kws['Ranking.date'][k])
            GoogleRank = int(new_kws['Ranking.Google.Rank'][k])
            BaseRank = int(new_kws['Ranking.Google.BaseRank'][k])

            cur.execute('SELECT Id FROM keywords WHERE StatId = ? LIMIT 1', (StatId,))
            KeywordId = int(cur.fetchone()[0])

            cur.execute(f'''INSERT OR IGNORE INTO ranks_{save_name}(KeywordId, Date, GoogleRank, GoogleBaseRank)
                        VALUES (?, ?, ?, ?)''',(KeywordId, Date, GoogleRank, BaseRank))

            cur.execute(f'UPDATE requests_{save_name} SET Status = ? WHERE JobId = ?', (1, job_id,))

        print(f'Completed {len(new_kws)} of {len(new_kws)}')
        print(f'\nSaving {date}...')
        con.commit()
        con.close()
        print(f'Done!')
        print('---------------------')

        time.sleep(1)
con.commit()
con.close()
if len(incomplete) > 0:
    print('These Clients were skipped:')
    for i in incomplete:
        print(i)
else:
    print('Done!')

#%%

for i in new_kws.index:
    print(new_kws['Ranking.Google.Rank'][i])

#%%

con.close()