#%% architect_pace_add_keywords.py

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

client = 'Simply Be'
client_list = client_list[client_list['Client Name'] == client].reset_index(drop=True)

#%%

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')

cur = con.cursor()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    print (f'Adding {save_name} ranks...')
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


#%% import keywords from STAT

all_kws = pd.DataFrame()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    stat_id = client_list['STAT ID'][i]
    keywords_list = f'/keywords/list?site_id={stat_id}&format=json'
    url = stat_base_url + keywords_list + '&results=5000'
    print('\n'+f'Requesting data for {client_name}'
          '\n'+f'({i+1} of {len(client_list)})')

    n = 1 # for showing progress through site ids
    p = 1 # for showing progress through pagination

    response = requests.get(url)
    response = response.json()

    kw_list = response.get('Response').get('Result')                                # extracts 'result' which is a list of dictionaries - 1 dict for each keyword
    print(p, keywords_list)
    p += 1 # for showing progress through pagination

    while True:                                                                     # Loop through pages to get all data
        try:
            nextpage = response.get('Response').get('nextpage')                     # get 'nextpage' path from response
            url = stat_base_url + nextpage                                          # create URL from 'base_url and 'nextpage'
            print(p, nextpage)                                                           # print n + url to show script is not idle
            response = requests.get(url)                                            # ping the new url to get the data
            response = response.json()                                              # convert the JSON response into dict
            new_kws = response.get('Response').get('Result')                        # extract 'Result' from dict
            kw_list = kw_list + new_kws                                             # append the data to the kw_list
            p += 1                                                                  # add 1 to 'n'
        except TypeError:# TypeError:                                               # if 'nextpage' is blank, this will yield a TypeError when attempting to append
            break                                                                   # this will be the last page, so break the loop
#   process keywords for export
    total_kws = len(kw_list)
    kw_df = pd.json_normalize(kw_list)
    all_kws = all_kws.append(kw_df, ignore_index=True)
    #kw_df.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\{save_name}_keywords_list.csv', index=False)

for i in all_kws.index:
    StatId = int(all_kws['Id'][i])
    Keyword = all_kws['Keyword'][i]
    KeywordDevice = all_kws['KeywordDevice'][i]
    TargetedSearchVolume = int(all_kws['KeywordStats.RegionalSearchVolume'][i])

#   Devices
    cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
    try:
        DeviceId = int(cur.fetchone()[0])
    except:
        cur.execute('INSERT OR IGNORE INTO devices (Device) VALUES (?)', (KeywordDevice,))
        cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
        DeviceId = int(cur.fetchone()[0])

#   Keywords
    cur.execute('SELECT Id FROM keywords WHERE StatId = ? LIMIT 1', (StatId,))
    try:
        KeywordId = int(cur.execute.fetchone()[0])
    except:
        cur.execute(
        '''INSERT OR IGNORE INTO keywords
        (StatId, Keyword, DeviceId, TargetedSearchVolume)
        VALUES (?, ?, ?, ?, ?)''',
        (StatId, Keyword, DeviceId, TargetedSearchVolume,)
        )

con.commit()
con.close()

print('DURN!')

# %%
